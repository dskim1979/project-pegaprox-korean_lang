# site recovery failover orchestrator
# NS Mar 2026 - handles planned, emergency, test failovers and auto-heartbeat

import json
import logging
import time
import uuid
import requests
from datetime import datetime

from pegaprox.core.db import get_db
from pegaprox.globals import cluster_managers
from pegaprox.utils.audit import log_audit
from pegaprox.utils.realtime import broadcast_sse

logger = logging.getLogger('pegaprox.site_recovery')

_heartbeat_running = False


def _create_event(plan_id, event_type, triggered_by='system'):
    event_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()
    db = get_db()
    db.execute('''INSERT INTO site_recovery_events (id, plan_id, event_type, status, started_at, triggered_by)
        VALUES (?, ?, ?, 'running', ?, ?)''', (event_id, plan_id, event_type, now, triggered_by))
    return event_id


def _complete_event(event_id, status, details=None):
    now = datetime.utcnow().isoformat()
    db = get_db()
    db.execute('UPDATE site_recovery_events SET status = ?, completed_at = ?, details = ? WHERE id = ?',
               (status, now, json.dumps(details or {}), event_id))


def _fire_webhook(url):
    """Call pre/post failover webhook, don't block on failure"""
    if not url:
        return
    try:
        requests.post(url, json={'event': 'site_recovery', 'timestamp': datetime.utcnow().isoformat()}, timeout=30)
    except Exception as e:
        logger.warning(f"[SR] Webhook failed: {url} - {e}")


def _broadcast_progress(plan_id, message, progress=None):
    """Push realtime update to frontend"""
    broadcast_sse({'type': 'site_recovery', 'plan_id': plan_id, 'message': message, 'progress': progress})


def _get_plan(plan_id):
    db = get_db()
    row = db.query_one('SELECT * FROM site_recovery_plans WHERE id = ?', (plan_id,))
    if not row:
        return None
    plan = dict(row)
    for k in ('network_mappings', 'storage_mappings'):
        try:
            plan[k] = json.loads(plan[k] or '{}')
        except Exception:
            plan[k] = {}
    return plan


def _get_plan_vms(plan_id):
    db = get_db()
    rows = db.query('SELECT * FROM site_recovery_vms WHERE plan_id = ? ORDER BY boot_group, vmid', (plan_id,))
    return [dict(r) for r in rows] if rows else []


def _group_vms_by_boot(vms):
    """Group VMs by boot_group, returns sorted list of (group_num, [vms])"""
    groups = {}
    for vm in vms:
        g = vm.get('boot_group', 0)
        groups.setdefault(g, []).append(vm)
    return sorted(groups.items())


# MK: the actual migration logic delegates to the existing cross-cluster-migrate infra
def _migrate_vm_cross_cluster(src_mgr, tgt_mgr, vmid, vm_type, storage_map, net_map):
    """Migrate single VM from source to target using existing remote_migrate API.
    Returns (success: bool, error: str)"""
    try:
        # find VM's node on source
        node_status = src_mgr.get_node_status()
        vm_node = None
        if node_status:
            for node_name, ndata in node_status.items():
                vms_on_node = src_mgr.get_vms(node_name) if hasattr(src_mgr, 'get_vms') else []
                for v in vms_on_node:
                    if v.get('vmid') == vmid:
                        vm_node = node_name
                        break
                if vm_node:
                    break

        if not vm_node:
            return False, f"VM {vmid} not found on source cluster"

        # determine target storage
        # MK: check if VM's current storage has a mapping, else use first available
        target_storage = ''
        if storage_map:
            # try to map from VM's disk storage
            try:
                config = src_mgr.get_vm_config(vm_node, vmid, vm_type)
                for k, v in (config or {}).items():
                    if k.startswith(('scsi', 'virtio', 'ide', 'sata')) and isinstance(v, str) and ':' in v:
                        src_stor = v.split(':')[0]
                        if src_stor in storage_map:
                            target_storage = storage_map[src_stor]
                            break
            except Exception:
                pass
        if not target_storage:
            target_storage = list(storage_map.values())[0] if storage_map else 'local-lvm'

        target_bridge = 'vmbr0'
        if net_map:
            target_bridge = list(net_map.values())[0] if net_map else 'vmbr0'

        # create temp token on target for migration auth
        token_result = tgt_mgr.create_api_token('pegaprox-sr')
        if not token_result.get('success'):
            return False, f"Failed to create API token on target: {token_result.get('error', 'unknown')}"

        token_id = token_result['token_id']
        token_value = token_result['token_value']

        # get target fingerprint
        fp_result = tgt_mgr.get_cluster_fingerprint()
        if not fp_result.get('success'):
            tgt_mgr.delete_api_token('pegaprox-sr')
            return False, f"Failed to get target fingerprint: {fp_result.get('error', '')}"

        fingerprint = fp_result['fingerprint']
        target_host = tgt_mgr.host

        # build target endpoint string (Proxmox format)
        target_endpoint = f"apitoken=PVEAPIToken={token_id}={token_value},host={target_host},fingerprint={fingerprint}"

        # execute migration
        result = src_mgr.remote_migrate_vm(
            node=vm_node, vmid=vmid, vm_type=vm_type,
            target_endpoint=target_endpoint,
            target_storage=target_storage,
            target_bridge=target_bridge,
            online=True, delete_source=True
        )

        # cleanup token after a delay (migration is async)
        def _delayed_cleanup():
            time.sleep(1800)  # 30 min grace - migrations can take a while
            try:
                tgt_mgr.delete_api_token('pegaprox-sr')
            except Exception:
                pass

        import gevent
        gevent.spawn(_delayed_cleanup)

        if result.get('success'):
            return True, ''
        return False, result.get('error', 'Migration failed')

    except Exception as e:
        logger.error(f"[SR] Migration error for VM {vmid}: {e}")
        return False, str(e)


def _start_replicated_vm(tgt_mgr, vmid, vm_type='qemu'):
    """Start a replicated VM on target (emergency failover).
    The VM should already exist on target from replication.
    Returns (success, error)"""
    try:
        # find which node the replicated VM is on
        node_status = tgt_mgr.get_node_status()
        if not node_status:
            return False, "Cannot get target node status"

        for node_name in node_status:
            try:
                vms = tgt_mgr.get_vms(node_name) if hasattr(tgt_mgr, 'get_vms') else []
                for v in vms:
                    if v.get('vmid') == vmid:
                        # found it, start it
                        result = tgt_mgr.start_vm(node_name, vmid, vm_type)
                        if result:
                            return True, ''
                        return False, f"start_vm returned falsy for {vmid}"
            except Exception as e:
                continue

        return False, f"VM {vmid} not found on target cluster"
    except Exception as e:
        return False, str(e)


def execute_failover(plan_id, failover_type='planned'):
    """Main failover orchestrator. Runs in greenlet.

    failover_type: 'planned', 'emergency', 'failback'
    """
    plan = _get_plan(plan_id)
    if not plan:
        logger.error(f"[SR] Plan {plan_id} not found")
        return

    event_id = _create_event(plan_id, failover_type)
    vms = _get_plan_vms(plan_id)
    boot_groups = _group_vms_by_boot(vms)
    results = {}
    failed = False

    logger.info(f"[SR] Starting {failover_type} failover for plan '{plan['name']}' ({len(vms)} VMs, {len(boot_groups)} boot groups)")
    _broadcast_progress(plan_id, f"Starting {failover_type} failover...", 0)

    # pre-webhook
    _fire_webhook(plan.get('pre_failover_webhook'))

    # determine source/target based on type
    if failover_type == 'failback':
        # reverse: target becomes source, source becomes target
        src_id = plan['target_cluster']
        tgt_id = plan['source_cluster']
    else:
        src_id = plan['source_cluster']
        tgt_id = plan['target_cluster']

    src_mgr = cluster_managers.get(src_id)
    tgt_mgr = cluster_managers.get(tgt_id)

    net_map = plan.get('network_mappings', {})
    stor_map = plan.get('storage_mappings', {})

    total_vms = len(vms)
    completed = 0

    for group_idx, (group_num, group_vms) in enumerate(boot_groups):
        logger.info(f"[SR] Boot group {group_num} ({len(group_vms)} VMs)")
        _broadcast_progress(plan_id, f"Boot group {group_num}...", int(completed / total_vms * 100))

        for vm in group_vms:
            vmid = vm['vmid']
            vm_type = vm.get('vm_type', 'qemu')
            vm_name = vm.get('vm_name', f'VM {vmid}')

            if failover_type == 'emergency':
                # source is down - start replicated VM on target
                logger.info(f"[SR] Emergency: starting {vm_name} ({vmid}) on target")
                _broadcast_progress(plan_id, f"Starting {vm_name} on target...", int(completed / total_vms * 100))
                ok, err = _start_replicated_vm(tgt_mgr, vmid, vm_type)
            else:
                # planned or failback - live migrate
                if not src_mgr or not src_mgr.is_connected:
                    ok, err = False, "Source cluster not connected"
                else:
                    logger.info(f"[SR] Migrating {vm_name} ({vmid}): {src_id} → {tgt_id}")
                    _broadcast_progress(plan_id, f"Migrating {vm_name}...", int(completed / total_vms * 100))
                    ok, err = _migrate_vm_cross_cluster(src_mgr, tgt_mgr, vmid, vm_type, stor_map, net_map)

            results[str(vmid)] = {'success': ok, 'error': err, 'vm_name': vm_name}
            if not ok:
                logger.error(f"[SR] Failed for {vm_name}: {err}")
                failed = True
            else:
                logger.info(f"[SR] {vm_name} OK")

            completed += 1

        # wait boot_delay before next group (use first VM's delay in group)
        if group_idx < len(boot_groups) - 1:
            delay = group_vms[0].get('boot_delay', 30)
            if delay > 0:
                logger.info(f"[SR] waiting {delay}s before next boot group")
                _broadcast_progress(plan_id, f"Waiting {delay}s before next group...", int(completed / total_vms * 100))
                time.sleep(delay)

    # done
    final_status = 'failed' if failed else 'completed'
    _complete_event(event_id, final_status, results)

    db = get_db()
    now = datetime.utcnow().isoformat()
    db.execute("UPDATE site_recovery_plans SET status = ?, last_failover = ?, updated_at = ? WHERE id = ?",
               (final_status, now, now, plan_id))

    _fire_webhook(plan.get('post_failover_webhook'))
    _broadcast_progress(plan_id, f"Failover {final_status}", 100)

    log_audit('system', f'site_recovery.{failover_type}_complete',
              f"Plan '{plan['name']}' {failover_type} {final_status}: {completed}/{total_vms} VMs")

    logger.info(f"[SR] Failover {final_status} for '{plan['name']}': {sum(1 for r in results.values() if r['success'])}/{total_vms} succeeded")


def execute_test_failover(plan_id):
    """Clone replicated VMs on target, start in test mode.
    VMs stay running until user triggers cleanup."""
    plan = _get_plan(plan_id)
    if not plan:
        return

    event_id = _create_event(plan_id, 'test')
    vms = _get_plan_vms(plan_id)
    tgt_mgr = cluster_managers.get(plan['target_cluster'])
    results = {}
    test_vmids = []

    logger.info(f"[SR] Test failover for '{plan['name']}' ({len(vms)} VMs)")
    _broadcast_progress(plan_id, "Starting test failover...", 0)

    for i, vm in enumerate(vms):
        vmid = vm['vmid']
        vm_name = vm.get('vm_name', f'VM {vmid}')
        _broadcast_progress(plan_id, f"Cloning {vm_name}...", int(i / len(vms) * 100))

        try:
            # find the replicated VM on target and clone it
            node_status = tgt_mgr.get_node_status() or {}
            found = False
            for node_name in node_status:
                try:
                    tgt_vms = tgt_mgr.get_vms(node_name) if hasattr(tgt_mgr, 'get_vms') else []
                    all_vmids = [v.get('vmid') for v in tgt_vms]
                    for v in tgt_vms:
                        if v.get('vmid') == vmid:
                            # find free VMID for test clone
                            test_vmid = vmid + 90000
                            while test_vmid in all_vmids:
                                test_vmid += 1
                            vtype = vm.get('vm_type', 'qemu')
                            clone_result = tgt_mgr.clone_vm(node_name, vmid, vtype,
                                                            newid=test_vmid, name=f"SR-TEST-{vm_name}")
                            if clone_result:
                                test_vmids.append({'vmid': test_vmid, 'vm_type': vtype})
                                tgt_mgr.start_vm(node_name, test_vmid, vtype)
                                results[str(vmid)] = {'success': True, 'test_vmid': test_vmid}
                            else:
                                results[str(vmid)] = {'success': False, 'error': 'Clone failed'}
                            found = True
                            break
                except Exception as e:
                    continue
                if found:
                    break
            if not found:
                results[str(vmid)] = {'success': False, 'error': 'VM not found on target'}
        except Exception as e:
            results[str(vmid)] = {'success': False, 'error': str(e)}

    _complete_event(event_id, 'completed', {'results': results, 'test_vmids': test_vmids})

    db = get_db()
    now = datetime.utcnow().isoformat()
    db.execute("UPDATE site_recovery_plans SET last_test = ?, updated_at = ? WHERE id = ?", (now, now, plan_id))

    # keep status as 'testing' until cleanup
    _broadcast_progress(plan_id, "Test failover complete. Cleanup when ready.", 100)
    logger.info(f"[SR] Test failover complete for '{plan['name']}': {len(test_vmids)} clones created")


def cleanup_test(plan_id):
    """Stop and delete test clone VMs"""
    plan = _get_plan(plan_id)
    if not plan:
        return

    tgt_mgr = cluster_managers.get(plan['target_cluster'])
    if not tgt_mgr:
        return

    # find last test event with test_vmids
    db = get_db()
    event = db.query_one(
        "SELECT details FROM site_recovery_events WHERE plan_id = ? AND event_type = 'test' ORDER BY started_at DESC LIMIT 1",
        (plan_id,))
    if not event:
        return

    try:
        details = json.loads(event['details'] or '{}')
    except Exception:
        details = {}

    test_vmids = details.get('test_vmids', [])

    for entry in test_vmids:
        # LW: entry can be dict {vmid, vm_type} or int (legacy)
        if isinstance(entry, dict):
            test_vmid = entry.get('vmid', 0)
            vtype = entry.get('vm_type', 'qemu')
        else:
            test_vmid = entry
            vtype = 'qemu'
        try:
            node_status = tgt_mgr.get_node_status() or {}
            for node_name in node_status:
                try:
                    tgt_mgr.stop_vm(node_name, test_vmid, vtype)
                    time.sleep(3)
                    tgt_mgr.delete_vm(node_name, test_vmid, vtype, purge=True)
                    logger.info(f"[SR] Cleaned up test VM {test_vmid}")
                    break
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"[SR] Cleanup failed for test VM {test_vmid}: {e}")

    db.execute("UPDATE site_recovery_plans SET status = 'ready', updated_at = ? WHERE id = ?",
               (datetime.utcnow().isoformat(), plan_id))

    _broadcast_progress(plan_id, "Test cleanup complete", 100)


# ---- Auto-Failover Heartbeat ----

_last_fail_times = {}  # plan_id -> first_fail_timestamp
_cooldowns = {}  # plan_id -> cooldown_until_timestamp


def _heartbeat_check():
    """Check all plans with auto_failover enabled.
    If source cluster unreachable for failover_timeout seconds, trigger emergency failover."""
    db = get_db()
    plans = db.query("SELECT * FROM site_recovery_plans WHERE auto_failover = 1 AND status = 'ready'")
    if not plans:
        return

    now = time.time()

    for row in plans:
        plan = dict(row)
        plan_id = plan['id']

        # respect cooldown
        if plan_id in _cooldowns and now < _cooldowns[plan_id]:
            continue

        src_mgr = cluster_managers.get(plan['source_cluster'])
        src_reachable = src_mgr and src_mgr.is_connected if src_mgr else False

        if src_reachable:
            # clear failure tracking
            _last_fail_times.pop(plan_id, None)
            continue

        # source unreachable
        if plan_id not in _last_fail_times:
            _last_fail_times[plan_id] = now
            logger.warning(f"[SR] Heartbeat: source '{plan['source_cluster']}' unreachable for plan '{plan['name']}'")
            continue

        elapsed = now - _last_fail_times[plan_id]
        timeout = plan.get('failover_timeout', 120)

        if elapsed >= timeout:
            logger.error(f"[SR] AUTO-FAILOVER triggered for '{plan['name']}' after {int(elapsed)}s")
            _last_fail_times.pop(plan_id, None)
            _cooldowns[plan_id] = now + 3600  # 1h cooldown

            # trigger emergency failover — double-check status to avoid race
            try:
                import gevent
                fresh = db.query_one("SELECT status FROM site_recovery_plans WHERE id = ?", (plan_id,))
                if fresh and fresh['status'] != 'ready':
                    continue
                db.execute("UPDATE site_recovery_plans SET status = 'running', updated_at = ? WHERE id = ?",
                           (datetime.utcnow().isoformat(), plan_id))
                gevent.spawn(execute_failover, plan_id, 'emergency')
                log_audit('system', 'site_recovery.auto_failover',
                          f"Auto-failover triggered for '{plan['name']}' - source unreachable for {int(elapsed)}s")
            except Exception as e:
                logger.error(f"[SR] Auto-failover spawn failed: {e}")


def heartbeat_loop():
    """Background loop for auto-failover heartbeat monitoring"""
    global _heartbeat_running
    _heartbeat_running = True
    logger.info("[SR] Heartbeat monitor started")

    while _heartbeat_running:
        try:
            _heartbeat_check()
        except Exception as e:
            logger.error(f"[SR] Heartbeat error: {e}")
        time.sleep(30)


def start_heartbeat():
    import gevent
    gevent.spawn(heartbeat_loop)


def stop_heartbeat():
    global _heartbeat_running
    _heartbeat_running = False
