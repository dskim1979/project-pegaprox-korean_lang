# -*- coding: utf-8 -*-
"""
XCP-ng / Xen Orchestra Pool Manager - Layer 5
XAPI (XML-RPC) connection, VM lifecycle, storage, network ops.

NS: Mar 2026 - first-class XCP-ng integration, same sidebar as Proxmox.
"""

import logging
import threading
import time
import uuid as _uuid
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from pegaprox.constants import LOG_DIR
from pegaprox import globals as _g
from pegaprox.core.db import get_db
from pegaprox.utils.realtime import broadcast_sse

# XenAPI is optional - only needed for XCP-ng clusters
try:
    import XenAPI
    XENAPI_AVAILABLE = True
except ImportError:
    XenAPI = None
    XENAPI_AVAILABLE = False

# state mapping: XAPI power_state -> our standard states
_POWER_STATE_MAP = {
    'Running': 'running',
    'Halted': 'stopped',
    'Suspended': 'suspended',
    'Paused': 'paused',
}


def _sanitize_str(val):
    """Strip NUL bytes and limit length. Same idea as manager.py sanitizer."""
    if not isinstance(val, str):
        return str(val) if val is not None else ''
    return val.replace('\x00', '')[:4096]


class XcpngManager:
    """
    XCP-ng pool manager - duck-typed to match PegaProxManager's public interface.

    MK: intentionally no ABC - PegaProxManager is ~10k lines and retrofitting
    a base class would be a nightmare. Both types live in cluster_managers dict,
    API layer dispatches transparently.

    Not all methods are implemented yet, unfinished ones raise NotImplementedError
    so we get a clear signal instead of silent failures.
    """

    # match PegaProxManager's lock descriptions
    LOCK_DESCRIPTIONS = {
        'migrate': 'Migration in progress',
        'snapshot': 'Snapshot operation in progress',
        'clone': 'Clone operation in progress',
        'create': 'VM creation in progress',
        'suspended': 'VM suspended',
    }

    def __init__(self, cluster_id: str, config):
        self.id = cluster_id
        self.config = config
        self.cluster_type = 'xcpng'
        self.running = False
        self.thread = None
        self.stop_event = threading.Event()
        self.last_run = None

        # XAPI session
        self._session = None
        self._session_lock = threading.Lock()
        self._last_keepalive = 0

        # connection state (same attrs as PegaProxManager)
        self.is_connected = False
        self.current_host = None
        self.connection_error = None
        self._consecutive_failures = 0
        self._last_reconnect_attempt = 0

        # node/vm caches
        self._cached_nodes = None
        self._nodes_cache_time = 0
        self._nodes_cache_ttl = 8  # seconds, same as PegaProxManager
        self._cached_vms = None
        self._vms_cache_time = 0

        # maintenance stubs (needed for API compat)
        self.nodes_in_maintenance = {}
        self.maintenance_lock = threading.Lock()
        self.nodes_updating = {}
        self.update_lock = threading.Lock()
        self.ha_enabled = False
        self.ha_node_status = {}
        self.ha_lock = threading.Lock()
        self.ha_recovery_in_progress = {}
        self._cached_node_dict = {}

        # task tracking - xapi opaque refs -> our task dicts
        self._active_tasks = {}
        self._task_lock = threading.Lock()

        # logging - per cluster, same as PegaProxManager
        self.logger = logging.getLogger(f"XCPng_{config.name}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        if self.logger.handlers:
            self.logger.handlers.clear()
        fh = logging.FileHandler(f"{LOG_DIR}/{cluster_id}.log")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        fmt = logging.Formatter('[%(asctime)s] [%(name)s] %(levelname)s: %(message)s')
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    # ──────────────────────────────────────────
    # Connection management
    # ──────────────────────────────────────────

    def _get_xapi_url(self):
        host = self.config.host
        # NS: allow both bare hostname and full URL
        if not host.startswith('http'):
            host = f"https://{host}"
        return host

    def connect(self) -> bool:
        """Establish XAPI session to the XCP-ng pool master."""
        if not XENAPI_AVAILABLE:
            self.connection_error = 'XenAPI library not installed (pip install XenAPI)'
            self.logger.error(self.connection_error)
            return False

        with self._session_lock:
            try:
                url = self._get_xapi_url()
                session = XenAPI.Session(url, ignore_ssl=not self.config.ssl_verification)
                session.xenapi.login_with_password(
                    self.config.user, self.config.pass_,
                    '1.0', 'PegaProx'
                )
                self._session = session
                self.is_connected = True
                self.connection_error = None
                self.current_host = self.config.host
                self._consecutive_failures = 0
                self._last_keepalive = time.time()
                self.logger.info(f"Connected to XCP-ng pool: {self.config.host}")
                return True
            except Exception as e:
                self.is_connected = False
                self.connection_error = str(e)
                self._consecutive_failures += 1
                # only log first few failures
                if self._consecutive_failures <= 3:
                    self.logger.error(f"XAPI connect failed: {e}")
                return False

    # compat alias for API layer
    def connect_to_proxmox(self) -> bool:
        return self.connect()

    def disconnect(self):
        with self._session_lock:
            if self._session:
                try:
                    self._session.xenapi.session.logout()
                except Exception:
                    pass
                self._session = None
            self.is_connected = False
            self.logger.info("Disconnected from XCP-ng pool")

    def _keepalive(self):
        """Ping session to prevent timeout. XAPI sessions expire after ~24h idle."""
        now = time.time()
        if now - self._last_keepalive < 300:
            return
        try:
            self._session.xenapi.session.get_uuid(self._session._session)
            self._last_keepalive = now
        except Exception:
            self.logger.warning("XAPI session expired, reconnecting...")
            self.is_connected = False
            self.connect()

    def _api(self):
        """Get the xenapi proxy, reconnecting if needed."""
        if not self._session or not self.is_connected:
            if not self.connect():
                return None
        self._keepalive()
        return self._session.xenapi

    # ──────────────────────────────────────────
    # Start/stop background loop
    # ──────────────────────────────────────────

    def start(self):
        if self.running:
            return
        self.running = True
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run_loop, daemon=True,
                                       name=f"xcpng-{self.id}")
        self.thread.start()
        self.logger.info("XCP-ng manager started")

    def stop(self):
        self.running = False
        self.stop_event.set()
        self.disconnect()
        self.logger.info("XCP-ng manager stopped")

    def _run_loop(self):
        """Background loop - periodic status refresh & task polling."""
        # initial connect
        self.connect()
        while not self.stop_event.is_set():
            try:
                if self.is_connected:
                    self._refresh_cache()
                    self._poll_tasks()
                    self.last_run = datetime.now()
                else:
                    # throttle reconnect attempts
                    now = time.time()
                    if now - self._last_reconnect_attempt > 30:
                        self._last_reconnect_attempt = now
                        self.connect()
            except Exception as e:
                self.logger.error(f"Loop error: {e}")

            interval = getattr(self.config, 'check_interval', 300)
            # NS: poll more often so tasks show up quickly
            self.stop_event.wait(min(interval, 15))

    # ──────────────────────────────────────────
    # Cache refresh
    # ──────────────────────────────────────────

    def _refresh_cache(self):
        """Pull fresh node/VM data from XAPI."""
        api = self._api()
        if not api:
            return

        now = time.time()
        if now - self._nodes_cache_time < self._nodes_cache_ttl:
            return

        try:
            self._cached_nodes = self._fetch_nodes(api)
            self._cached_vms = self._fetch_vms(api)
            self._nodes_cache_time = now
            self._vms_cache_time = now
        except Exception as e:
            self.logger.error(f"Cache refresh failed: {e}")
            self._consecutive_failures += 1
            if self._consecutive_failures > 5:
                self.is_connected = False

    # ──────────────────────────────────────────
    # Nodes
    # ──────────────────────────────────────────

    def _fetch_nodes(self, api) -> list:
        host_refs = api.host.get_all()
        nodes = []
        for ref in host_refs:
            rec = api.host.get_record(ref)
            metrics_ref = rec.get('metrics', 'OpaqueRef:NULL')
            mem_total = 0
            mem_free = 0
            try:
                if metrics_ref != 'OpaqueRef:NULL':
                    m = api.host_metrics.get_record(metrics_ref)
                    mem_total = int(m.get('memory_total', 0))
                    mem_free = int(m.get('memory_free', 0))
            except Exception:
                pass

            cpu_count = len(rec.get('host_CPUs', []))

            # NS: query_data_source for live CPU avg (fraction 0-1)
            cpu_util = 0
            try:
                cpu_util = float(api.host.query_data_source(ref, 'cpu_avg'))
            except Exception:
                pass

            # uptime - try data source first, fallback to other_config boot_time
            uptime_secs = 0
            try:
                uptime_secs = int(float(api.host.query_data_source(ref, 'uptime')))
            except Exception:
                try:
                    bt = rec.get('other_config', {}).get('boot_time', '')
                    if bt:
                        uptime_secs = int(time.time() - float(bt))
                except Exception:
                    pass

            # network I/O - sum across physical interfaces
            netin = 0
            netout = 0
            for pif_ref in rec.get('PIFs', []):
                try:
                    dev = api.PIF.get_device(pif_ref)
                    if not dev:
                        continue
                    # bytes/sec from XAPI data source
                    netin += max(0, float(api.host.query_data_source(ref, f'pif_{dev}_rx')))
                    netout += max(0, float(api.host.query_data_source(ref, f'pif_{dev}_tx')))
                except Exception:
                    pass

            nodes.append({
                'node': _sanitize_str(rec.get('hostname', rec.get('name_label', ''))),
                'status': 'online' if rec.get('enabled', True) else 'offline',
                'id': _sanitize_str(rec.get('uuid', '')),
                'cpu': cpu_util,
                'maxcpu': cpu_count,
                'mem': mem_total - mem_free,
                'maxmem': mem_total,
                'uptime': uptime_secs,
                'netin': netin,   # bytes/sec (rate, not cumulative)
                'netout': netout,
                'type': 'node',
                '_ref': ref,
            })
        return nodes

    def get_nodes(self) -> list:
        if self._cached_nodes is not None:
            # strip internal fields
            return [{k: v for k, v in n.items() if not k.startswith('_')}
                    for n in self._cached_nodes]
        api = self._api()
        if not api:
            return []
        nodes = self._fetch_nodes(api)
        self._cached_nodes = nodes
        self._nodes_cache_time = time.time()
        return [{k: v for k, v in n.items() if not k.startswith('_')} for n in nodes]

    # ──────────────────────────────────────────
    # VMs
    # ──────────────────────────────────────────

    def _fetch_vms(self, api) -> list:
        db = get_db()
        vm_refs = api.VM.get_all()
        now = time.time()
        vms = []
        for ref in vm_refs:
            try:
                rec = api.VM.get_record(ref)
            except Exception:
                continue

            # skip templates, control domains, snapshots
            if rec.get('is_a_template', False):
                continue
            if rec.get('is_control_domain', False):
                continue
            if rec.get('is_a_snapshot', False):
                continue

            vm_uuid = rec.get('uuid', '')
            vmid = db.xcpng_get_vmid(self.id, vm_uuid)

            # figure out which host its on
            resident = rec.get('resident_on', 'OpaqueRef:NULL')
            node_name = ''
            if resident != 'OpaqueRef:NULL':
                try:
                    node_name = api.host.get_hostname(resident)
                except Exception:
                    pass

            power = rec.get('power_state', 'Halted')
            status = _POWER_STATE_MAP.get(power, 'unknown')

            vcpus = int(rec.get('VCPUs_at_startup', 0))
            vcpus_max = int(rec.get('VCPUs_max', 0))
            mem_max = int(rec.get('memory_dynamic_max', 0)) or int(rec.get('memory_static_max', 0))

            # MK: pull live stats from VM_metrics if VM is running
            cpu_frac = 0
            mem_actual = int(rec.get('memory_target', 0))
            vm_uptime = 0

            vm_metrics_ref = rec.get('metrics', 'OpaqueRef:NULL')
            if vm_metrics_ref != 'OpaqueRef:NULL' and power == 'Running':
                try:
                    vm_m = api.VM_metrics.get_record(vm_metrics_ref)
                    # average vCPU utilisation
                    utils = vm_m.get('VCPUs_utilisation', {})
                    if utils:
                        cpu_frac = sum(float(v) for v in utils.values()) / len(utils)
                    # actual memory consumption
                    ma = int(vm_m.get('memory_actual', 0))
                    if ma > 0:
                        mem_actual = ma
                    # uptime from start_time
                    st = vm_m.get('start_time')
                    if st:
                        try:
                            # XAPI returns xmlrpc DateTime - str gives ISO-ish
                            from datetime import datetime as _dt
                            started = _dt.fromisoformat(str(st).replace('T', ' ').split('.')[0])
                            vm_uptime = max(0, int(now - started.timestamp()))
                        except Exception:
                            pass
                except Exception:
                    pass

            # disk size - sum of VBDs -> VDIs
            disk_total = 0
            for vbd_ref in rec.get('VBDs', []):
                try:
                    vbd_rec = api.VBD.get_record(vbd_ref)
                    if vbd_rec.get('type') == 'Disk':
                        vdi_ref = vbd_rec.get('VDI', 'OpaqueRef:NULL')
                        if vdi_ref != 'OpaqueRef:NULL':
                            vdi_size = int(api.VDI.get_virtual_size(vdi_ref))
                            disk_total += vdi_size
                except Exception:
                    pass

            vms.append({
                'vmid': vmid,
                'name': _sanitize_str(rec.get('name_label', '')),
                'status': status,
                'type': 'qemu',  # XCP-ng only has HVM/PV VMs, map to 'qemu' for compat
                'node': _sanitize_str(node_name),
                'cpu': cpu_frac,
                'maxcpu': vcpus_max or vcpus,
                'mem': mem_actual,
                'maxmem': mem_max,
                'disk': 0,
                'maxdisk': disk_total,
                'uptime': vm_uptime,
                'netin': 0,   # needs per-VIF query_data_source, too expensive per-VM
                'netout': 0,
                'template': '',
                'tags': [],
                'lock': '',
                'uuid': vm_uuid,
                '_ref': ref,
            })
        return vms

    def get_vms(self, node=None) -> list:
        if self._cached_vms is not None:
            vms = self._cached_vms
        else:
            api = self._api()
            if not api:
                return []
            vms = self._fetch_vms(api)
            self._cached_vms = vms
            self._vms_cache_time = time.time()

        result = [{k: v for k, v in vm.items() if not k.startswith('_')} for vm in vms]
        if node:
            result = [vm for vm in result if vm.get('node') == node]
        return result

    # ──────────────────────────────────────────
    # Storage
    # ──────────────────────────────────────────

    def get_storages(self, node=None) -> list:
        api = self._api()
        if not api:
            return []
        try:
            sr_refs = api.SR.get_all()
            storages = []
            for ref in sr_refs:
                rec = api.SR.get_record(ref)
                sr_type = rec.get('type', '')
                # skip internal/udev SRs
                if sr_type in ('udev', 'iso'):
                    continue
                total = int(rec.get('physical_size', 0))
                used = int(rec.get('physical_utilisation', 0))
                storages.append({
                    'storage': _sanitize_str(rec.get('name_label', '')),
                    'type': sr_type,
                    'total': total,
                    'used': used,
                    'avail': total - used if total > used else 0,
                    'status': 'available',
                    'shared': bool(rec.get('shared', False)),
                    'content': 'images',
                    'uuid': rec.get('uuid', ''),
                })
            return storages
        except Exception as e:
            self.logger.error(f"get_storages failed: {e}")
            return []

    # ──────────────────────────────────────────
    # Networks
    # ──────────────────────────────────────────

    def get_networks(self, node=None) -> list:
        api = self._api()
        if not api:
            return []
        try:
            net_refs = api.network.get_all()
            nets = []
            for ref in net_refs:
                rec = api.network.get_record(ref)
                # LW: skip internal xapi networks
                if rec.get('name_label', '').startswith('xapi'):
                    continue
                nets.append({
                    'iface': _sanitize_str(rec.get('bridge', rec.get('name_label', ''))),
                    'type': 'bridge',
                    'active': True,
                    'name': _sanitize_str(rec.get('name_label', '')),
                    'uuid': rec.get('uuid', ''),
                })
            return nets
        except Exception as e:
            self.logger.error(f"get_networks failed: {e}")
            return []

    # ──────────────────────────────────────────
    # Cluster status (for dashboard aggregation)
    # ──────────────────────────────────────────

    def get_cluster_status(self) -> dict:
        nodes = self.get_nodes()
        vms = self.get_vms()
        total_cpu = sum(n.get('maxcpu', 0) for n in nodes)
        total_mem = sum(n.get('maxmem', 0) for n in nodes)
        used_mem = sum(n.get('mem', 0) for n in nodes)
        running_vms = len([v for v in vms if v.get('status') == 'running'])
        return {
            'nodes': len(nodes),
            'vms': len(vms),
            'running_vms': running_vms,
            'total_cpu': total_cpu,
            'total_mem': total_mem,
            'used_mem': used_mem,
            'cluster_type': 'xcpng',
        }

    def test_connection(self) -> bool:
        return self.connect()

    # ──────────────────────────────────────────
    # SSE broadcast compat (same shape as PegaProxManager)
    # ──────────────────────────────────────────

    def get_node_status(self) -> dict:
        """Return node metrics keyed by hostname - used by broadcast loop.

        Must match PegaProxManager.get_node_status() output format so the
        frontend doesn't need cluster_type-specific rendering.
        """
        nodes = self._cached_nodes
        if nodes is None:
            nodes = self.get_nodes()
            # get_nodes strips _ref, re-fetch from cache
            nodes = self._cached_nodes or []

        result = {}
        for n in nodes:
            name = n.get('node', '')
            maxmem = n.get('maxmem', 0)
            mem_used = n.get('mem', 0)
            cpu_frac = n.get('cpu', 0)
            mem_pct = round(mem_used / maxmem * 100, 1) if maxmem else 0
            cpu_pct = round(cpu_frac * 100, 1)
            result[name] = {
                'status': n.get('status', 'unknown'),
                'cpu_percent': cpu_pct,
                'mem_used': mem_used,
                'mem_total': maxmem,
                'mem_percent': mem_pct,
                'disk_used': 0,   # XAPI doesn't expose dom0 rootfs usage
                'disk_total': 0,
                'disk_percent': 0,
                'netin': n.get('netin', 0),
                'netout': n.get('netout', 0),
                'uptime': n.get('uptime', 0),
                'score': cpu_pct + mem_pct,
                'maintenance_mode': name in self.nodes_in_maintenance,
                'offline': n.get('status') != 'online',
            }
        return result

    def get_vm_resources(self) -> list:
        """Return VM list for broadcast loop - same format as Proxmox /cluster/resources."""
        return self.get_vms() or []

    # ──────────────────────────────────────────
    # VM Lifecycle
    # ──────────────────────────────────────────

    def _resolve_vm(self, vmid):
        """Resolve a VMID (int or str) to XAPI VM ref."""
        db = get_db()
        vm_uuid = db.xcpng_resolve_vmid(self.id, vmid)
        if not vm_uuid:
            raise ValueError(f"Unknown VMID {vmid} for cluster {self.id}")
        api = self._api()
        if not api:
            raise ConnectionError("Not connected to XCP-ng")
        return api.VM.get_by_uuid(vm_uuid)

    def start_vm(self, node, vmid) -> str:
        api = self._api()
        if not api:
            return None
        ref = self._resolve_vm(vmid)
        # start paused=False, force=False
        task_ref = api.Async.VM.start(ref, False, False)
        task_id = self._track_task(task_ref, 'start_vm', vmid)
        self.logger.info(f"Starting VM {vmid}")
        return task_id

    def stop_vm(self, node, vmid) -> str:
        """Hard shutdown."""
        api = self._api()
        if not api:
            return None
        ref = self._resolve_vm(vmid)
        task_ref = api.Async.VM.hard_shutdown(ref)
        return self._track_task(task_ref, 'stop_vm', vmid)

    def shutdown_vm(self, node, vmid) -> str:
        """Clean shutdown (ACPI)."""
        api = self._api()
        if not api:
            return None
        ref = self._resolve_vm(vmid)
        task_ref = api.Async.VM.clean_shutdown(ref)
        return self._track_task(task_ref, 'shutdown_vm', vmid)

    def reboot_vm(self, node, vmid) -> str:
        api = self._api()
        if not api:
            return None
        ref = self._resolve_vm(vmid)
        task_ref = api.Async.VM.clean_reboot(ref)
        return self._track_task(task_ref, 'reboot_vm', vmid)

    def suspend_vm(self, node, vmid) -> str:
        api = self._api()
        if not api:
            return None
        ref = self._resolve_vm(vmid)
        task_ref = api.Async.VM.suspend(ref)
        return self._track_task(task_ref, 'suspend_vm', vmid)

    def resume_vm(self, node, vmid) -> str:
        api = self._api()
        if not api:
            return None
        ref = self._resolve_vm(vmid)
        # start_paused=False, force=False
        task_ref = api.Async.VM.resume(ref, False, False)
        return self._track_task(task_ref, 'resume_vm', vmid)

    def delete_vm(self, node, vmid, vm_type='qemu', purge=False, destroy_unreferenced=False) -> dict:
        api = self._api()
        if not api:
            return {'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            # must be halted to destroy
            power = api.VM.get_power_state(ref)
            if power != 'Halted':
                api.VM.hard_shutdown(ref)
                # wait briefly
                time.sleep(2)

            # destroy associated VDIs if purge
            if purge:
                vbds = api.VM.get_VBDs(ref)
                for vbd_ref in vbds:
                    try:
                        vbd_rec = api.VBD.get_record(vbd_ref)
                        if vbd_rec.get('type') == 'Disk':
                            vdi_ref = vbd_rec.get('VDI', 'OpaqueRef:NULL')
                            if vdi_ref != 'OpaqueRef:NULL':
                                api.VDI.destroy(vdi_ref)
                    except Exception as e:
                        self.logger.warning(f"Failed to destroy VDI: {e}")

            api.VM.destroy(ref)

            # cleanup vmid mapping
            db = get_db()
            cursor = db.conn.cursor()
            cursor.execute('DELETE FROM xcpng_vmid_map WHERE cluster_id = ? AND vmid = ?',
                          (self.id, int(vmid)))
            db.conn.commit()

            # invalidate cache
            self._cached_vms = None
            self.logger.info(f"Destroyed VM {vmid}")
            return {'success': True}
        except Exception as e:
            self.logger.error(f"delete_vm {vmid} failed: {e}")
            return {'error': str(e)}

    def clone_vm(self, node, vmid, vm_type='qemu', newid=None, name=None, **kwargs) -> dict:
        api = self._api()
        if not api:
            return {'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            clone_name = name or f"clone-of-{vmid}"
            new_ref = api.VM.clone(ref, clone_name)
            new_uuid = api.VM.get_uuid(new_ref)
            db = get_db()
            new_vmid = db.xcpng_get_vmid(self.id, new_uuid)
            self._cached_vms = None
            self.logger.info(f"Cloned VM {vmid} -> {new_vmid} ({clone_name})")
            return {'success': True, 'vmid': new_vmid}
        except Exception as e:
            self.logger.error(f"clone_vm {vmid} failed: {e}")
            return {'error': str(e)}

    def migrate_vm(self, node, vmid, vm_type='qemu', target_node=None, online=True, options=None):
        """Legacy migrate interface (used by auto-balancer). Delegates to migrate_vm_manual."""
        return self.migrate_vm_manual(node, vmid, vm_type, target_node, online, options)

    def migrate_vm_manual(self, node, vmid, vm_type='qemu', target_node=None,
                          online=True, options=None) -> dict:
        """Migrate VM to another host in the XCP-ng pool.

        NS Mar 2026 - pool_migrate for live, shutdown+start for offline.
        """
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected to XCP-ng'}

        if not target_node:
            return {'success': False, 'error': 'Target node is required'}

        try:
            vm_ref = self._resolve_vm(vmid)
            power = api.VM.get_power_state(vm_ref)

            # find target host ref by hostname
            target_ref = None
            for href in api.host.get_all():
                if api.host.get_hostname(href) == target_node:
                    target_ref = href
                    break

            if not target_ref:
                return {'success': False, 'error': f'Host {target_node} not found in pool'}

            # check target is enabled
            if not api.host.get_enabled(target_ref):
                return {'success': False, 'error': f'Host {target_node} is disabled/maintenance'}

            if online and power == 'Running':
                # live migration within same pool
                # MK: options dict is empty for XCP-ng, we just pass the standard XAPI migrate opts
                migrate_opts = {'force': 'true'}  # xenapi wants string bools
                task_ref = api.Async.VM.pool_migrate(vm_ref, target_ref, migrate_opts)
                task_id = self._track_task(task_ref, 'migrate_vm', vmid)
                self.logger.info(f"Live migrating VM {vmid} -> {target_node}")
            elif power == 'Halted':
                # offline: just set affinity + start on target
                api.VM.set_affinity(vm_ref, target_ref)
                task_ref = api.Async.VM.start_on(vm_ref, target_ref, False, False)
                task_id = self._track_task(task_ref, 'migrate_vm', vmid)
                self.logger.info(f"Cold migrating VM {vmid} -> {target_node} (start_on)")
            else:
                # suspended/paused -> shut down first, then move
                api.VM.hard_shutdown(vm_ref)
                time.sleep(2)
                api.VM.set_affinity(vm_ref, target_ref)
                task_ref = api.Async.VM.start_on(vm_ref, target_ref, False, False)
                task_id = self._track_task(task_ref, 'migrate_vm', vmid)
                self.logger.info(f"Migrate VM {vmid} -> {target_node} (shutdown + start_on)")

            self._cached_vms = None
            return {'success': True, 'task': task_id}
        except Exception as e:
            self.logger.error(f"migrate_vm {vmid} -> {target_node}: {e}")
            return {'success': False, 'error': str(e)}

    def _get_templates(self, api) -> list:
        """Fetch available VM templates from pool."""
        templates = []
        for ref in api.VM.get_all():
            try:
                rec = api.VM.get_record(ref)
                if rec.get('is_a_template') and not rec.get('is_control_domain'):
                    templates.append({
                        'uuid': rec.get('uuid', ''),
                        'name': rec.get('name_label', ''),
                        'description': rec.get('name_description', ''),
                        '_ref': ref,
                    })
            except Exception:
                pass
        return templates

    def get_templates(self, node=None) -> list:
        """List VM templates. node param ignored (XCP-ng templates are pool-wide)."""
        api = self._api()
        if not api:
            return []
        tpls = self._get_templates(api)
        return [{k: v for k, v in t.items() if not k.startswith('_')} for t in tpls]

    def create_vm(self, node, vm_config) -> dict:
        """Create VM from template on XCP-ng.

        vm_config keys:
          template   - UUID or name of the source template (required)
          name       - VM name (required)
          vcpus      - number of vCPUs
          memory     - RAM in bytes (or int with 'G' suffix stripped)
          sr         - target SR UUID for disk provisioning
          network    - network UUID or bridge name to attach
          start      - bool, start VM after creation
          description - optional description
        """
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected to XCP-ng'}

        tpl_ident = vm_config.get('template', '')
        vm_name = _sanitize_str(vm_config.get('name', ''))

        if not tpl_ident:
            return {'success': False, 'error': 'Template UUID or name is required'}
        if not vm_name:
            return {'success': False, 'error': 'VM name is required'}

        try:
            # resolve template - try UUID first, then name_label
            tpl_ref = None
            try:
                tpl_ref = api.VM.get_by_uuid(tpl_ident)
            except Exception:
                # not a UUID, search by name
                refs = api.VM.get_by_name_label(tpl_ident)
                for r in refs:
                    if api.VM.get_is_a_template(r):
                        tpl_ref = r
                        break

            if not tpl_ref:
                return {'success': False, 'error': f'Template not found: {tpl_ident}'}

            # NS: security - verify it IS actually a template, not a regular VM someone
            # is trying to clone through the create endpoint
            if not api.VM.get_is_a_template(tpl_ref):
                return {'success': False, 'error': 'Specified VM is not a template'}

            # clone from template
            new_ref = api.VM.clone(tpl_ref, vm_name)

            # provision (instantiates template disks on default SR)
            api.VM.provision(new_ref)

            # description
            desc = vm_config.get('description', '')
            if desc:
                api.VM.set_name_description(new_ref, _sanitize_str(desc))

            # vCPUs
            vcpus = vm_config.get('vcpus')
            if vcpus:
                vcpus = int(vcpus)
                if vcpus < 1 or vcpus > 256:
                    self.logger.warning(f"create_vm: vcpus {vcpus} out of range, clamping")
                    vcpus = max(1, min(vcpus, 256))
                api.VM.set_VCPUs_max(new_ref, str(vcpus))
                api.VM.set_VCPUs_at_startup(new_ref, str(vcpus))

            # memory - accept bytes or string with G suffix
            memory = vm_config.get('memory')
            if memory:
                mem_bytes = int(str(memory).replace('G', '').replace('g', ''))
                # if value looks like GB (< 1024), convert
                if mem_bytes < 4096:
                    mem_bytes = mem_bytes * 1024 * 1024 * 1024
                if mem_bytes < 128 * 1024 * 1024:  # min 128MB
                    mem_bytes = 128 * 1024 * 1024
                s = str(mem_bytes)
                api.VM.set_memory_limits(new_ref, s, s, s, s)

            # target SR - move VDIs if different from template default
            target_sr = vm_config.get('sr')
            if target_sr:
                self._move_vm_disks_to_sr(api, new_ref, target_sr)

            # network - attach VIF to specified network
            net_ident = vm_config.get('network')
            if net_ident:
                self._attach_network(api, new_ref, net_ident)

            # register VMID
            new_uuid = api.VM.get_uuid(new_ref)
            db = get_db()
            new_vmid = db.xcpng_get_vmid(self.id, new_uuid)

            self.logger.info(f"Created VM {new_vmid} ({vm_name}) from template {tpl_ident}")

            # optionally start
            if vm_config.get('start'):
                try:
                    api.Async.VM.start(new_ref, False, False)
                except Exception as e:
                    self.logger.warning(f"Auto-start after create failed: {e}")

            self._cached_vms = None
            return {'success': True, 'vmid': new_vmid, 'uuid': new_uuid}
        except Exception as e:
            self.logger.error(f"create_vm failed: {e}")
            return {'success': False, 'error': str(e)}

    def _move_vm_disks_to_sr(self, api, vm_ref, target_sr_uuid):
        """Move all VDIs of a VM to a different SR. Used during template-based creation."""
        try:
            target_sr = api.SR.get_by_uuid(target_sr_uuid)
        except Exception:
            self.logger.warning(f"Target SR {target_sr_uuid} not found, skipping disk move")
            return

        for vbd_ref in api.VM.get_VBDs(vm_ref):
            try:
                rec = api.VBD.get_record(vbd_ref)
                if rec.get('type') != 'Disk':
                    continue
                vdi_ref = rec.get('VDI', 'OpaqueRef:NULL')
                if vdi_ref == 'OpaqueRef:NULL':
                    continue
                # check if already on target
                current_sr = api.VDI.get_SR(vdi_ref)
                if current_sr == target_sr:
                    continue
                api.VDI.pool_migrate(vdi_ref, target_sr, {})
            except Exception as e:
                # LW: non-fatal, disk stays on original SR
                self.logger.warning(f"Failed to move VDI to target SR: {e}")

    def _attach_network(self, api, vm_ref, net_ident):
        """Attach a VIF to the VM for the specified network."""
        net_ref = None
        try:
            net_ref = api.network.get_by_uuid(net_ident)
        except Exception:
            # try by bridge name or label
            for nref in api.network.get_all():
                rec = api.network.get_record(nref)
                if rec.get('bridge') == net_ident or rec.get('name_label') == net_ident:
                    net_ref = nref
                    break

        if not net_ref:
            self.logger.warning(f"Network {net_ident} not found, skipping VIF attach")
            return

        # find next available device index
        existing_vifs = api.VM.get_VIFs(vm_ref)
        used_devices = set()
        for vif_ref in existing_vifs:
            try:
                used_devices.add(int(api.VIF.get_device(vif_ref)))
            except Exception:
                pass
        device = str(next(i for i in range(10) if i not in used_devices))

        vif_record = {
            'VM': vm_ref,
            'network': net_ref,
            'device': device,
            'MTU': '1500',
            'MAC': '',  # auto-generate
            'other_config': {},
            'qos_algorithm_type': '',
            'qos_algorithm_params': {},
        }
        api.VIF.create(vif_record)

    # ──────────────────────────────────────────
    # VM Config
    # ──────────────────────────────────────────

    def get_vm_config(self, node, vmid, vm_type='qemu') -> dict:
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            rec = api.VM.get_record(ref)

            # build disk list from VBDs
            disks = []
            cdroms = []
            for vbd_ref in rec.get('VBDs', []):
                try:
                    vbd = api.VBD.get_record(vbd_ref)
                    if vbd.get('type') == 'CD':
                        cd_info = {'device': vbd.get('userdevice', ''), 'empty': vbd.get('empty', True)}
                        if not vbd.get('empty') and vbd.get('VDI') != 'OpaqueRef:NULL':
                            cd_info['iso'] = api.VDI.get_name_label(vbd['VDI'])
                        cdroms.append(cd_info)
                    elif vbd.get('type') == 'Disk' and vbd.get('VDI') != 'OpaqueRef:NULL':
                        vdi = api.VDI.get_record(vbd['VDI'])
                        disks.append({
                            'id': vbd.get('userdevice', ''),
                            'size': int(vdi.get('virtual_size', 0)),
                            'used': int(vdi.get('physical_utilisation', 0)),
                            'storage': api.SR.get_name_label(vdi.get('SR', 'OpaqueRef:NULL')),
                            'name': vdi.get('name_label', ''),
                            'uuid': vdi.get('uuid', ''),
                            'bootable': vbd.get('bootable', False),
                        })
                except Exception:
                    pass

            # network interfaces from VIFs
            nets = []
            for vif_ref in rec.get('VIFs', []):
                try:
                    vif = api.VIF.get_record(vif_ref)
                    net_label = api.network.get_name_label(vif.get('network', 'OpaqueRef:NULL'))
                    bridge = api.network.get_bridge(vif.get('network', 'OpaqueRef:NULL'))
                    nets.append({
                        'id': vif.get('device', ''),
                        'mac': vif.get('MAC', ''),
                        'network': net_label,
                        'bridge': bridge,
                        'mtu': vif.get('MTU', '1500'),
                    })
                except Exception:
                    pass

            config = {
                'name': rec.get('name_label', ''),
                'description': rec.get('name_description', ''),
                'memory': int(rec.get('memory_static_max', 0)),
                'vcpus': int(rec.get('VCPUs_max', 0)),
                'vcpus_at_startup': int(rec.get('VCPUs_at_startup', 0)),
                'power_state': rec.get('power_state', ''),
                'os_version': rec.get('os_version', {}),
                'platform': rec.get('platform', {}),
                'uuid': rec.get('uuid', ''),
                'disks': disks,
                'cdroms': cdroms,
                'networks': nets,
                'boot_params': rec.get('PV_args', ''),
                'ha_restart_priority': rec.get('ha_restart_priority', ''),
            }
            return {'success': True, 'config': config}
        except Exception as e:
            self.logger.error(f"get_vm_config {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    def update_vm_config(self, node, vmid, vm_type='qemu', config_updates=None):
        """Update XCP-ng VM configuration.

        Supported config_updates keys:
          name        - VM name (name_label)
          description - VM description
          vcpus       - vCPU count (hot-add if within VCPUs_max, else needs shutdown)
          memory      - RAM in bytes
        """
        if not config_updates:
            return {'success': True, 'message': 'Nothing to update'}

        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected to XCP-ng'}

        try:
            ref = self._resolve_vm(vmid)
            power = api.VM.get_power_state(ref)
            changed = []

            # name
            if 'name' in config_updates:
                new_name = _sanitize_str(config_updates['name'])
                if new_name:
                    api.VM.set_name_label(ref, new_name)
                    changed.append('name')

            if 'description' in config_updates:
                api.VM.set_name_description(ref, _sanitize_str(config_updates['description']))
                changed.append('description')

            # vCPUs
            if 'vcpus' in config_updates:
                vcpus = int(config_updates['vcpus'])
                if vcpus < 1:
                    vcpus = 1
                current_max = int(api.VM.get_VCPUs_max(ref))

                if power == 'Running':
                    if vcpus <= current_max:
                        # hot-change within max - XAPI allows this live
                        api.VM.set_VCPUs_number_live(ref, str(vcpus))
                        changed.append(f'vcpus={vcpus} (live)')
                    else:
                        return {'success': False,
                                'error': f'Cannot hot-add beyond VCPUs_max ({current_max}). '
                                         f'Shut down VM first or increase VCPUs_max while halted.'}
                else:
                    # halted - can change both max and startup
                    api.VM.set_VCPUs_max(ref, str(vcpus))
                    api.VM.set_VCPUs_at_startup(ref, str(vcpus))
                    changed.append(f'vcpus={vcpus}')

            # memory
            if 'memory' in config_updates:
                mem = int(str(config_updates['memory']).replace('G', '').replace('g', ''))
                if mem < 4096:
                    mem = mem * 1024 * 1024 * 1024
                if mem < 128 * 1024 * 1024:
                    mem = 128 * 1024 * 1024
                s = str(mem)

                if power == 'Running':
                    # dynamic range only when running
                    try:
                        api.VM.set_memory_dynamic_range(ref, s, s)
                        changed.append('memory (dynamic)')
                    except Exception as e:
                        # some XCP-ng builds don't support dynamic range change
                        return {'success': False, 'error': f'Cannot change memory while running: {e}'}
                else:
                    api.VM.set_memory_limits(ref, s, s, s, s)
                    changed.append('memory')

            if not changed:
                return {'success': True, 'message': 'No recognized config keys to update'}

            self._cached_vms = None
            self.logger.info(f"VM {vmid} config updated: {', '.join(changed)}")
            return {'success': True, 'message': f'Configuration updated ({", ".join(changed)})'}
        except ValueError as e:
            return {'success': False, 'error': f'Invalid value: {e}'}
        except Exception as e:
            self.logger.error(f"update_vm_config {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    # ──────────────────────────────────────────
    # Snapshots
    # ──────────────────────────────────────────

    def get_snapshots(self, node, vmid, vm_type='qemu') -> list:
        api = self._api()
        if not api:
            return []
        try:
            ref = self._resolve_vm(vmid)
            snap_refs = api.VM.get_snapshots(ref)
            snaps = []
            for sref in snap_refs:
                rec = api.VM.get_record(sref)
                snaps.append({
                    'name': rec.get('name_label', ''),
                    'description': rec.get('name_description', ''),
                    'snaptime': rec.get('snapshot_time', {}).get('value', '') if isinstance(rec.get('snapshot_time'), dict) else str(rec.get('snapshot_time', '')),
                    'uuid': rec.get('uuid', ''),
                })
            return snaps
        except Exception as e:
            self.logger.error(f"get_snapshots {vmid}: {e}")
            return []

    def create_snapshot(self, node, vmid, vm_type='qemu', snapname='', description='', vmstate=False):
        api = self._api()
        if not api:
            return {'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            snap_ref = api.VM.snapshot(ref, snapname)
            if description:
                api.VM.set_name_description(snap_ref, description)
            self.logger.info(f"Created snapshot '{snapname}' for VM {vmid}")
            return {'success': True}
        except Exception as e:
            self.logger.error(f"create_snapshot {vmid}: {e}")
            return {'error': str(e)}

    def delete_snapshot(self, node, vmid, vm_type='qemu', snapname=''):
        api = self._api()
        if not api:
            return {'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            snap_refs = api.VM.get_snapshots(ref)
            for sref in snap_refs:
                if api.VM.get_name_label(sref) == snapname:
                    api.VM.destroy(sref)
                    self.logger.info(f"Deleted snapshot '{snapname}' for VM {vmid}")
                    return {'success': True}
            return {'error': f'Snapshot {snapname} not found'}
        except Exception as e:
            self.logger.error(f"delete_snapshot {vmid}: {e}")
            return {'error': str(e)}

    def rollback_snapshot(self, node, vmid, vm_type='qemu', snapname=''):
        api = self._api()
        if not api:
            return {'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            snap_refs = api.VM.get_snapshots(ref)
            for sref in snap_refs:
                if api.VM.get_name_label(sref) == snapname:
                    api.VM.revert(sref)
                    self.logger.info(f"Reverted VM {vmid} to snapshot '{snapname}'")
                    return {'success': True}
            return {'error': f'Snapshot {snapname} not found'}
        except Exception as e:
            self.logger.error(f"rollback_snapshot {vmid}: {e}")
            return {'error': str(e)}

    # ──────────────────────────────────────────
    # Storage content
    # ──────────────────────────────────────────

    def get_storage_content(self, node, storage) -> list:
        api = self._api()
        if not api:
            return []
        try:
            # find SR by name
            sr_refs = api.SR.get_by_name_label(storage)
            if not sr_refs:
                return []
            sr_ref = sr_refs[0]
            vdi_refs = api.SR.get_VDIs(sr_ref)
            content = []
            for vdi_ref in vdi_refs:
                rec = api.VDI.get_record(vdi_ref)
                content.append({
                    'volid': f"{storage}:{rec.get('uuid', '')}",
                    'name': rec.get('name_label', ''),
                    'size': int(rec.get('virtual_size', 0)),
                    'used': int(rec.get('physical_utilisation', 0)),
                    'format': rec.get('type', 'unknown'),
                })
            return content
        except Exception as e:
            self.logger.error(f"get_storage_content: {e}")
            return []

    # ──────────────────────────────────────────
    # Task tracking
    # ──────────────────────────────────────────

    def _track_task(self, task_ref, action, vmid) -> str:
        """Register an async XAPI task for polling."""
        task_id = str(_uuid.uuid4())[:8]
        with self._task_lock:
            self._active_tasks[task_id] = {
                'ref': task_ref,
                'action': action,
                'vmid': vmid,
                'started': datetime.now().isoformat(),
                'status': 'running',
            }
        return task_id

    def _poll_tasks(self):
        """Check status of active XAPI tasks and clean up old ones."""
        api = self._api()
        if not api:
            return

        now = time.time()
        finished = []
        expired = []
        with self._task_lock:
            for task_id, info in self._active_tasks.items():
                # NS: purge completed/failed tasks after 5 min so dict doesn't grow forever
                if info['status'] in ('completed', 'failed'):
                    try:
                        started = datetime.fromisoformat(info['started'])
                        age = now - started.timestamp()
                        if age > 300:
                            expired.append(task_id)
                    except Exception:
                        expired.append(task_id)
                    continue

                try:
                    status = api.task.get_status(info['ref'])
                    if status == 'success':
                        info['status'] = 'completed'
                        finished.append(task_id)
                        self._cached_vms = None
                        broadcast_sse({'type': 'task', 'task_id': task_id,
                                       'status': 'completed', 'action': info['action']})
                    elif status in ('failure', 'cancelled'):
                        info['status'] = 'failed'
                        err_info = api.task.get_error_info(info['ref'])
                        info['error'] = str(err_info) if err_info else 'Unknown error'
                        finished.append(task_id)
                        broadcast_sse({'type': 'task', 'task_id': task_id,
                                       'status': 'failed', 'action': info['action']})
                except Exception:
                    pass  # task ref might be gone already

            for tid in expired:
                del self._active_tasks[tid]

    def get_tasks(self, limit=50) -> list:
        """Return active/recent tasks in PegaProx format."""
        with self._task_lock:
            tasks = []
            for task_id, info in list(self._active_tasks.items())[-limit:]:
                tasks.append({
                    'upid': task_id,
                    'type': info['action'],
                    'status': info['status'],
                    'vmid': info['vmid'],
                    'starttime': info['started'],
                    'node': self.current_host or '',
                    'user': 'xapi@xcpng',
                })
            return tasks

    # ──────────────────────────────────────────
    # VM action dispatch - NS Mar 2026
    # ──────────────────────────────────────────

    def vm_action(self, node, vmid, vm_type='qemu', action='start', force=False):
        """Dispatch power action - mirrors PegaProxManager interface."""
        dispatch = {
            'start': self.start_vm,
            'stop': lambda n, v: self.stop_vm(n, v) if force else self.shutdown_vm(n, v),
            'shutdown': self.shutdown_vm,
            'reboot': self.reboot_vm,
            'reset': self.reboot_vm,  # XCP-ng has no separate reset, just reboot
            'suspend': self.suspend_vm,
            'resume': self.resume_vm,
        }
        fn = dispatch.get(action)
        if not fn:
            return {'success': False, 'error': f'Unknown action: {action}'}
        try:
            task_id = fn(node, vmid)
            if task_id:
                self.logger.info(f"vm_action {action} on {vmid} -> task {task_id}")
                return {'success': True, 'data': task_id}
            return {'success': False, 'error': f'{action} returned no task'}
        except Exception as e:
            self.logger.error(f"vm_action {action} on {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    # ──────────────────────────────────────────
    # VNC / Console - MK Mar 2026
    # ──────────────────────────────────────────

    def get_vnc_ticket(self, node, vmid, vm_type='qemu'):
        """Get console connection info for XCP-ng VM.

        XAPI exposes consoles via RFB (VNC) or text console.
        We return connection details so the frontend can open a noVNC session.
        """
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected to XCP-ng'}
        try:
            ref = self._resolve_vm(vmid)
            power = api.VM.get_power_state(ref)
            if power != 'Running':
                return {'success': False, 'error': 'VM must be running to open console'}

            console_refs = api.VM.get_consoles(ref)
            rfb_console = None
            for cref in console_refs:
                proto = api.console.get_protocol(cref)
                if proto == 'rfb':
                    rfb_console = cref
                    break

            if not rfb_console:
                return {'success': False, 'error': 'No VNC console available for this VM'}

            # console URL is like https://host/console?ref=OpaqueRef:xxxx
            location = api.console.get_location(rfb_console)
            # extract session ID for auth
            session_ref = api.xenapi._session
            return {
                'success': True,
                'type': 'xcpng_vnc',
                'url': location,
                'session_ref': session_ref,
                'host': self.current_host or self.config.host,
                'port': 443,
            }
        except ValueError as e:
            return {'success': False, 'error': str(e)}
        except Exception as e:
            self.logger.error(f"get_vnc_ticket {vmid}: {e}")
            return {'success': False, 'error': f'Console error: {e}'}

    # ──────────────────────────────────────────
    # Disk management - LW Mar 2026
    # ──────────────────────────────────────────

    def add_disk(self, node, vmid, vm_type='qemu', disk_config=None):
        """Create a new VDI and attach it to the VM via VBD."""
        if not disk_config:
            return {'success': False, 'error': 'No disk config provided'}

        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}

        try:
            ref = self._resolve_vm(vmid)

            # figure out target SR
            sr_uuid = disk_config.get('storage')
            if sr_uuid:
                sr_ref = api.SR.get_by_uuid(sr_uuid)
            else:
                # use first available shared SR, or pool default
                pool_refs = api.pool.get_all()
                sr_ref = api.pool.get_default_SR(pool_refs[0]) if pool_refs else None
                if not sr_ref or sr_ref == 'OpaqueRef:NULL':
                    return {'success': False, 'error': 'No default SR configured and no storage specified'}

            size_gb = int(str(disk_config.get('size', 32)).replace('G', '').replace('g', ''))
            size_bytes = size_gb * 1024 * 1024 * 1024

            vdi_rec = {
                'name_label': disk_config.get('name', f'disk-{vmid}'),
                'name_description': f'Added via PegaProx',
                'SR': sr_ref,
                'virtual_size': str(size_bytes),
                'type': 'user',
                'sharable': False,
                'read_only': False,
                'other_config': {},
            }
            vdi_ref = api.VDI.create(vdi_rec)

            # find next free userdevice slot
            existing_vbds = api.VM.get_VBDs(ref)
            used_devs = set()
            for vbd_ref in existing_vbds:
                try:
                    used_devs.add(api.VBD.get_userdevice(vbd_ref))
                except Exception:
                    pass
            next_dev = '1'
            for i in range(1, 16):
                if str(i) not in used_devs:
                    next_dev = str(i)
                    break

            vbd_rec = {
                'VM': ref,
                'VDI': vdi_ref,
                'userdevice': next_dev,
                'bootable': False,
                'mode': 'RW',
                'type': 'Disk',
                'empty': False,
                'other_config': {},
                'qos_algorithm_type': '',
                'qos_algorithm_params': {},
            }
            api.VBD.create(vbd_rec)

            self._cached_vms = None
            self.logger.info(f"[OK] Added {size_gb}GB disk to VM {vmid} on SR {sr_uuid or 'default'}")
            return {'success': True, 'message': f'Disk added ({size_gb}GB)'}
        except Exception as e:
            self.logger.error(f"add_disk {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    def resize_vm_disk(self, node, vmid, vm_type='qemu', disk=None, size=None):
        """Resize a VDI. Only grow is supported by XAPI."""
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}

        try:
            ref = self._resolve_vm(vmid)
            vbds = api.VM.get_VBDs(ref)

            # disk param can be userdevice number or VDI uuid
            target_vdi = None
            for vbd_ref in vbds:
                vbd_rec = api.VBD.get_record(vbd_ref)
                if vbd_rec.get('type') != 'Disk':
                    continue
                if str(vbd_rec.get('userdevice', '')) == str(disk) or \
                   api.VDI.get_uuid(vbd_rec['VDI']) == str(disk):
                    target_vdi = vbd_rec['VDI']
                    break

            if not target_vdi:
                return {'success': False, 'error': f'Disk {disk} not found on VM {vmid}'}

            # parse size - accept "64G", "64", bytes
            new_size = str(size).replace('G', '').replace('g', '')
            try:
                sz = int(new_size)
                if sz < 4096:  # probably GB
                    sz = sz * 1024 * 1024 * 1024
            except ValueError:
                return {'success': False, 'error': f'Invalid size: {size}'}

            current = int(api.VDI.get_virtual_size(target_vdi))
            if sz <= current:
                return {'success': False, 'error': f'New size must be larger than current ({current // (1024**3)}GB). XAPI does not support shrinking.'}

            api.VDI.resize(target_vdi, str(sz))
            self.logger.info(f"Resized disk {disk} on VM {vmid} to {sz // (1024**3)}GB")
            return {'success': True, 'message': f'Disk resized to {sz // (1024**3)}GB'}
        except Exception as e:
            self.logger.error(f"resize_vm_disk {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    def remove_disk(self, node, vmid, vm_type='qemu', disk_id=None, delete_data=False):
        """Detach VBD and optionally destroy the VDI."""
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}

        try:
            ref = self._resolve_vm(vmid)
            vbds = api.VM.get_VBDs(ref)

            target_vbd = None
            target_vdi = None
            for vbd_ref in vbds:
                vbd_rec = api.VBD.get_record(vbd_ref)
                if vbd_rec.get('type') != 'Disk':
                    continue
                dev = str(vbd_rec.get('userdevice', ''))
                vdi_uuid = api.VDI.get_uuid(vbd_rec['VDI']) if vbd_rec.get('VDI') != 'OpaqueRef:NULL' else ''
                if dev == str(disk_id) or vdi_uuid == str(disk_id):
                    target_vbd = vbd_ref
                    target_vdi = vbd_rec.get('VDI')
                    break

            if not target_vbd:
                return {'success': False, 'error': f'Disk {disk_id} not found'}

            # unplug first if VM is running
            power = api.VM.get_power_state(ref)
            if power == 'Running':
                try:
                    api.VBD.unplug(target_vbd)
                except Exception:
                    pass  # might already be unplugged

            api.VBD.destroy(target_vbd)

            if delete_data and target_vdi and target_vdi != 'OpaqueRef:NULL':
                try:
                    api.VDI.destroy(target_vdi)
                except Exception as de:
                    self.logger.warning(f"VBD removed but VDI destroy failed: {de}")

            self._cached_vms = None
            action_word = 'removed and deleted' if delete_data else 'detached'
            self.logger.info(f"Disk {disk_id} {action_word} from VM {vmid}")
            return {'success': True, 'message': f'Disk {disk_id} {action_word}'}
        except Exception as e:
            self.logger.error(f"remove_disk {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    def move_disk(self, node, vmid, vm_type, disk_id, target_storage, delete_original=True):
        """Move VDI to a different SR. Uses VDI.copy + optional destroy."""
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            vbds = api.VM.get_VBDs(ref)
            src_vdi = None
            src_vbd = None
            for vbd_ref in vbds:
                rec = api.VBD.get_record(vbd_ref)
                if rec.get('type') != 'Disk':
                    continue
                dev = str(rec.get('userdevice', ''))
                if dev == str(disk_id):
                    src_vbd = vbd_ref
                    src_vdi = rec['VDI']
                    break
            if not src_vdi:
                return {'success': False, 'error': f'Disk {disk_id} not found'}

            target_sr = api.SR.get_by_uuid(target_storage)

            # async copy
            task_ref = api.Async.VDI.copy(src_vdi, target_sr)
            tid = self._track_task(task_ref, 'move_disk', vmid)
            # note: caller would need to swap VBD after completion for a full move
            # this is a best-effort approach
            return {'success': True, 'message': 'Disk copy started', 'task': tid}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    # ──────────────────────────────────────────
    # CD-ROM - NS Mar 2026
    # ──────────────────────────────────────────

    def set_cdrom(self, node, vmid, iso_path=None, drive='ide2'):
        """Mount or eject ISO. iso_path should be a VDI UUID on an ISO SR."""
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            vbds = api.VM.get_VBDs(ref)

            # find existing CD VBD
            cd_vbd = None
            for vbd_ref in vbds:
                vbd_rec = api.VBD.get_record(vbd_ref)
                if vbd_rec.get('type') == 'CD':
                    cd_vbd = vbd_ref
                    break

            if iso_path:
                # mount - find the ISO VDI
                vdi_ref = api.VDI.get_by_uuid(iso_path)

                if cd_vbd:
                    # eject first if something is loaded
                    try:
                        if not api.VBD.get_empty(cd_vbd):
                            api.VBD.eject(cd_vbd)
                    except Exception:
                        pass
                    api.VBD.insert(cd_vbd, vdi_ref)
                else:
                    # create a new CD VBD
                    vbd_rec = {
                        'VM': ref, 'VDI': vdi_ref, 'userdevice': '3',
                        'bootable': False, 'mode': 'RO', 'type': 'CD',
                        'empty': False, 'other_config': {},
                        'qos_algorithm_type': '', 'qos_algorithm_params': {},
                    }
                    api.VBD.create(vbd_rec)
                return {'success': True, 'message': 'ISO mounted'}
            else:
                # eject
                if not cd_vbd:
                    return {'success': True, 'message': 'No CD drive found, nothing to eject'}
                try:
                    if not api.VBD.get_empty(cd_vbd):
                        api.VBD.eject(cd_vbd)
                except Exception:
                    pass
                return {'success': True, 'message': 'CD-ROM ejected'}
        except Exception as e:
            self.logger.error(f"set_cdrom {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    # ──────────────────────────────────────────
    # Network interface management - LW Mar 2026
    # ──────────────────────────────────────────

    def add_network(self, node, vmid, vm_type='qemu', net_config=None):
        """Create a VIF and attach to VM."""
        if not net_config:
            return {'success': False, 'error': 'No network config'}
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)

            # resolve network - by UUID, bridge name, or label
            net_ident = net_config.get('bridge') or net_config.get('network')
            if not net_ident:
                return {'success': False, 'error': 'bridge or network required'}

            net_ref = self._find_network(api, net_ident)
            if not net_ref:
                return {'success': False, 'error': f'Network {net_ident} not found'}

            # next free VIF device
            existing = api.VM.get_VIFs(ref)
            used = set()
            for vif in existing:
                try:
                    used.add(api.VIF.get_device(vif))
                except Exception:
                    pass
            dev = '0'
            for i in range(0, 8):
                if str(i) not in used:
                    dev = str(i)
                    break

            vif_rec = {
                'device': dev,
                'network': net_ref,
                'VM': ref,
                'MAC': net_config.get('macaddr', ''),  # empty = auto-generate
                'MTU': str(net_config.get('mtu', 1500)),
                'other_config': {},
                'qos_algorithm_type': '',
                'qos_algorithm_params': {},
            }
            vif_ref = api.VIF.create(vif_rec)

            # plug if VM running
            power = api.VM.get_power_state(ref)
            if power == 'Running':
                try:
                    api.VIF.plug(vif_ref)
                except Exception:
                    pass

            self._cached_vms = None
            return {'success': True, 'message': f'Network interface {dev} added'}
        except Exception as e:
            self.logger.error(f"add_network {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    def update_network(self, node, vmid, vm_type='qemu', net_id=None, net_config=None):
        """Update VIF config. XAPI VIFs are immutable - must destroy and recreate."""
        if not net_config:
            return {'success': True, 'message': 'Nothing to update'}
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            vifs = api.VM.get_VIFs(ref)

            target_vif = None
            for vif_ref in vifs:
                if api.VIF.get_device(vif_ref) == str(net_id):
                    target_vif = vif_ref
                    break

            if not target_vif:
                return {'success': False, 'error': f'VIF device {net_id} not found'}

            old_rec = api.VIF.get_record(target_vif)
            power = api.VM.get_power_state(ref)

            # XAPI VIFs can't be modified in-place, recreate with new settings
            new_net = old_rec['network']
            if net_config.get('bridge') or net_config.get('network'):
                ident = net_config.get('bridge') or net_config['network']
                found = self._find_network(api, ident)
                if found:
                    new_net = found

            if power == 'Running':
                try:
                    api.VIF.unplug(target_vif)
                except Exception:
                    pass
            api.VIF.destroy(target_vif)

            vif_rec = {
                'device': str(net_id),
                'network': new_net,
                'VM': ref,
                'MAC': net_config.get('macaddr', old_rec.get('MAC', '')),
                'MTU': str(net_config.get('mtu', old_rec.get('MTU', '1500'))),
                'other_config': old_rec.get('other_config', {}),
                'qos_algorithm_type': old_rec.get('qos_algorithm_type', ''),
                'qos_algorithm_params': old_rec.get('qos_algorithm_params', {}),
            }
            new_vif = api.VIF.create(vif_rec)

            if power == 'Running':
                try:
                    api.VIF.plug(new_vif)
                except Exception:
                    pass

            self._cached_vms = None
            return {'success': True, 'message': f'Network {net_id} updated'}
        except Exception as e:
            self.logger.error(f"update_network {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    def remove_network(self, node, vmid, vm_type='qemu', net_id=None):
        """Remove a VIF from the VM."""
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}
        try:
            ref = self._resolve_vm(vmid)
            vifs = api.VM.get_VIFs(ref)

            target = None
            for vif_ref in vifs:
                if api.VIF.get_device(vif_ref) == str(net_id):
                    target = vif_ref
                    break

            if not target:
                return {'success': False, 'error': f'VIF {net_id} not found'}

            power = api.VM.get_power_state(ref)
            if power == 'Running':
                try:
                    api.VIF.unplug(target)
                except Exception:
                    pass

            api.VIF.destroy(target)
            self._cached_vms = None
            return {'success': True, 'message': f'Network {net_id} removed'}
        except Exception as e:
            self.logger.error(f"remove_network {vmid}: {e}")
            return {'success': False, 'error': str(e)}

    def _find_network(self, api, ident):
        """Find network ref by UUID, bridge name, or label."""
        # try UUID first
        try:
            return api.network.get_by_uuid(ident)
        except Exception:
            pass
        # try name_label
        refs = api.network.get_by_name_label(ident)
        if refs:
            return refs[0]
        # try bridge match
        for ref in api.network.get_all():
            try:
                if api.network.get_bridge(ref) == ident:
                    return ref
            except Exception:
                pass
        return None

    # ──────────────────────────────────────────
    # Task log / cancel
    # ──────────────────────────────────────────

    def get_task_log(self, node, upid, limit=1000):
        """Return task progress info. XAPI tasks don't have detailed logs like Proxmox."""
        with self._task_lock:
            info = self._active_tasks.get(upid)
        if not info:
            return f"Task {upid} not found"
        lines = [
            f"Action: {info.get('action', '?')}",
            f"Status: {info.get('status', '?')}",
            f"VMID: {info.get('vmid', '?')}",
            f"Started: {info.get('started', '?')}",
        ]
        if info.get('error'):
            lines.append(f"Error: {info['error']}")

        # try to get progress from XAPI
        api = self._api()
        if api and info.get('ref'):
            try:
                progress = api.task.get_progress(info['ref'])
                lines.append(f"Progress: {float(progress) * 100:.0f}%")
            except Exception:
                pass
        return '\n'.join(lines)

    def stop_task(self, node, upid):
        """Cancel a running XAPI task."""
        with self._task_lock:
            info = self._active_tasks.get(upid)
        if not info:
            return False
        api = self._api()
        if not api:
            return False
        try:
            api.task.cancel(info['ref'])
            with self._task_lock:
                info['status'] = 'failed'
                info['error'] = 'Cancelled by user'
            self.logger.info(f"Task {upid} cancelled")
            return True
        except Exception as e:
            self.logger.error(f"stop_task {upid}: {e}")
            return False

    # ──────────────────────────────────────────
    # Maintenance mode - MK Mar 2026
    # ──────────────────────────────────────────

    def enter_maintenance_mode(self, node_name, skip_evacuation=False):
        """Disable host and optionally evacuate VMs.
        XCP-ng host.disable() prevents new VMs from starting.
        host.evacuate() live-migrates all running VMs away.
        """
        api = self._api()
        if not api:
            return None  # compat - PegaProxManager returns a MaintenanceTask but we keep it simple

        try:
            # find host ref by hostname
            host_ref = None
            for href in api.host.get_all():
                if api.host.get_hostname(href) == node_name or \
                   api.host.get_name_label(href) == node_name:
                    host_ref = href
                    break
            if not host_ref:
                self.logger.error(f"[MAINT] Host {node_name} not found")
                return None

            api.host.disable(host_ref)
            self.logger.info(f"[MAINT] Host {node_name} disabled")

            if not skip_evacuation:
                # evacuate is async-ish - XAPI handles it
                try:
                    api.host.evacuate(host_ref)
                    self.logger.info(f"[MAINT] Host {node_name} evacuated")
                except Exception as ev:
                    self.logger.warning(f"[MAINT] Evacuation of {node_name} failed: {ev}")

            self._cached_nodes = None
            return {'status': 'completed', 'node': node_name}
        except Exception as e:
            self.logger.error(f"enter_maintenance {node_name}: {e}")
            return None

    def exit_maintenance_mode(self, node_name):
        """Re-enable a host that was in maintenance."""
        api = self._api()
        if not api:
            return None
        try:
            host_ref = None
            for href in api.host.get_all():
                if api.host.get_hostname(href) == node_name or \
                   api.host.get_name_label(href) == node_name:
                    host_ref = href
                    break
            if not host_ref:
                self.logger.error(f"[MAINT] Host {node_name} not found")
                return None
            api.host.enable(host_ref)
            self._cached_nodes = None
            self.logger.info(f"[MAINT] Host {node_name} re-enabled")
            return {'status': 'completed', 'node': node_name}
        except Exception as e:
            self.logger.error(f"exit_maintenance {node_name}: {e}")
            return None

    def get_maintenance_status(self):
        """Check which hosts are disabled (in maintenance)."""
        api = self._api()
        if not api:
            return {}
        result = {}
        try:
            for href in api.host.get_all():
                hostname = api.host.get_hostname(href)
                enabled = api.host.get_enabled(href)
                if not enabled:
                    result[hostname] = {'status': 'maintenance', 'node': hostname}
        except Exception:
            pass
        return result

    # ──────────────────────────────────────────
    # Node details - NS Mar 2026
    # ──────────────────────────────────────────

    def get_node_details(self, node_name):
        """Get detailed host info - hardware, software, etc."""
        api = self._api()
        if not api:
            return {}
        try:
            host_ref = None
            for href in api.host.get_all():
                if api.host.get_hostname(href) == node_name or \
                   api.host.get_name_label(href) == node_name:
                    host_ref = href
                    break
            if not host_ref:
                return {}

            rec = api.host.get_record(host_ref)
            sw = rec.get('software_version', {})
            cpu_info = rec.get('cpu_info', {})
            bios = rec.get('bios_strings', {})

            metrics_ref = rec.get('metrics', 'OpaqueRef:NULL')
            mem_total = mem_free = 0
            if metrics_ref != 'OpaqueRef:NULL':
                try:
                    m = api.host_metrics.get_record(metrics_ref)
                    mem_total = int(m.get('memory_total', 0))
                    mem_free = int(m.get('memory_free', 0))
                except Exception:
                    pass

            # PCIs
            pci_list = []
            for pci_ref in rec.get('PCIs', []):
                try:
                    prec = api.PCI.get_record(pci_ref)
                    pci_list.append({
                        'id': prec.get('pci_id', ''),
                        'class': prec.get('class_name', ''),
                        'vendor': prec.get('vendor_name', ''),
                        'device': prec.get('device_name', ''),
                    })
                except Exception:
                    pass

            return {
                'hostname': rec.get('hostname', ''),
                'uuid': rec.get('uuid', ''),
                'address': rec.get('address', ''),
                'enabled': rec.get('enabled', True),
                'cpu_model': cpu_info.get('modelname', ''),
                'cpu_count': int(cpu_info.get('cpu_count', 0)),
                'cpu_socket_count': int(cpu_info.get('socket_count', 0)),
                'memory_total': mem_total,
                'memory_free': mem_free,
                'xen_version': sw.get('xen', ''),
                'product_version': sw.get('product_version', ''),
                'product_brand': sw.get('product_brand', 'XCP-ng'),
                'kernel_version': sw.get('linux', ''),
                'build_number': sw.get('build_number', ''),
                'bios_vendor': bios.get('bios-vendor', ''),
                'system_manufacturer': bios.get('system-manufacturer', ''),
                'system_product': bios.get('system-product-name', ''),
                'pci_devices': pci_list[:50],  # cap it
            }
        except Exception as e:
            self.logger.error(f"get_node_details {node_name}: {e}")
            return {}

    # ──────────────────────────────────────────
    # Storage upload (ISO) - NS Mar 2026
    # ──────────────────────────────────────────

    def upload_to_storage(self, node, storage, filename, file_stream, content_type='iso'):
        """Upload an ISO or template to an ISO SR via XAPI HTTP import.
        Uses the /import_raw_vdi endpoint with VDI.create for ISO SRs.
        """
        api = self._api()
        if not api:
            return {'success': False, 'error': 'Not connected'}
        try:
            # find the ISO SR
            sr_refs = api.SR.get_by_name_label(storage)
            if not sr_refs:
                # try UUID
                try:
                    sr_ref = api.SR.get_by_uuid(storage)
                    sr_refs = [sr_ref]
                except Exception:
                    return {'success': False, 'error': f'Storage {storage} not found'}
            sr_ref = sr_refs[0]
            sr_type = api.SR.get_type(sr_ref)

            # for ISO SRs, we use HTTP PUT to the host
            import requests as _req
            session_ref = api.xenapi._session
            host_url = f"https://{self.current_host or self.config.host}"

            if sr_type == 'iso' or content_type == 'iso':
                # XAPI ISO import is via VDI create + HTTP upload
                vdi_rec = {
                    'name_label': _sanitize_str(filename),
                    'name_description': 'Uploaded via PegaProx',
                    'SR': sr_ref,
                    'virtual_size': '0',  # will be set by import
                    'type': 'user',
                    'sharable': False,
                    'read_only': True,
                    'other_config': {},
                }
                vdi_ref = api.VDI.create(vdi_rec)
                vdi_uuid = api.VDI.get_uuid(vdi_ref)

                # HTTP PUT to import endpoint
                url = f"{host_url}/import_raw_vdi?session_id={session_ref}&vdi={vdi_uuid}&format=raw"
                resp = _req.put(url, data=file_stream, verify=False,
                               headers={'Content-Type': 'application/octet-stream'})
                if resp.status_code in (200, 204):
                    self.logger.info(f"Uploaded {filename} to {storage}")
                    return {'success': True, 'message': f'{filename} uploaded'}
                else:
                    # rollback
                    try:
                        api.VDI.destroy(vdi_ref)
                    except Exception:
                        pass
                    return {'success': False, 'error': f'Upload failed: HTTP {resp.status_code}'}
            else:
                return {'success': False, 'error': 'Only ISO upload supported for XCP-ng'}
        except Exception as e:
            self.logger.error(f"upload_to_storage: {e}")
            return {'success': False, 'error': str(e)}

    # ──────────────────────────────────────────
    # Proxmox-specific stubs (no-ops for XCP-ng)
    # ──────────────────────────────────────────

    def _create_session(self):
        """Compat stub - PegaProxManager returns a requests.Session. We don't need this."""
        return None

    def get_last_migration_log(self):
        return []

    def get_pools(self):
        """XCP-ng pools are the cluster itself - return empty for Proxmox pool compat."""
        return []

    def get_pool_members(self, pool_id):
        """Proxmox resource pools don't exist in XCP-ng."""
        return {'members': []}

    def get_iso_list(self, node, storage=None):
        """List ISOs on ISO-type SRs."""
        api = self._api()
        if not api:
            return []
        try:
            isos = []
            for sr_ref in api.SR.get_all():
                sr_type = api.SR.get_type(sr_ref)
                if sr_type != 'iso':
                    continue
                sr_name = api.SR.get_name_label(sr_ref)
                if storage and sr_name != storage:
                    continue
                for vdi_ref in api.SR.get_VDIs(sr_ref):
                    rec = api.VDI.get_record(vdi_ref)
                    isos.append({
                        'volid': f"{sr_name}:iso/{rec.get('name_label', '')}",
                        'name': rec.get('name_label', ''),
                        'size': int(rec.get('virtual_size', 0)),
                        'uuid': rec.get('uuid', ''),
                    })
            return isos
        except Exception as e:
            self.logger.error(f"get_iso_list: {e}")
            return []

    @property
    def nodes(self):
        """Node dict keyed by hostname - needed by metrics collector + search."""
        cached = self._cached_nodes
        if not cached:
            return {}
        return {n['node']: n for n in cached}

    @property
    def host(self):
        return self.current_host or self.config.host
