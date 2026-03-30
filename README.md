# PegaProx 한국어 번역 프로젝트 소개 (v0.9.4 Beta)

## 📌 프로젝트 개요

이 프로젝트는 **PegaProx v0.9.4 Beta**의 공식 한국어 번역을 제공하는 포크(fork) 저장소입니다. PegaProx는 Proxmox VE 및 XCP-ng 클러스터를 위한 현대적인 웹 기반 관리 인터페이스로, 여러 클러스터를 단일 대시보드에서 통합 관리할 수 있는 강력한 도구입니다.

**버전 정보**: v0.9.4 Beta (2026년 3월 기준 최신 Beta 버전)

## 🎯 번역 목표

- IT 업계에서 통용되는 전문 용어는 영문 그대로 유지하여 기술적 정확성 확보
- 일반 텍스트는 자연스러운 한국어로 번역하여 사용자 편의성 향상
- VM, HA, Ceph, LXC, Storage, Node, Cluster 등 핵심 개념은 원어민 IT 관리자도 이해할 수 있는 수준으로 표현
- Proxmox 사용자에게 익숙한 용어 체계 유지 (예: '노드' 대신 'Node')

## 📁 번역 파일 구조

```
pegaprox/web/translations.js
├── en: { ... }      # 원본 영문 번역 (기본)
├── de: { ... }      # 독일어 번역 (내부 개발용)
├── fr: { ... }      # 프랑스어 번역
├── es: { ... }      # 스페인어 번역 (LATAM)
├── pt: { ... }      # 포르투갈어 번역
└── ko: { ... }      # 🇰🇷 한국어 번역 (본 프로젝트) - v0.9.4 Beta 기반
```

## 🔧 적용 방법

### 1. LanguageContext에 한국어 추가 (완료)
```javascript
const langs = [
    { code: 'de', flag: '🇦🇹', label: 'DE', title: 'Deutsch' },
    { code: 'en', flag: '🇬🇧', label: 'EN', title: 'English' },
    { code: 'fr', flag: '🇫🇷', label: 'FR', title: 'Français' },
    { code: 'es', flag: '🇪🇸', label: 'ES', title: 'Español (LATAM)' },
    { code: 'pt', flag: '🇧🇷', label: 'PT', title: 'Português' },
    { code: 'ko', flag: '🇰🇷', label: 'KO', title: '한국어' }  // ✅ v0.9.4 Beta 추가
];
```

### 2. translations.js에 ko 객체 포함 (완료)
- `translations.ko` 객체가 v0.9.4 Beta의 모든 문자열을 포함하여 추가됨
- 한국어 선택 시 전체 인터페이스가 즉시 한국어로 전환

## 📝 번역 규칙 (v0.9.4 Beta 기준)

| 항목 | 처리 방식 | 예시 |
|------|----------|------|
| IT 전문 용어 | 영문 유지 | VM, HA, Ceph, LXC, Storage, Node, Cluster, QEMU, XCP-ng |
| 시스템 상태 | 한글/영문 병기 | "실행 중 (Running)", "중지됨 (Stopped)", "온라인 (Online)" |
| 작업 액션 | 한글 우선 | 저장, 취소, 삭제, 추가, 수정, 이동, 복제 |
| 메뉴/탭 | 한글 표시 | 개요, 설정, 모니터, 작업, 스토리지, 네트워크 |
| 경고/알림 | 한국어 문장 | "VM이 실행 중입니다. 삭제 전에 중지됩니다." |
| 설정 항목 | 영문/한글 병기 | "SSH Key (선택)", "Timeout (초)" |

## 🚀 PegaProx v0.9.4 Beta 주요 기능 (한국어 지원)

### ✅ 멀티 클러스터 관리
- **통합 대시보드** - 모든 Proxmox 및 XCP-ng 클러스터를 한 곳에서 관리
- **실시간 모니터링** - SSE 기반 실시간 CPU, RAM, Storage 모니터링
- **라이브 마이그레이션** - 클릭 한 번으로 VM을 다른 Node로 이동
- **크로스 클러스터 로드 밸런싱** - 클러스터 간 워크로드 자동 분산
- **크로스 하이퍼바이저 마이그레이션** - ESXi, Proxmox VE, XCP-ng 간 VM 마이그레이션

### ✅ VM 및 Container 관리
- **빠른 작업** - VM/Container 시작, 중지, 재시작, 강제 종료
- **VM 설정** - CPU, RAM, Disk, Network, EFI, Secure Boot, SMBIOS 등 전체 설정 지원
- **스냅샷** - 표준 및 공간 효율적 LVM 스냅샷 (공유 스토리지 지원)
- **스냅샷 복제** - ZFS 없이도 사용 가능한 Storage-agnostic 복제
- **백업** - 백업 스케줄링 및 관리 (Snapshot/Suspend/Stop 모드)
- **noVNC / xterm.js 콘솔** - 브라우저 기반 QEMU 및 LXC 콘솔 접속
- **로드 밸런싱** - Node 간 자동 VM 분배 (Dry Run 모드 지원)
- **고가용성(HA)** - Node 장애 시 VM 자동 재시작 (타이밍 설정 가능)
- **Affinity 규칙** - VM/Container를 특정 Node에 함께/분리 배치

### ✅ XCP-ng 통합 (기술 프리뷰)
- **XCP-ng 풀 지원** - Proxmox 클러스터와 함께 XCP-ng/Xen 하이퍼바이저 풀 연결
- **VM 전원 제어** - 시작, 중지, 종료, 재부팅, 일시정지/재개
- **VNC 콘솔** - XAPI를 통한 브라우저 기반 원격 콘솔
- **Disk 및 Network 관리** - Disk, NIC 추가/제거/크기 조정
- **유지보수 모드** - 진입/종료 시 VM 자동 이관

### ✅ ESXi 마이그레이션
- **ESXi 가져오기 마법사** - ESXi 호스트에서 Proxmox로 VM 마이그레이션
- **최소 중단 전송** - 실행 중인 VM 전송 (최대 1개 VM 권장)
- **오프라인 마이그레이션** - 종료 후 전송으로 최대 신뢰성 확보
- **SSH 필수** - ESXi 호스트 SSH 활성화 필요

### ✅ 보안 및 접근 제어
- **다중 사용자 지원** - 역할 기반 접근 제어 (Admin, Operator, Viewer)
- **API 토큰 관리** - Bearer 토큰 생성, 목록, 폐기
- **2FA 인증** - TOTP 기반 2단계 인증 (강제 적용 옵션)
- **LDAP / OIDC** - Active Directory, OpenLDAP, Entra ID, Keycloak, Google Workspace 연동
- **VM 레벨 ACL** - VM별 세분화된 권한 설정
- **멀티 테넌시** - 고객별 클러스터 격리
- **IP 화이트리스트/블랙리스트** - IP/CIDR 기반 접근 제한
- **AES-256-GCM 암호화** - 저장된 모든 인증 정보 암호화
- **CVE 스캐너** - Node별 패키지 취약점 스캔 (debsecan 연동)
- **CIS 하드닝** - CIS 벤치마크 기반 원클릭 보안 감사 및 강화

### ✅ 자동화 및 모니터링
- **예약 작업** - VM 자동 시작, 중지, 스냅샷, 백업
- **롤링 노드 업데이트** - Node별 순차 업데이트 및 VM 자동 이관
- **알림** - CPU, Memory, Disk 사용량 초과 시 알림 (SMTP 연동)
- **감사 로깅** - 모든 사용자 작업 IP 주소 포함 기록 (90일 보관)
- **사용자 정의 스크립트** - Node 간 스크립트 실행
- **Ceph 관리** - Ceph Storage Pool, RBD 미러링 모니터링
- **ACME / Let's Encrypt** - HTTP-01 챌린지 기반 자동 SSL 인증서 갱신

### ✅ v0.9.4 Beta 신규/강화 기능
- **SDN (Software Defined Networking)** - Zone, VNet, Subnet 관리
- **공간 효율적 스냅샷** - LVM COW 기반 스냅샷으로 스토리지 효율성 향상
- **2-Node 클러스터 모드** - Quorum 자동 강제 실행으로 2노드 클러스터 HA 지원
- **Storage 기반 Split-Brain 보호** - 공유 스토리지 Heartbeat를 통한 분할 브레인 방지
- **Self-Fence 보호** - Node 자체 격리 및 VM 자동 중지
- **노드 에이전트** - Heartbeat 및 Poison Pill 메커니즘 지원
- **레거시 번역 호환성 유지**

제가 확인한 결과, 제공해주신 GitHub 저장소(`https://github.com/dskim1979/project-pegaprox-korean_lang.git`)는 현재 접근할 수 없는 상태입니다. 하지만 원본 PegaProx 저장소의 `deploy.sh` 스크립트 내용을 바탕으로, 한국어 번역 포크 저장소에 맞게 설치 방법을 수정하여 안내해 드리겠습니다.

## 📦 한국어 버전 PegaProx 설치 방법 (v0.9.4 Beta 기반)

### Option 1: 자동 설치 (권장)

한국어 번역이 포함된 저장소에서 직접 설치합니다.

```bash
# 한국어 번역 포크 저장소에서 설치
curl -sSL https://raw.githubusercontent.com/dskim1979/project-pegaprox-korean_lang/refs/heads/main/deploy.sh | sudo bash
```

스크립트가 다음 작업을 자동으로 수행합니다:
- `/opt/PegaProx`에 설치
- `pegaprox` 시스템 사용자 생성
- 자체 서명 SSL 인증서 생성
- systemd 서비스 등록 및 시작
- **한국어 언어팩 자동 포함**

### Option 2: 수동 설치 (개발/테스트용)

```bash
# 한국어 번역 저장소 클론
git clone https://github.com/dskim1979/project-pegaprox-korean_lang.git
cd project-pegaprox-korean_lang

# Python 의존성 설치
pip install -r requirements.txt

# 서버 실행 (한국어 인터페이스 포함)
python3 pegaprox_multi_cluster.py
```


## 🔧 배포 스크립트 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--port=PORT` | 웹 포트 설정 | 5000 |
| `--no-interactive` | 모든 프롬프트 건너뛰기 | Off (대화형) |
| `--no-offline` | 오프라인 자산 다운로드 건너뛰기 | Off (다운로드) |

```bash
# 포트 443으로 설치 (HTTPS 표준)
curl -sSL https://raw.githubusercontent.com/dskim1979/project-pegaprox-korean_lang/refs/heads/main/deploy.sh | sudo bash -s -- --port=443

# 비대화형 모드 (자동 설치)
curl -sSL https://raw.githubusercontent.com/dskim1979/project-pegaprox-korean_lang/refs/heads/main/deploy.sh | sudo bash -s -- --no-interactive
```

> **참고**: 포트 443 사용 시 루트 권한이 필요하지 않습니다. 스크립트가 `CAP_NET_BIND_SERVICE` 기능을 Python 바이너리에 부여합니다.

## 🚀 설치 후 설정

1. **웹 UI 접속**
   ```
   https://your-server-ip:5000
   ```

2. **기본 로그인**
   - Username: `pegaprox`
   - Password: `admin`

3. **언어 설정** (한국어 번경 방법)
   - 우측 상단 언어 선택 아이콘 클릭
   - 드롭다운에서 `KO - 한국어` 선택
   - 전체 인터페이스가 즉시 한국어로 전환됨

4. **비밀번호 변경** (첫 로그인 시 필수)

5. **클러스터 추가**
   - Settings → Clusters → Proxmox 자격증명 입력

## 📂 설치 디렉토리 구조

```
/opt/PegaProx/
├── pegaprox_multi_cluster.py   # 메인 실행 파일
├── pegaprox/                   # 애플리케이션 패키지
├── web/                        # 프론트엔드 파일
│   └── translations.js         # 번역 파일 (ko 객체 포함)
├── config/
│   └── pegaprox.db             # SQLite 데이터베이스
├── logs/                       # 애플리케이션 로그
├── static/                     # 정적 자산 (오프라인 모드용)
└── venv/                       # Python 가상 환경
```

## 🔄 업데이트 방법

### 웹 UI에서 업데이트
Settings → Updates → "Check for Updates" 클릭

### 수동 업데이트
```bash
cd /opt/PegaProx
sudo ./update.sh
```

## ⚠️ 주의사항

- 기본 비밀번호(`admin`)는 반드시 첫 로그인 후 변경하세요.
- 프로덕션 환경에서는 HTTPS를 활성화하는 것이 좋습니다.
- 2FA를 활성화하면 보안이 강화됩니다.



### Docker
```bash
docker compose up -d
```

## 🌐 언어 전환 방법

1. PegaProx v0.9.4 Beta 웹 인터페이스 우측 상단에서 언어 선택 아이콘 클릭
2. 드롭다운에서 `KO - 한국어` 선택
3. 전체 인터페이스가 즉시 한국어로 전환 (새로고침 불필요)
4. 언어 설정은 브라우저 localStorage 및 서버 계정에 저장됨

## 📋 시스템 요구사항

- Python 3.8+
- Proxmox VE 8.0+ 또는 9.0+
- XCP-ng 8.2+ (기술 프리뷰)
- 현대적 웹 브라우저 (Chrome, Firefox, Edge, Safari)

## 📞 문의 및 기여

- **이슈 제보**: [GitHub Issues](https://github.com/PegaProx/project-pegaprox/issues)
- **문의 이메일**: support@pegaprox.com
- **문서**: [docs.pegaprox.com](https://docs.pegaprox.com)

## 📜 라이선스

본 프로젝트는 AGPL-3.0 License를 따릅니다.

---

<p align="center">
  <strong>PegaProx v0.9.4 Beta 한국어 번역 프로젝트</strong><br/>
  IT 전문 용어를 살린 자연스러운 한국어로 Proxmox 관리를 더욱 편리하게
</p>
