# Chapter 00 - 개발 환경 설정

FastAPI + Kafka 스터디를 시작하기 전에 필요한 개발 환경을 설정합니다.

---

## 1. Docker Desktop 설치 및 확인

### 설치

- [Docker Desktop 공식 다운로드 페이지](https://www.docker.com/products/docker-desktop/)에서 본인 OS에 맞는 버전을 다운로드합니다.
- 설치 후 Docker Desktop을 실행하고, 상단 상태바(macOS) 또는 시스템 트레이(Windows)에서 Docker 아이콘이 **Running** 상태인지 확인합니다.

### 설치 확인

터미널에서 아래 명령어를 실행합니다.

```bash
docker --version
# 예시 출력: Docker version 27.x.x, build xxxxxxx

docker compose version
# 예시 출력: Docker Compose version v2.x.x
```

두 명령어 모두 버전 정보가 출력되면 정상적으로 설치된 것입니다.

---

## 2. Python 3.12+ 설치 및 가상환경(venv) 사용법

### 설치

- [Python 공식 사이트](https://www.python.org/downloads/)에서 **3.12 이상** 버전을 다운로드하여 설치합니다.
- macOS에서는 Homebrew를 사용할 수도 있습니다:
  ```bash
  brew install python@3.12
  ```

### 설치 확인

```bash
python3 --version
# 예시 출력: Python 3.12.x
```

### 가상환경(venv) 생성 및 활성화

프로젝트마다 독립적인 패키지 환경을 유지하기 위해 가상환경을 사용합니다.

```bash
# 가상환경 생성
python3 -m venv .venv

# 활성화 (macOS / Linux)
source .venv/bin/activate

# 활성화 (Windows PowerShell)
.venv\Scripts\Activate.ps1

# 비활성화
deactivate
```

가상환경이 활성화되면 프롬프트 앞에 `(.venv)`가 표시됩니다.

---

## 3. Docker Compose 기본 명령어

| 명령어 | 설명 |
|---|---|
| `docker compose up` | 컨테이너를 생성하고 시작합니다 |
| `docker compose up -d` | 백그라운드(detached) 모드로 시작합니다 |
| `docker compose down` | 컨테이너를 중지하고 제거합니다 |
| `docker compose ps` | 실행 중인 컨테이너 목록을 확인합니다 |
| `docker compose logs` | 컨테이너 로그를 확인합니다 |
| `docker compose logs -f` | 실시간으로 로그를 스트리밍합니다 |
| `docker compose build` | 이미지를 빌드합니다 |
| `docker compose restart` | 컨테이너를 재시작합니다 |

### 자주 쓰는 조합 예시

```bash
# 이미지 새로 빌드하면서 백그라운드로 실행
docker compose up -d --build

# 볼륨까지 포함하여 완전히 정리
docker compose down -v
```

---

## 4. IDE 추천 - VS Code

[Visual Studio Code](https://code.visualstudio.com/)를 추천합니다. 아래 확장을 설치하면 개발 생산성이 크게 올라갑니다.

### 필수 확장

| 확장 | 설명 |
|---|---|
| **Python** (`ms-python.python`) | Python 언어 지원, 린팅, 디버깅 |
| **Pylance** (`ms-python.vscode-pylance`) | 빠른 타입 체크 및 자동 완성 |
| **Docker** (`ms-azuretools.vscode-docker`) | Dockerfile, Compose 파일 편집 및 컨테이너 관리 |

### 권장 확장

| 확장 | 설명 |
|---|---|
| **Ruff** (`charliermarsh.ruff`) | 빠른 Python 린터 및 포매터 |
| **REST Client** (`humao.rest-client`) | VS Code 내에서 HTTP 요청 테스트 |
| **Thunder Client** (`rangav.vscode-thunder-client`) | GUI 기반 API 테스트 도구 |

---

## 환경 점검 스크립트

이 디렉토리에 포함된 `check-environment.sh` 스크립트를 실행하면 필요한 도구가 올바르게 설치되었는지 한 번에 확인할 수 있습니다.

```bash
./check-environment.sh
```
