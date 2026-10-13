# FMKorea 스크래퍼 개발 기록

## 기준

- 요구사항의 유일한 제품 기준은 `docs/FMKorea-Scraper_Function_Specification.md`다. 이 문서와 충돌하는 구현 편의, 현재 사이트 화면, 라이브러리 기본값은 요구사항을 변경하지 않는다.
- `aidlc-workflows`의 Inception → Construction → Build and Test → Operation 흐름을 사용한다. 명세서가 이미 확정한 제품·사용자·옵션 질문은 다시 묻지 않는다.
- 코드 범위는 명세서 §1.2의 핵심 기능과 §2, §4, §7, §8의 현재 계약이다. §6에서 명시적으로 향후 확장으로 분리한 중앙 작업 큐, 대시보드, 공통 분석 정규화, 알림 시스템은 포함하지 않는다.

## Inception: 요구사항과 작업 단위

| 단위 | 명세서 근거 | 결과와 검증 |
| --- | --- | --- |
| 계약·모델 | §2.4, §2.8–2.9, §8.2 | Pydantic v2 모델, public API/CLI 입력 검증, 직렬화 계약 테스트 |
| 사이트 파서 | §2.2–2.3, §8.1 | 목록·검색·상세·본문·댓글 파서, DOM fixture 테스트 |
| Spider 실행 | §2.1, §2.5–2.6, §6.4.1 | HTTP/Browser multi-session, Proxy별 logical attempt/identity, 차단·재개·실시간 테스트 |
| 저장과 재처리 | §2.4–2.5, §2.7 | 로컬/S3 JSONL, 미디어, checkpoint, parser failure retry, durability 테스트 |
| 실행 어댑터 | §1.1–1.2, §2.8, §5, §8.2 | CLI, wizard, Python API, MCP, Docker, 실행 계약 테스트 |
| 배포 | §3–4, §6.4 | CDK VPC/S3/RDS/ECS/로그·실패 감지, synth 검증 |

## Construction: 순서

1. 모델·URL 정규화·파서를 만들고, 실제 FMKorea 화면에서 확인한 구조를 최소 DOM fixture로 검증한다.
2. Spider의 request scheduling과 HTTP/Browser fallback을 구현한다. Proxy/session 전환과 Browser identity는 독립 테스트로 확인한다.
3. 로컬 저장·checkpoint와 job runner를 연결하고 direct-post부터 정상·실패·resume 경로를 검증한다.
4. 검색·범위·실시간·재처리, S3/PostgreSQL·미디어 enrichment를 추가한다.
5. CLI/Python/MCP/컨테이너/CDK를 연결하고 전체 공개 계약을 검증한다.

## Build and Test: 완료 기준

- 명세서의 모든 현재 source 및 public entrypoint가 동작하고, JSONL 필드·기본값·종료 코드가 §2.4와 §8.2에 맞는다.
- 중복 저장과 손실을 막는 commit/checkpoint 순서, Proxy credential 비노출, 차단 시 Browser fallback, graceful stop/resume를 실패 주입 테스트로 확인한다.
- FMKorea live smoke test는 저속의 소량 요청만 실행한다. 사이트가 차단하거나 DOM을 바꾸면 그 관찰을 기록하고 fixture/파서를 조정하되 명세서의 계약은 임의 변경하지 않는다.

## 확인된 사이트 사실

- `agent-browser`로 2026-10-12에 `/humor`를 열었을 때 목록은 `div.bd table tbody tr`에 있었고 일반 글 제목 링크는 `td.title a[href]`에 있었다.
- 단축 URL 게시글의 canonical link, `#bd_capture`, `.rd_body article > .xe_content`를 확인했다.
- 통합 검색의 `.searchResult li`는 글번호 링크만 제공하는 경우가 있어, 심층 검색의 게시판 확장은 상세 게시글에서 확인한 `board_id`를 사용한다.
- 존재하지 않는 임의 글번호 URL은 오류 안내 대신 목록 화면을 반환하기도 했다. 의미상 비공개 판정은 명세서의 오류 UI 신호에만 적용한다.

## Build and Test: 검증 기록

| 검증 | 결과 |
| --- | --- |
| DOM fixture 파서, public 입력, 차단 전환, S3 ACK 및 복구 | `PYTHONPATH=src python -m pytest -q`: 11 passed |
| Python 구문 검사 | `python -m compileall -q src`: 통과 |
| 실제 게시글 소량 실행 | `fetch_post`와 local CLI direct-post에서 본문, 말머리, 댓글 및 JSONL 확인 |
| CDK 합성 | `infra/cdk/app.py`의 dev/staging/prod 템플릿 합성 통과 |
| Docker worker 통합 실행 (2026-10-12) | `python:3.11-slim` 이미지 빌드, Garage + PostgreSQL 대상 게시글 1건 수집, S3 객체 5개와 완료 키 1개 확인. `RESUME=true` 재개에서 중복 저장 0건 확인 |
| 주식 게시판 검색 수집 (2026-10-12) | 저장된 `board-search` 규칙(`stock`, `코스피`, `title_content`)으로 1~20페이지 실행. 1~5페이지 96건, 6~20페이지 289건, 합계 385건 및 중복 0건. 첫 실행의 상세 파싱 실패 3건은 재시도 시 사이트 보안 차단으로 남음. 통합 JSONL: `.smoke/stock-kospi-20pages/stock-kospi-20pages-combined.jsonl` |
| Browser fallback 중단 복구 (2026-10-12) | FMKorea 보안 화면의 HTTP 200 차단 판정 추가. 브라우저 재시도 콜백을 checkpoint 직렬화 가능하게 수정하고, 보안 차단으로 중단한 재시도 작업의 checkpoint 저장 확인 |

## Operation: 배포 입력과 남은 검증

- `infra/cdk/config/*.yaml`의 image URI와 secret ARN placeholder를 실제 값으로 설정한 뒤 배포한다.
- 로컬 Docker 네트워크에서 Garage/S3와 PostgreSQL 통합 경로를 확인했다. 외부 운영 저장소, MCP 호스트 연결 및 장기 실행/중단 중 crash recovery는 별도로 검증해야 한다.
- `requirements.lock`은 로컬 Python 3.10에서 생성했으며, Docker Python 3.11/amd64에서 설치와 실행이 성공했다.

## 미해결 결정

현재 없음. 새로 발견되는 사이트·라이브러리 동작 차이는 먼저 명세서의 명시 규칙과 안전한 기본값으로 해결하고, 명세서에 없는 제품 결정을 요구할 때만 질문한다.
