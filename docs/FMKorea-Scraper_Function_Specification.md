# FMKorea Scraper Function Specification

에펨코리아 게시판 게시글 및 댓글 스크래퍼 상세 가이드

이 문서는 `FMkorea-Scraper`를 Scrapling 기반으로 재구현하기 위한 기능 및 아키텍처 기준 문서다. 개발자는 이 문서만으로 수집 기능, 실행 형태, 데이터 계약, 차단 대응 정책을 동일하게 재현할 수 있어야 한다.

## 1. 프로젝트 개요
- **목표**
  - 에펨코리아 게시판/검색/게시글/댓글을 수집해 게시글 단위 JSONL로 저장
  - 게시판 검색, 통합 검색, 심층검색, 실시간 수집, 실패 재처리 기능 제공
  - 향후 통합 커뮤니티 스크래핑 시스템에서 개별 커뮤니티 워커로 재사용
- **기술 스택**: `scrapling[fetchers]`, `pydantic`, `python-dotenv`, `json`, `polars`
  - **스크래핑 엔진**: Scrapling `Spider` + multi-session 구조
  - **HTTP 모드**: Spider의 기본 `FetcherSession`으로 목록/검색/상세 요청을 처리
  - **Browser 모드**: Blocking 대응 Browser 엔진은 Scrapling `StealthyFetcher` 계열을 사용한다. Spider에서는 이를 persistent async session인 `AsyncStealthySession`으로 등록하고 `manager.add(..., lazy=True)`로 지연 시작한다. HTTP 단계에서 Blocking으로 판정된 요청은 같은 `Request` 문맥을 유지한 채 Stealthy Browser session으로 전환해 재시도한다.
  - **데이터 저장**: 로컬 JSONL/미디어 파일, 컨테이너 환경의 S3 호환 객체 저장소, PostgreSQL 애플리케이션 Job checkpoint, Scrapling `crawldir` crawler state
- **운영 보조**: `.env` Proxy, Scrapling browser impersonation/stealth header, 요청 속도 제어, parser-stage 실패 재처리

### 1.1. 사용 형태
- 단독 실행형 스크래퍼
  - CLI 또는 설정 파일 기반으로 에펨코리아 게시글/댓글 수집
  - UI는 우선 터미널 기반으로 구성하고, GUI/Web UI는 향후 개선 사항으로 분리
  - 명령형 실행과 선택형 터미널 마법사를 같은 CLI 진입점에서 제공 가능
- Python 라이브러리
  - 다른 Python 코드에서 게시판 검색, 통합 검색, 게시글 수집, 댓글 수집 함수를 import하여 사용
  - CLI/Docker/MCP는 같은 core service를 호출하고, 입출력 어댑터만 분리
- 워커 컨테이너
  - 향후 통합 커뮤니티 스크래핑 시스템에서 에펨코리아 전용 수집 워커로 실행
  - 외부 오케스트레이터가 RabbitMQ 같은 Job Queue를 소비한 뒤 `JobConfig`를 환경 변수 또는 실행 인자로 전달해 one-shot worker 컨테이너 실행
  - JSONL/미디어는 S3 호환 저장소에 저장하고 애플리케이션 Job checkpoint는 PostgreSQL에 저장한다. 정확한 Spider scheduler resume이 필요한 배포만 `FMKOREA_CRAWDIR`로 persistent state root를 지정하며, 이 root 아래에 Scrapling crawler state와 S3 미업로드 staging을 함께 보존한다. 미지정 ephemeral worker는 reconstruction resume을 사용한다.
- MCP 도구
  - 외부 에이전트/자동화 시스템이 에펨코리아 검색, 게시글 수집, 댓글 수집 기능을 호출할 수 있도록 제공
  - MCP 구현은 별도 wrapper로 두고, 스크래퍼 core 로직을 중복 구현하지 않음

### 1.2. 재구현 아키텍처 기준
- 핵심 기능
  - Pydantic 기반 `JobConfig`, 게시글/댓글/본문/미디어/체크포인트/실패/리포트 모델
  - Scrapling `Spider` 기반 request scheduling, callback 처리, 동시성 제어, pause/resume
  - Spider multi-session 기반 HTTP/Browser 이중 실행 경로. Browser fallback은 Scrapling `StealthyFetcher` 계열을 기준으로 구현
  - Scrapling Spider의 `download_delay`, concurrency, blocked retry, AutoThrottle 옵션을 이용한 요청 제어
  - HTTP/Browser session별 timeout, Proxy, 브라우저 식별자 및 cookie lifecycle
  - Browser fallback에서는 readiness 완료 후 Browser cookie와 실제 User-Agent/Chrome major를 `BrowserIdentityCandidate`로 캡처하고, non-blocked 응답에서 freshness sequence 검증을 통과한 candidate만 in-memory `BrowserIdentityState`로 승격한다. HTTP 경로에는 active identity의 cookie를 항상 동기화하되, raw Browser User-Agent는 동일 major의 Scrapling/curl_cffi impersonation profile이 정확히 지원될 때만 함께 적용한다.
  - Scrapling `Selector` 기반 목록/검색/상세/본문/댓글 파싱
  - `is_blocked()`와 `retry_blocked_request()`를 확장해 HTTP 단계의 Blocking 요청을 Scrapling `StealthyFetcher` 계열 Browser session으로 반드시 전환
  - Scrapling `ProxyRotator`로 **새 logical attempt가 HTTP에서 시작할 때** Proxy를 선택하고, 그 HTTP 요청이 Blocking이면 동일 Proxy의 Browser fallback을 시도한다. Browser fallback도 Blocking일 때에만 다음 retry의 새 logical attempt에서 Proxy를 다시 선택한다.
  - Spider scheduler가 목록/검색 → 상세 → 추가 댓글 요청을 `Request`/`Response` callback 흐름으로 관리하고, binary 미디어 다운로드는 §2.4의 별도 정책을 따른다.
  - Browser session은 lazy initialization으로 최초 Browser 전환 시점에만 시작하고 이후 같은 Spider 실행 동안 재사용
  - 대규모 Job은 `Spider.stream()`을 기본 실행 경로로 사용하고 `on_scraped_item()`에서 저장한 item을 반환해 실시간 소비한다. Job runner는 item을 별도 축적하지 않아 Spider의 in-memory `ItemList` 전체 누적을 피한다.
  - 로컬 JSONL/미디어 저장, S3 호환 저장소 저장, 로컬/PostgreSQL Job checkpoint와 Scrapling `crawldir` crawler state 분리
  - CLI, 터미널 마법사, Docker one-shot worker, MCP tool handler, Python import API
  - parser-stage 실패 게시글만 골라 direct-post 경로로 재처리하는 기능
  - AWS CDK 기반 VPC, S3, RDS PostgreSQL, ECS Fargate worker, CloudWatch/EventBridge 인프라 코드
- 제외 범위
  - 사용자 cookie 파일 입력/저장/재사용
  - 스크래퍼 내부 RabbitMQ/SQS consumer
  - fallback 전용 저장소, dashboard, custom metric
- 패키지 구조 원칙
  - application code는 `src/fmkorea_scraper/` 아래에 둔다.
  - public API는 `src/fmkorea_scraper/core/api.py`가 제공한다.
  - CLI는 `src/fmkorea_scraper/adapters/cli.py`, Docker entrypoint는 `src/fmkorea_scraper/adapters/container.py`, MCP는 `src/fmkorea_scraper/adapters/mcp.py`가 담당한다.
  - Spider 실행 엔진은 `spider/`, 파서는 `collect/`와 `parse/`, 저장소는 `outputs/`, 실패 재처리는 `retry/`가 담당한다.

## 2. 주요 기능

### 2.1. 수집 범위

#### 2.1.1. 수집 내용
- 게시판
  - `board_id`: URL의 `mid`
  - `board_name`: 페이지/목록에서 확인 가능한 게시판명
- 글 번호
  - `document_srl`
- 말머리
  - `category.category_id`: URL 또는 말머리 링크의 `category`
  - `category.name`: 목록/상세 페이지에 표시되는 말머리명
  - `category.url`: 말머리 링크 URL을 알 수 있으면 저장
- 제목
- 글쓴이
  - 닉네임
  - 회원 번호: 작성자 링크/클래스의 `member_<member_srl>`에서 파싱 가능한 경우 저장
  - 레벨: 작성자 아이콘 `img.level[alt]`, `img.level[title]`에서 파싱 가능한 경우 저장
- 조회수
- 추천/비추천
  - 저장 필드는 공통 `ReactionInfo`인 `reactions.up_count`, `reactions.down_count`, `reactions.score`를 사용한다.
  - 상세 페이지 상단의 `추천 수` 또는 UI의 포텐 계열 값은 `up_count`로 저장한다.
  - 비추천/방출 계열 값이 페이지에 없거나 빈 값이면 `down_count=0`으로 저장한다.
  - `score`는 항상 `up_count - down_count`다.
- 댓글 수
- 작성일
- 원본 URL
  - 입력 URL과 `.document_address a.a` 또는 canonical URL을 함께 정규화
- 본문 콘텐츠
  - `content.text`: 검색/미리보기/Polars 분석용 순수 텍스트
  - `content.blocks`: 원문 순서를 보존한 텍스트, 이미지, 펨코 자체 동영상, YouTube, X, 일반 링크
- 첨부파일
  - `.rd_file a[data-href]`의 파일명, `file_srl`, 다운로드 URL, 크기/다운로드 수 텍스트
- 댓글 + 덧글
  - `comment_srl`, `comment_id_raw`
  - `parent_comment_srl`, `parent_comment_id_raw`
  - `depth`
  - `source_dom_id`
  - `source_order`
  - `is_best`
  - `badges`
  - `author`, `created_at`, `created_at_raw`, `content`, `reactions`, `permalink`

#### 2.1.2. 수집 방식
- 목록/검색 단계
  - 게시글 후보 링크와 후보 메타데이터만 수집한다.
  - 최종 저장용 제목, 말머리, 글쓴이, 작성일, 조회수, 추천수, 댓글 수, 본문, 댓글은 게시글 상세 페이지에서 다시 파싱한다.
  - 목록 단계의 제목/말머리/작성일/조회수/추천수는 필터링, 중복 제거, 진단 로그용 `PostCandidate.metadata`로만 사용한다.
- 상세 페이지 단계
  - `document_srl` 상세 URL을 열어 최종 게시글 JSON을 생성한다.
  - 본문과 미디어는 `#bd_capture .rd_body article > .xe_content` 또는 `.rd_body .xe_content`를 기준 컨테이너로 파싱한다.
- Scrapling Spider 실행 구조
  - `FMKoreaSpider(Spider)`가 일반 Job 하나의 전체 수집 생명주기를 담당한다. 실시간 Job도 polling마다 Spider를 재생성하지 않고 **하나의 장기 실행 `FMKoreaSpider`**를 Job 종료 시점까지 유지한다.
  - `allowed_domains`는 FMKorea 서비스 도메인(`fmkorea.com` 및 필요한 서브도메인)으로 제한하고, direct-post 입력 URL도 동일한 allowlist로 검증한다.
  - 운영 Spider에서는 Scrapling `development_mode`를 항상 비활성화하며 public CLI/env 옵션으로 노출하지 않는다. 운영 Blocking retry와 rate-limit 정책은 실제 network response 기준으로만 동작해야 한다.
  - 시작 source는 Spider의 start request 생성 단계에서 목록/검색/direct-post `Request`로 변환한다.
  - 목록/검색 callback은 수집 범위 내 `PostCandidate`를 만들고 각 상세 URL을 후속 `Request`로 `yield`한다.
  - 상세 callback은 `MetadataParser`, `ContentParser`, `CommentParser` 결과를 합쳐 Pydantic `PostResult`를 생성한 뒤 `model_dump(mode="json")`으로 dict로 변환해 yield한다. Scrapling callback은 임의 Pydantic 객체가 아니라 `dict | Request | None` 계약을 따른다.
  - **logical attempt = `HTTP(Pn) + Blocking 발생 시 Browser(Pn)`**로 정의한다. `ProxyAttemptCoordinator`가 새 logical attempt마다 `ProxyRotator.get_proxy()`를 한 번 호출한다. 같은 attempt의 Browser fallback과 HTTP/Browser transport retry에서는 Proxy를 재선택하지 않는다.
  - Browser fallback까지 Blocking이면 Scrapling의 남은 blocked-retry budget을 사용해 **다음 HTTP logical attempt**에서 새 Proxy를 선택한다. 성공한 Browser 응답은 해당 request만 완료하며 후속의 새로운 crawl request는 다시 HTTP logical attempt로 시작한다.
  - Proxy마다 HTTP `FetcherSession`과 Browser `AsyncStealthySession`을 각각 lazy 등록하고 cookie jar 및 Browser identity를 Proxy별로 분리한다. 전역 `browser_sticky`/`http_probe` 상태는 사용하지 않는다.
  - `follow_routed()`는 새 logical request의 Proxy/session 선택과 Scrapling `response.follow()`의 위험한 kwargs/meta 상속 방지만 담당하는 얇은 wrapper다.
  - Spider scheduler와 concurrency 설정이 링크 요청의 병렬 실행, 우선순위, 중복 제거를 담당하며 별도 `asyncio.Queue`/게시글 worker pool을 두지 않는다.
  - `ScheduledPostKeyTracker`는 `(site_id, board_id, document_id_raw)` 기준의 run-local logical claim set이다. 목록/검색 callback이 상세 `Request`를 scheduler에 넣기 직전에 atomic claim하고, 이미 claim된 게시글이면 새 상세 request를 만들지 않는다. Scrapling request fingerprint는 URL뿐 아니라 session ID(`sid`) 등에 따라 달라질 수 있으므로 HTTP/Browser session 전환 중 같은 게시글이 논리적으로 중복 예약되는 것을 이 tracker가 막는다.
  - `CompletedKeyTracker`는 `(site_id, board_id, document_id_raw)` 기준으로 **output backend에 durable commit된** 게시글을 다시 저장하지 않도록 한다. `ScheduledPostKeyTracker`와 목적이 다르며, 전자는 현재 run의 상세-request 예약 중복 방지, 후자는 저장 완료 상태의 재수집/재저장 방지를 담당한다.
  - `UnavailableKeyTracker`는 현재 run에서 `404/410` 또는 strong semantic-unavailable로 확정된 게시글 키를 in-memory로 보관하여 realtime polling 중 같은 unavailable 게시글을 반복 요청하지 않도록 한다. 이 tracker는 애플리케이션 checkpoint에 영속화하지 않으므로 process 재시작/resume 시에는 해당 게시글을 다시 확인할 수 있다.
  - 미디어 다운로드 여부는 Job 시작 시 `ScrapeOptions.media_mode`으로 결정한다. `media_mode="url_only"`이면 crawl 결과의 URL/메타데이터만 최종 저장하고, `media_mode="download"`이면 Spider crawl phase 종료 후 **post-crawl media enrichment pipeline**을 실행한다.
  - Spider callback과 `on_scraped_item()`은 binary 미디어를 다운로드하지 않는다. crawl phase에서는 `media[].url`, provider, file metadata 등을 수집하고 `file_path/object_key=null`인 `PostResult`를 staging output에 전달한다. 실제 durability 시점은 output backend별 commit 규칙을 따른다.
  - post-crawl enrichment는 staging JSONL을 읽어 downloadable image/FMKorea video를 다운로드하고 `media[].file_path/object_key`를 채운 뒤 최종 `job_YYYYMMDD_HHMMSS.jsonl`을 생성한다. YouTube/X 외부 임베드는 URL/metadata만 유지한다.
  - 해당 게시글 record가 output backend에서 durable commit된 뒤에만 애플리케이션 Job checkpoint의 `completed_keys`에 반영한다. 로컬 persistent output은 durable file write/flush, S3 호환 output은 immutable segment upload ACK를 commit 기준으로 사용한다. scheduler crawl state는 Scrapling `crawldir` checkpoint가 독립적으로 관리하며 `post_unavailable` 키는 `completed_keys`가 아니라 run-local `UnavailableKeyTracker`에만 넣는다.
  - 저장은 `OutputStorePort`, `MediaStorePort`, `FailureStorePort`, `JobCheckpointStorePort`를 통해서만 수행한다.

#### 2.1.3. 수집 옵션
- 게시글 수집 옵션
  - 특정 게시글 수집
    - `document_srl` 단독 입력
    - `board_id` + `document_srl` 입력
  - 게시판 전체 수집
  - 게시판 말머리 수집
    - `category=<category_id>`를 사용
    - 없는 말머리이거나 해당 게시판에서 유효하지 않으면 실패 처리
  - 특정 기간 수집
    - 링크 수집 중 후보 작성일을 확인해 지정 기간 밖으로 벗어나면 수집 종료
    - 기간 필터링은 목록/검색 단계에서 파싱한 후보 작성일과 후보 `document_srl`을 기준으로 수행한다.
    - 상세 페이지의 최종 작성일은 JSON 저장값으로 다시 파싱하며, 상세 파싱 후 기간 범위를 다시 적용해 제외하지 않는다.
  - 특정 글번호 범위 수집
    - `document_srl` 범위로 필터링
  - 노키워드 기간/글번호 범위 수집
    - 검색 키워드 없이 게시판 목록을 페이지 단위로 순회하며 기간/글번호 조건으로 필터링
  - 실시간 키워드 수집
    - `run --source realtime-keyword`로 활성화한다.
    - `RealtimeKeywordSource`, `RealtimeTermination` 모델을 사용한다.
    - `RealtimeCoordinator`는 Job당 하나의 장기 실행 `FMKoreaSpider`를 생성하고 `stream()`을 한 번만 실행한다. Spider 내부 realtime polling callback은 interruptible wait로 poll interval과 stop/deadline을 함께 감시하고, 계속 실행할 수 있을 때만 다음 검색 `Request`를 `dont_filter=True`로 yield하여 동일 SessionManager/HTTP session/Browser context를 유지한다.
  - 실시간 전체 수집
    - `run --source realtime-all`로 활성화한다.
    - `RealtimeAllSource`, `RealtimeTermination` 모델을 사용한다.
    - `RealtimeCoordinator`는 Job당 하나의 장기 실행 `FMKoreaSpider`를 유지하고, Spider 내부 realtime polling callback은 interruptible wait가 정상 timeout된 경우에만 다음 게시판 목록 `Request`를 `dont_filter=True`로 yield한다. `completed_keys`는 동일 Spider/Job 생명주기 동안 공유되어 새 게시글만 상세 수집한다.
  - 심층검색
    - 통합 검색으로 키워드가 발견되는 게시판 목록을 먼저 만들고, 각 게시판에서 게시판 내 키워드 검색을 수행
    - 통합 검색 페이지 제한을 보완하기 위한 수집 모드
    - 글번호 범위 또는 기간 범위 중 하나 이상을 반드시 입력
  - 파싱 실패 게시글 재처리
    - 전체 Job을 다시 실행하지 않고 기존 `failures.jsonl`의 parser-stage 실패 게시글만 재수집
- 댓글 수집 옵션
  - 댓글 미수집
  - 전체 댓글 수집
  - BEST 댓글만 수집
- 파서 제한 옵션
  - `max_comment_pages`: 추가 댓글 페이지 요청 상한. 값이 없으면 상세 HTML에 포함된 댓글만 파싱한다.
  - `max_comments`: 댓글 저장 개수 상한. 값이 없으면 발견된 정상 댓글을 모두 저장한다.
  - `max_content_blocks`: 본문 content block 저장 개수 상한. 값이 없으면 발견된 정상 block을 모두 저장한다.
  - 위 제한값은 `JobConfig.scrape_options`에 저장해 체크포인트 재개와 parser-stage 실패 재처리에서 동일하게 재현한다.
- 본문 미디어 수집 옵션
  - `media_mode="url_only"`: 이미지/동영상 URL 및 메타데이터만 저장하고 post-crawl downloader를 실행하지 않는다.
  - `media_mode="download"`: crawl phase에서는 URL/메타데이터를 staging에 저장하고, crawl 종료 후 post-crawl enrichment pipeline에서 파일을 다운로드해 최종 JSONL의 `file_path/object_key`를 채운다.
  - YouTube, X 등 외부 임베드는 다운로드하지 않고 URL, provider, id, embed_url만 저장
- 실시간 종료 조건
  - 실시간 수집도 다른 수집 모드와 동일하게 `run --source realtime-keyword|realtime-all`과 `JobConfig.source`로 구성한다. public CLI 이름, 타입, 기본값, 필수 조건 및 validation 계약은 **§8.2.10을 유일한 기준(source of truth)**으로 사용한다.
  - 실시간 Job은 duration 또는 absolute end time 중 정확히 하나의 종료 조건을 가져야 한다.
  - Job의 종료 조건 판단은 `RealtimeCoordinator`와 realtime Spider가 공유하며, 최초 Job 시작 시 `duration_seconds|end_at`을 절대 `realtime_deadline_at`으로 정규화한다.
  - 세션을 유지하기 위한 polling 대기는 Spider의 전용 realtime polling callback에서 **interruptible wait**로 수행한다. 구현은 공용 `stop_event`를 기다리되 `anyio.move_on_after(poll_interval_seconds)` 같은 timeout scope를 사용해 poll timer와 stop signal 중 먼저 도달하는 조건을 처리한다.
  - wait 종료 후 `stop_event`가 set되었거나 `realtime_deadline_at`에 도달했으면 callback은 다음 poll `Request`를 yield하지 않고 종료한다. 정상 timeout으로 poll interval만 경과한 경우에만 다음 poll `Request`를 생성한다.
  - Scrapling engine은 scheduler가 비고 active task가 0이면 crawl을 종료하므로, realtime polling callback이 interruptible wait 중 active task로 남아 동일 crawl과 SessionManager를 유지한다. 일반 상세/목록 callback에 임의 sleep을 넣지 않는다.
  - polling callback 하나가 대기 중 global active task slot 하나를 점유하므로 realtime Job에서는 `concurrent_requests >= 2`를 요구하며, 실제 HTML crawl에 동시에 사용할 수 있는 global slot은 최대 `concurrent_requests - 1`개임을 전제로 한다.
  - realtime polling wait 자체는 Scrapling scheduler queue의 pending `Request`가 아니므로 `crawldir` snapshot에 polling timer를 정확히 표현하지 못한다. 따라서 **realtime Job은 `FMKOREA_CRAWDIR` 유무와 관계없이 process 재시작 시 scheduler-state exact resume을 사용하지 않고**, 애플리케이션 `JobConfig` + `completed_keys` + durable S3 segment state + `realtime_deadline_at` 기반 reconstruction resume을 사용한다. `FMKOREA_CRAWDIR`이 지정된 배포에서는 해당 root의 staging 영역을 crash recovery에 사용할 수 있지만 realtime continuation의 source of truth로 Scrapling pending queue/seen set을 복원하지 않는다. Browser cookie/context도 새 process에서 재획득한다.
  - 종료 조건 도달 시 새 poll 예약을 중단하고 이미 in-flight인 request/callback을 정상 종료한 뒤 finalization/enrichment까지 완료하면 `status="completed"`로 최종 체크포인트를 저장한다.
  - 수동 중단 또는 Browser 전환 및 재시도 후에도 복구되지 않는 오류/차단 발생 시 `status="stopped"` 저장 후 종료한다.

#### 2.1.4. 수집 주기
- 수집 주기는 사용자가 설정 가능하다.
- 일반 수집에서는 Spider의 `download_delay`, blocked retry, AutoThrottle, concurrency 제한을 적용한다.
- 실시간 모드에서는 하나의 장기 실행 Spider가 source의 `poll_interval_seconds`마다 다음 poll `Request`를 생성한다. polling마다 Spider/SessionManager를 종료·재생성하지 않는다.
- 새 게시글만 상세 수집하도록 `(site_id, board_id, document_srl)` 기준 중복을 제거한다.

### 2.2. 사이트 API

#### 2.2.1. 게시글 상세 페이지
- 표준 URL
  - `https://www.fmkorea.com/index.php?mid=<board_id>&document_srl=<document_srl>`
- 단축 URL
  - `https://www.fmkorea.com/<document_srl>`
  - `https://www.fmkorea.com/best/<document_srl>`
- 상세 페이지 요청 후 canonical URL, `.document_address a.a`, 현재 URL을 비교해 `canonical_url`을 정규화한다.
- `board_id`가 URL에 없는 단축 URL은 페이지 내부 링크, canonical, 목록 후보 메타데이터 중 가능한 값을 사용한다.

#### 2.2.2. 게시판 목록 API
- 기본 URL
  - `https://www.fmkorea.com/<board_id>?page=<page>`
  - 또는 `https://www.fmkorea.com/index.php?mid=<board_id>&page=<page>`
- 말머리 필터
  - `category=<category_id>`
- 목록 행 파싱
  - 목록 테이블은 게시판 스킨별 id가 바뀌므로 고정 id보다 `div.bd table tbody tr` 범위에서 탐색한다.
  - 공지/광고 행은 `notice`, `ad_wrapper`, `naver_power_link_search_tr` 계열 클래스를 수집 제외한다.
  - 게시글 링크는 `a.hx[href]`, `td.title a[href]`에서 추출한다.
  - 링크는 최종적으로 `document_srl`이 있는 URL 또는 `/<document_srl>` 형태만 후보로 채택한다.
- 목록 단계 수집값
  - `document_srl`
  - 후보 URL
  - 후보 말머리
  - 후보 제목
  - 후보 댓글 수
  - 후보 작성일
  - 후보 조회수/추천수
  - 위 값은 필터링과 진단용이며 최종 저장값은 상세 페이지에서 다시 파싱한다.

#### 2.2.3. 게시판 내 검색 API
- 대상: 특정 게시판 내 검색
- 기본 링크: `https://www.fmkorea.com/search.php`
- 요청 파라미터
  - 게시판 ID: `mid=<board_id>`
  - 검색옵션: `search_target=<search_target>`
    - 제목 + 내용: `title_content`
    - 요약보기 검색: `title_content_summary`
    - 제목: `title`
    - 내용: `content`
    - 글쓴이: `nick_name`
    - 댓글: `comment`
  - 키워드: `search_keyword=<keyword>`
  - 페이지 번호: `page=<page>`
  - 말머리 필터: `category=<category_id>`
- 수집 순서
  - `page=1`부터 마지막 페이지까지 순차 수집한다.
  - 다음 페이지가 없거나 검색 결과 없음 표시가 나오면 종료한다.
- 게시글 후보 행 파싱
  - `div.bd table tbody tr` 하위에서 후보 행을 찾는다.
  - 공지/광고 행은 제외한다.
  - `a.hx[href]`, `td.title a[href]`의 링크에서 `document_srl`을 추출한다.
  - 검색 단계에서는 후보 링크를 `follow_routed()`로 상세 페이지 후속 `Request`로 yield하고, 최종 저장용 게시글 필드는 상세 페이지에서 다시 수집한다.

#### 2.2.4. 통합 검색 API
- 대상: 전체 게시판 검색
- 기본 링크: `https://www.fmkorea.com/search.php?act=IS`
- 요청 파라미터
  - 검색 키워드: `is_keyword=<keyword>`
  - 검색 대상: `where=<where>`
    - 문서: `document`
    - 댓글: `comment`
  - 문서 검색옵션: `search_target=<search_target>`
    - 제목 + 내용: `title_content`
    - 제목: `title`
    - 내용: `content`
  - 페이지 번호: `page=<page>`
  - 정렬 옵션: `sph_sort=<sort>`
    - 날짜순: `date`
    - 정확도순: `relevance`
    - `sph_sort`는 사이트 URL에서 인식 가능한 파라미터지만 본 명세의 `IntegratedSearchSource`, CLI, Python public request 모델에는 입력 필드로 노출하지 않는다.
    - URL builder는 public 입력에 없는 `sph_sort`를 임의 생성하지 않으며, 정렬 옵션을 공식 기능으로 추가할 때 source schema와 CLI/API 계약을 함께 확장한다.
- 페이지 번호
  - 시작값: 1
  - 최대값: 1000
  - 다음 페이지가 검색 결과 없음 상태이거나 `noResult` 표시가 나오면 마지막 페이지로 판단한다.
- 통합 검색 결과도 후보 링크 수집 단계로만 취급한다.

### 2.3. 게시글 상세 페이지 파싱

#### 2.3.1. 메타데이터 파싱
- 기준 컨테이너: `#bd_capture`
- 작성일
  - `.rd_hd .date`
  - 예: `2026.08.10 21:40`
- 제목
  - `.rd_hd h1 .np_18px_span`
  - fallback: `.rd_hd h1`
- 작성자
  - `.rd_hd .member_plate`
  - 클래스의 `member_<member_srl>`에서 회원 번호를 추출한다.
  - `img.level[alt]`에서 `[레벨:24]` 형식의 레벨을 추출한다.
- 조회수/추천수/댓글 수
  - `.rd_hd .btm_area .side.fr span` 텍스트를 라벨 기준으로 파싱한다.
  - `조회 수 <b>...</b>`, `추천 수 <b>...</b>`, `댓글 <b>...</b>`
- 말머리
  - 상세 페이지 주변의 `a.category[href*="category="]`를 우선 사용한다.
  - 후보 목록에서 전달받은 category 힌트가 있으면 fallback으로 사용한다.
- 첨부파일
  - `.rd_file a[data-href]`
  - `data-href`에서 `file_srl`, `sid`, `mid`를 파싱한다.
  - `title`에서 파일 크기/다운로드 수를 가능한 범위에서 파싱한다.

#### 2.3.2. 본문 콘텐츠 및 미디어 수집
- 조사 기준
  - 이미지/텍스트 예시: `https://www.fmkorea.com/best/10195585295`
  - 펨코 자체 영상 예시: `https://www.fmkorea.com/search.php?mid=football_world&document_srl=10196696065`
  - X 예시: `https://www.fmkorea.com/index.php?mid=best&document_srl=8984251370`
  - YouTube 예시: `https://www.fmkorea.com/search.php?mid=lol&document_srl=10192764813`
- 기준 컨테이너
  - 1순위: `#bd_capture .rd_body article > .xe_content`
  - 2순위: `#bd_capture .rd_body .xe_content`
  - 전역 `.xe_content`는 광고/목록 영역에도 다수 존재할 수 있으므로 단독 기준으로 사용하지 않는다.
- 파싱 원칙
  - Scrapling `Selector`로 기준 컨테이너의 하위 노드를 DOM 순서대로 순회하여 `content.blocks` 배열에 저장한다.
  - 빈 `<p>`, 빈 `<div>`, `<br>`만 있는 노드는 저장하지 않는다.
  - `<!-- __LAZY__ -->`, `BeforeDocument`, `AfterDocument`, 플레이어 UI 텍스트, 첨부파일 표 텍스트는 본문 텍스트로 저장하지 않는다.
  - 각 블록은 공통으로 `order`, `type` 값을 가진다.
  - URL은 프로토콜 생략형 `//...`을 `https://...`로 정규화한다.
- `type="text"`
  - 일반 텍스트 문단 또는 텍스트만 있는 `p`, `div`, `span`
  - `text`에 공백 정규화된 문자열 저장
  - 이미지/동영상/외부 임베드 내부의 플레이어 대체 문구는 제외
- `type="image"`
  - 이미지 후보: 기준 컨테이너 내부 `img[src]`
  - 제외: 펨코 자체 영상 플레이어의 포스터 이미지 `.mejs__poster-img`, `video` 내부 UI 이미지
  - URL 우선순위: 부모 `a.highslide[href]` -> `src`
  - 저장 필드
    - `url`: 정규화된 이미지 URL
    - `original_url`: 부모 `a.highslide[href]`가 있을 경우 원본 이미지 URL
    - `file_srl`: `data-file-srl`
    - `alt`
    - `width`, `height`
    - `download_url`: `.rd_file a[data-href]`에서 같은 `file_srl`로 매칭되는 경우 저장
    - `download`, `file_path`, `object_key`: 다운로드 결과는 top-level `media[]` 항목에만 반영한다.
- `type="fmk_video"`
  - 펨코 자체 업로드 동영상 후보: `.auto_media_wrapper`, `video.mediaelement_video`, `video > source[type="video/mp4"]`
  - 정적 HTML에 `source src`, `poster`, schema.org `meta itemprop` 값이 포함되므로 기본 Scrapling 파서만으로 수집 가능하다.
  - 저장 필드
    - `video_url`: `source[src]` 또는 `meta[itemprop="contentURL"]`
    - `poster_url`: `video[poster]` 또는 `meta[itemprop="thumbnailUrl"]`
    - `file_srl`: `.auto_media_wrapper[data-file-download-url]`에서 파싱
    - `download_url`: `.auto_media_wrapper[data-file-download-url]`
    - `name`: `meta[itemprop="name"]`
    - `upload_date`: `meta[itemprop="uploadDate"]`
    - `description`: `meta[itemprop="description"]`, 없거나 빈 값이면 `null`
    - `width`, `height`
    - `download`, `file_path`, `object_key`: 다운로드 결과는 top-level `media[]` 항목에만 반영한다.
  - 같은 `video`의 하위 `source`는 별도 블록으로 중복 저장하지 않는다.
- `type="youtube"`
  - 정적 HTML 후보
    - 본문 텍스트/링크의 `youtube.com/watch`, `youtu.be/<video_id>`
  - 렌더링 후 후보
    - `iframe[src*="youtube.com/embed"]`
  - Scrapling 기본 구현은 정적 HTML의 YouTube URL을 파싱해 `embed_url`을 직접 생성한다.
  - 저장 필드
    - `video_id`
    - `url`
    - `embed_url`
    - `title`
    - `start_seconds`: `t`, `start` 파라미터가 있으면 초 단위 저장
  - 외부 영상 파일은 다운로드하지 않는다.
- `type="x"`
  - 정적 HTML 후보
    - 본문 텍스트/링크의 `https://x.com/<user>/status/<tweet_id>`
    - `https://twitter.com/<user>/status/<tweet_id>`
  - 렌더링 후 후보
    - `iframe[src*="platform.twitter.com/embed/Tweet.html"]`
  - Scrapling 기본 구현은 정적 HTML에 남아 있는 X URL을 우선 수집한다.
  - 렌더링된 iframe을 참고할 수 있는 환경에서는 `embed_url`도 저장한다.
  - 저장 필드
    - `username`
    - `status_id`
    - `tweet_id`
    - `url`
    - `embed_url`
    - `text`
  - X 첨부 미디어 원본 파일은 다운로드하지 않는다.
- `type="link"`
  - 일반 링크: 기준 컨테이너 내부 `a[href]` 또는 본문 텍스트의 bare URL
  - 단, 이미지 highslide 링크, 펨코 동영상 다운로드 링크, YouTube, X는 별도 타입으로 저장하고 `link` 블록으로 중복 저장하지 않는다.
  - 저장 필드
    - `text`
    - `url`
    - `title`: 링크 미리보기 또는 링크 노드에서 확인 가능한 제목. 없으면 `null`
- `type="attachment"`
  - 첨부파일 목록의 파일을 본문 흐름 안에서 보존해야 할 때 사용하는 block 타입이다.
  - 첨부파일의 기본 저장 위치는 top-level `attachments[]`이며, 본문 DOM 순서에 파일 링크가 명확히 등장하는 경우에만 `AttachmentBlock`으로 표현한다.
  - 저장 필드
    - `attachment`: `file_name`, `url`, `size_text`, `file_srl`, `sid`, `mid`, `download_text`, `file_path`, `object_key`를 가진 `Attachment` 객체
- `type="unknown_embed"`
  - YouTube, X, 펨코 자체 영상, 일반 링크로 분류하지 못한 외부 임베드를 보존하기 위한 fallback block 타입이다.
  - 임베드 내용을 억지로 텍스트로 병합하지 않고, 추후 파서 보강이 가능하도록 안전한 범위의 정보만 저장한다.
  - 저장 필드
    - `provider`: 식별 가능한 provider 명. 모르면 `null`
    - `url`: iframe, embed, object, 링크 등에서 추출 가능한 대표 URL. 없으면 `null`
    - `safe_text`: 스크립트, 광고, UI 문구를 제외하고 안전하게 보존 가능한 대체 텍스트. 없으면 `null`
- `content.text`
  - `content.blocks` 중 텍스트성 블록을 합쳐 저장한다.
  - 포함 대상: `text`, `link.text`, 텍스트만 있는 bare URL
  - 제외 대상: 조회수/추천수/댓글 수/첨부파일 목록/플레이어 UI/광고 텍스트
- top-level `media`
  - `content.blocks`의 `image`, `fmk_video`, `youtube`, `x` 블록만 뽑은 호환/요약 필드로 취급한다.
  - `link`, `attachment`, `unknown_embed`는 기본적으로 top-level `media[]`로 승격하지 않는다.
  - `MediaItem` 필드는 `media_id`, `media_type`, `source_block_order`, `url`, `file_path`, `object_key`, `description`, `provider`, `embed_url`, `file_srl`이다.
- 미디어 파일명
  - 로컬/S3 공통 상대 경로는 `media/<board_id>/<document_srl>/<media_type>/<order>-<filename_from_url>` 형태를 사용한다.
  - 저장소 전체 기준 로컬 경로는 `<base_output_path>/outputs/<job_id>/<run_id>/media/<board_id>/<document_srl>/<media_type>/<order>-<filename_from_url>`이다.
  - S3 object key는 `<S3_PREFIX>/<job_id>/<run_id>/media/<board_id>/<document_srl>/<media_type>/<order>-<filename_from_url>`이다.

#### 2.3.3. 댓글 수집
- 기준 컨테이너: `.fdb_lst_ul`
- 댓글 항목: `.fdb_lst_ul > li`
- 댓글 번호
  - `li[id^="comment_"]`에서 숫자 추출
  - BEST 댓글의 id는 `comment_<comment_srl>_`처럼 끝에 `_`가 붙을 수 있으므로 숫자만 파싱한다.
  - 원본 DOM id는 `source_dom_id`에 별도 저장한다.
- 원본 순서
  - 댓글 페이지/DOM에서 등장한 순서를 `source_order`로 저장한다.
  - Polars 분석 시에는 배열 순서에 의존하지 않고 `created_at`, `comment_srl`, `source_order` 기준으로 명시 정렬한다.
- BEST 댓글 여부
  - `li.comment_best` 또는 내부 `.icon-hit` 텍스트 `BEST`
  - BEST 댓글도 일반 댓글과 동일하게 `comments` 배열에 저장하고 별도 `best_comments` 배열을 만들지 않는다.
  - `is_best=true`로 저장한다.
  - 원문 표시 보존을 위해 `badges=["BEST"]`로 저장한다.
- 작성자
  - `.meta .member_plate`
  - `data-comment_srl`은 댓글 번호 후보로 사용 가능
  - 클래스 `member_<member_srl>`에서 작성자 회원 번호 추출
- 작성일
  - `.meta .date`
  - 상대 시간과 절대 시간이 섞일 수 있으므로 원문 문자열과 정규화 결과를 함께 저장한다.
- 내용
  - 댓글 본문 영역은 `.xe_content`, `.comment_content`, `.comment-content` 순으로 찾는다.
  - 댓글 본문 영역의 텍스트를 저장한다.
  - 이미지콘 구매 아이콘 등 장식 텍스트는 제외한다.
  - 댓글 내부 이미지의 `alt`가 있으면 텍스트 내용 뒤에 함께 보존한다.
  - 작성자/날짜/추천/비추천/주소복사/댓글로 가기 UI 텍스트는 제외
- 추천/비추천
  - 기준 컨테이너: `li[id^="comment_"] > div.fdb_nav.img_tx span.vote`
  - 추천 수: `a[title="추천"] .voted_count`
  - 비추천 수: `a[title="비추천"] .blamed_count`
  - 값이 빈 문자열이면 `0`으로 정규화한다.
  - `reactions.up_count`, `reactions.down_count`, `reactions.score=up_count-down_count`로 저장한다.
- 덧글
  - `data-parent` 또는 `data-parent-comment-srl`이 있으면 `parent_comment_srl`로 저장한다.
  - 부모 댓글 번호가 있거나 `li` class에 `reply`가 있으면 `depth=1`로 저장한다.
  - 명확한 부모를 찾지 못한 댓글은 `depth=0`, `parent_comment_srl=null`로 저장한다.
  - 별도 원본 class 필드는 두지 않고, 필요 시 공통 `metadata` 확장 필드에 제한된 JSON 값으로 보존한다.
- 댓글 추가 페이지
  - 상세 HTML에 포함된 댓글은 항상 먼저 파싱한다.
  - `max_comment_pages`가 `null`이면 추가 댓글 페이지 요청을 하지 않는다.
  - `max_comment_pages`가 지정되면 `.fdb_nav.comment_page a[href]`, `.comment_page a[href]`, `.comment-page a[href]`에서 댓글 페이지 링크를 찾는다.
  - 링크 텍스트, class, href 중 `cpage`, `comment_page`, `commentpage` 표식이 있는 링크만 추가 댓글 페이지 후보로 사용한다.
  - 현재 게시글과 다른 `document_srl`로 이동하는 링크는 제외한다.
  - 이미 파싱한 댓글과 추가 페이지 댓글은 `comment_srl` 또는 `source_dom_id` 기준으로 중복 제거한다.

### 2.4. 데이터 저장 JSONL
- 각 run의 최종 게시글 데이터는 `job_YYYYMMDD_HHMMSS.jsonl` 하나로 저장한다. 동일 Job을 resume하면 새 `run_id`와 새 timestamp 기반 파일을 생성하며 기존 JSONL에 이어 쓰지 않는다.
- 게시글 하나는 JSON 객체 한 줄로 append한다.
- Job 실행 조건과 요약 정보는 별도 metadata JSON 파일로 저장 가능하다.
- 이미지/동영상 파일은 별도의 `media/` 폴더 또는 S3 호환 객체 저장소에 저장한다.
- 컨테이너 운영 시 JSONL과 미디어는 S3 호환 객체 저장소에 저장하고 애플리케이션 Job checkpoint는 PostgreSQL에 저장한다. scheduler-state exact resume이 필요한 경우에만 §8.2.11의 `FMKOREA_CRAWDIR`에 persistent volume root를 지정하고, crawler state와 S3 미업로드 staging을 같은 root 아래에 함께 둔다.
- 기본 S3 호환 저장소는 Garage로 확장하되, SeaweedFS로 바꿀 수 있게 S3 호환 설정만 사용한다.
- 파일명
  - `job_YYYYMMDD_HHMMSS.jsonl`
  - 예: `job_20260811_120000.jsonl`
  - `YYYYMMDD_HHMMSS`는 해당 run 시작 시각을 기준으로 결정하고 run 생명주기 동안 바꾸지 않는다.
- 로컬 출력 구조
  - `<base_output_path>/outputs/<job_id>/<run_id>/job_YYYYMMDD_HHMMSS.jsonl`
  - `<base_output_path>/outputs/<job_id>/<run_id>/staging/posts.raw.jsonl` (`media_mode="download"` 또는 finalization 전 내부 staging)
  - `<base_output_path>/outputs/<job_id>/<run_id>/failures.jsonl`
  - `<base_output_path>/outputs/<job_id>/<run_id>/report.json`
  - `<base_output_path>/outputs/<job_id>/<run_id>/media/...`
  - `<base_output_path>/Checkpoint.json`
- S3 호환 출력 구조
  - `<S3_PREFIX>/<job_id>/<run_id>/job_YYYYMMDD_HHMMSS.jsonl`
  - `<S3_PREFIX>/<job_id>/<run_id>/failures.jsonl`
  - `<S3_PREFIX>/<job_id>/<run_id>/report.json`
  - `<S3_PREFIX>/<job_id>/<run_id>/manifest.json`
  - `<S3_PREFIX>/<job_id>/<run_id>/segments/posts/part-*.jsonl`
  - `<S3_PREFIX>/<job_id>/<run_id>/segments/failures/part-*.jsonl`
  - `<S3_PREFIX>/<job_id>/<run_id>/media/...`
  - `S3_PREFIX`가 없으면 `fmkorea` prefix를 기본 운영 예시로 사용한다.
- 경로 및 object key 정규화 규칙
  - `job_id`, `run_id`, `board_id`, `document_srl`, `media_type`, 파일명, `S3_PREFIX`의 각 세그먼트는 파일 시스템과 S3 key에서 안전하게 사용할 수 있도록 정규화한다.
  - 허용 문자는 영문 대소문자, 숫자, `.`, `_`, `-`이며, 그 외 문자는 `_`로 치환한다.
  - 세그먼트 앞뒤의 `.`, `_`, 공백은 제거한다.
  - 빈 문자열, `.`, `..`은 fallback 값으로 대체한다.
  - 세그먼트 길이는 기본 160자, 파일명은 220자 이내로 제한한다.
  - 로컬 저장 시 최종 경로가 `base_output_path` 밖으로 벗어나면 저장소 오류로 처리해 path traversal을 차단한다.
  - URL에서 파일명을 얻지 못하면 `media.bin`을 fallback 파일명으로 사용한다.
- S3 JSONL 저장 동작
  - `S3OutputStore`는 crawl phase의 로컬 staging spool `posts.raw.jsonl`과 `failures.jsonl`을 append하되, **게시글 한 줄마다 커지는 전체 object를 다시 업로드하지 않는다.**
  - 실행 중 durability를 위해 immutable JSONL segment를 batch 단위로 업로드한다. 기본 flush 기준은 **100 records**, **8 MiB**, **30초** 중 먼저 도달하는 조건이며, 게시글 수 기준 `N`의 권장 기본값은 `100`이다.
  - records/bytes 임계값은 `append_post()`가 즉시 검사할 수 있지만, **30초 time threshold는 새 item 유입이 없어도 동작해야 하므로 application runner가 `OutputFlushCoordinator` 역할의 periodic task를 하나 유지한다.** 별도 worker pool이나 queue를 만들지 않고 기존 runner task group 안의 경량 timer coroutine으로 구현한다.
  - periodic flush task는 최대 `s3_flush_seconds` 간격으로 `OutputStorePort.flush_pending()`을 호출하고, 새로 durable commit된 `committed_keys`를 `completed_keys`에 반영한 뒤 애플리케이션 checkpoint를 갱신한다. pending record가 없으면 no-op이다.
  - `S3OutputStore`는 concurrent `on_scraped_item() -> append_post()`와 periodic `flush_pending()`이 같은 open segment, byte/record counter, `pending_keys`, segment sequence를 동시에 변경하지 못하도록 **store 내부 단일 `anyio.Lock`**으로 mutable batch state를 보호한다. 별도 application queue/worker를 만들지 않는다.
  - lock 안에서는 local spool append, open-batch record/key/counter 변경, flush 대상 batch detach/segment number 예약까지만 수행하고, 이미 닫힌 immutable batch의 S3 upload처럼 오래 걸리는 원격 I/O는 가능하면 lock 밖에서 수행한다. upload가 성공한 batch만 durable `committed_keys`로 반환한다.
  - 동일 `flush_pending()` 대상에 대해 periodic flush와 shutdown final flush가 겹치지 않도록 graceful stop, normal crawl completion, terminal blocked 종료에서는 **periodic task에 stop을 요청한 뒤 해당 task가 완전히 종료(join)된 것을 확인하고** 최종 `flush_pending()`을 한 번 수행한다. 그 ACK 결과를 `completed_keys`/checkpoint에 반영한 후 finalization 또는 enrichment로 넘어간다.
  - open segment에 들어간 post key는 `pending_keys`로 취급한다. local spool append만 성공한 상태에서는 해당 key를 애플리케이션 `completed_keys`에 넣지 않는다.
  - 게시글 segment key 예시는 `<S3_PREFIX>/<job_id>/<run_id>/segments/posts/part-000001.jsonl`, `part-000002.jsonl` 순서다. failure도 `segments/failures/part-*.jsonl` 형식을 사용한다.
  - segment는 한 번 업로드한 뒤 같은 key를 덮어쓰지 않는 immutable object로 취급한다. S3 object append API에 의존하지 않는다.
  - immutable post segment upload가 성공하면 그 segment에 포함된 `pending_keys`를 한 번에 `completed_keys`로 승격하고 이후 애플리케이션 checkpoint를 갱신한다. upload 실패 시 해당 key는 pending 상태로 남기고 completed로 기록하지 않는다.
  - S3 segment upload가 일시적으로 실패한 경우 해당 closed batch와 `pending_keys`를 유실하지 않고 다음 periodic/final flush에서 다시 upload할 수 있는 pending 상태로 유지한다. 반면 local spool append/flush 자체 실패, persistent staging write 실패, 반복/최종 S3 flush 실패로 durability를 확보할 수 없는 상태는 systemic storage failure로 처리한다.
  - `media_mode="url_only"`이면 `finalize()`가 staging post records를 그대로 `job_YYYYMMDD_HHMMSS.jsonl`로 확정한다.
  - `media_mode="download"`이면 Spider 종료 후 `MediaEnrichmentPipeline`이 staging post records를 읽어 미디어 다운로드 결과를 반영한 `job_YYYYMMDD_HHMMSS.jsonl`을 생성한다.
  - 최종 게시글 JSONL과 `failures.jsonl`은 각각 최종 object key에 **한 번만** 업로드하고, `manifest.json`에 segment 목록, final object key, record count, byte count, enrichment 상태를 기록한다.
  - 최종 object 업로드와 manifest 기록이 성공한 뒤 중간 segment는 보존 또는 삭제할 수 있으며 기본 정책은 디버깅/복구를 위해 보존한다. 저장 비용이 중요한 배포에서는 lifecycle rule로 일정 기간 후 segment를 삭제할 수 있다.
  - flush 임계값은 `StorageConfig`의 `s3_flush_records=100`, `s3_flush_bytes=8388608`, `s3_flush_seconds=30.0`으로 둔다. `s3_flush_records`는 1 이상, byte/seconds 값은 0보다 커야 한다.
  - 이 구조는 Spider item 처리마다 원격 object 전체를 다시 전송하지 않으므로 대규모 crawl에서 S3 전송량과 write amplification을 크게 줄인다.
- 미디어 다운로드 저장 동작
  - `media_mode="url_only"`이면 `media[]`의 URL/메타데이터만 남기고 파일 다운로드는 수행하지 않는다.
  - `media_mode="download"`이면 Spider crawl phase가 끝난 뒤 `MediaEnrichmentPipeline`을 실행한다. 따라서 binary 다운로드는 Spider callback, scheduler concurrency, Browser fallback, `crawldir` lifecycle과 분리된다.
  - `MediaEnrichmentPipeline`은 standalone Scrapling `FetcherSession` 기반 binary downloader를 사용하고 `ScrapeOptions.media_download_concurrency` limiter로 동시성을 제한한다. 이 session은 crawl Spider의 `SessionManager`와 공유하지 않는다.
  - enrichment run 하나에서는 `FetcherSession` manager를 **한 번만** `async with`로 enter해 얻은 active async session/connection pool을 모든 media task가 공유한다. 동일 `FetcherSession` manager instance를 여러 concurrent task가 각각 중첩 `async with`로 다시 enter하지 않는다.
  - media task는 하나의 active session에서 `await session.get(...)`을 병렬 호출하되 limiter가 최대 in-flight 수를 제한한다. 같은 session의 connection pool/cookie state를 재사용하고 media 하나마다 새 `FetcherSession`을 만들지 않는다.
  - 미디어 다운로드 실패 시 해당 `MediaItem.file_path/object_key`는 `null`로 유지하고 media-stage warning/failure를 기록한 뒤 해당 게시글은 최종 JSONL에 계속 포함한다.
  - crawl phase의 `completed_keys`는 backend별 durable commit 이후 갱신한다. 로컬 persistent output은 durable staging write/flush, S3 호환 output은 immutable segment upload ACK를 기준으로 한다. enrichment 실패 때문에 이미 committed된 게시글 HTML을 다시 crawl하지 않으며, 미완료 미디어는 enrichment 단계에서 별도로 재시도할 수 있다.
  - realtime Job에서 `media_mode="download"`이면 장기 실행 Spider가 종료 조건에 도달해 crawl phase가 끝난 뒤 enrichment를 시작한다. polling 사이에는 media download를 수행하지 않아 HTTP/Browser crawl session에 영향을 주지 않는다.
  - 동일 미디어 URL이 여러 게시글에서 반복될 수 있으므로 미디어 저장소는 URL hash 또는 content hash 기반 중복 제거를 지원하고, 게시글별 `media[]`에는 실제 저장된 `file_path` 또는 `object_key`를 연결한다.
  - 저장 결과는 `content.blocks[]`에 다시 전파하지 않는다. `content.blocks[]`는 본문 DOM에서 파싱한 원본 순서/URL/메타데이터 보존용이고, 실제 저장 결과 확인은 `media[]`를 기준으로 한다.
- 게시글 JSONL schema
  - `schema_version="fmkorea.post.v1"`
  - Pydantic 모델 기준 최상위 타입은 `PostResult`
  - `MetadataModel`을 상속한 모든 객체는 `metadata` 필드를 가진다.
  - JSONL writer는 `model_dump(mode="json")` 결과를 기록하므로 `null`, 빈 배열, 기본값 필드를 임의로 제외하지 않는다.
  - 아래 JSON 예시는 읽기 쉬운 대표 구조이며, 실제 구현 기준은 각 Pydantic 모델 필드 전체다.
  - 시간 필드는 `scraped_at`, `created_at`, `created_at_raw`를 사용한다.
  - 글 번호는 `document_srl` 숫자와 `document_id_raw` 문자열을 함께 저장한다.
  - 추천/비추천은 공통 `ReactionInfo` 구조인 `up_count`, `down_count`, `score`로 저장한다. UI의 포텐은 `up_count`, 방출은 `down_count`에 대응한다.
  - FMKorea 원본 JSON에는 `is_repost`, `repost` 필드를 두지 않는다. 향후 공통 정규화 단계에서 필요한 경우 `null` 또는 기본값으로 보강한다.

#### 2.4.1. JobConfig 예시
```json
{
  "job_id": "job_20260811_120000",
  "site_id": "fmkorea",
  "job_type": "board_search",
  "source": {
    "type": "board_search",
    "board_id": "football_world",
    "category_id": "233499071",
    "keyword": "검색 키워드",
    "search_target": "title_content",
    "page_start": 1,
    "page_end": 5,
    "range_filter": {
      "date_from": "2026-08-01",
      "date_to": "2026-08-11",
      "min_document_srl": 10190000000,
      "max_document_srl": 10199999999
    }
  },
  "scrape_options": {
    "comment_mode": "all",
    "media_mode": "url_only",
    "media_download_concurrency": 2,
    "include_attachments": true,
    "max_comment_pages": 3,
    "max_content_blocks": null,
    "max_comments": null
  },
  "crawl_config": {
    "concurrent_requests": 4,
    "concurrent_requests_per_domain": 2,
    "download_delay": 1.0,
    "max_blocked_retries": 3,
    "autothrottle_enabled": false,
    "autothrottle_start_delay": 2.0,
    "autothrottle_max_delay": 30.0,
    "browser_max_pages": 2,
    "checkpoint_interval_seconds": 300.0,
    "robots_txt_obey": false
  },
  "storage_config": {
    "output_mode": "local",
    "job_checkpoint_mode": "local",
    "base_output_path": ".",
    "s3_flush_records": 100,
    "s3_flush_bytes": 8388608,
    "s3_flush_seconds": 30.0,
    "s3": null,
    "postgres": null
  },
  "created_at": "2026-08-11T12:00:00+09:00"
}
```

#### 2.4.2. JobConfig.source 모드별 예시
아래 예시는 `JobConfig.source` 필드에 들어가는 실제 discriminated union 구조다.

```json
{
  "type": "direct_post",
  "urls": [
    "https://www.fmkorea.com/best/10195585295",
    "https://www.fmkorea.com/index.php?mid=stock&document_srl=10197961451"
  ]
}
```

```json
{
  "type": "board_list",
  "board_id": "stock",
  "category_id": null,
  "page_start": 1,
  "page_end": 5,
  "range_filter": null
}
```

```json
{
  "type": "board_search",
  "board_id": "stock",
  "category_id": null,
  "keyword": "삼전",
  "search_target": "title_content",
  "page_start": 1,
  "page_end": 5,
  "range_filter": {
    "date_from": "2026-08-01",
    "date_to": "2026-08-11",
    "min_document_srl": null,
    "max_document_srl": null
  }
}
```

```json
{
  "type": "integrated_search",
  "keyword": "삼전",
  "search_target": "title_content",
  "page_start": 1,
  "page_end": 10,
  "range_filter": null
}
```

```json
{
  "type": "deep_search",
  "keyword": "삼전",
  "seed_search_target": "title_content",
  "seed_page_limit": 20,
  "board_ids": null,
  "range_filter": {
    "date_from": "2026-08-01",
    "date_to": "2026-08-11",
    "min_document_srl": null,
    "max_document_srl": null
  }
}
```

```json
{
  "type": "no_keyword_range",
  "board_id": "stock",
  "category_id": null,
  "range_filter": {
    "date_from": "2026-08-01",
    "date_to": "2026-08-11",
    "min_document_srl": null,
    "max_document_srl": null
  }
}
```

```json
{
  "type": "realtime_keyword",
  "scope": "board",
  "board_id": "stock",
  "keyword": "삼전",
  "search_target": "title_content",
  "poll_interval_seconds": 30.0,
  "termination": {
    "duration_seconds": 3600,
    "end_at": null
  }
}
```

```json
{
  "type": "realtime_all",
  "board_id": "stock",
  "category_id": null,
  "poll_interval_seconds": 30.0,
  "termination": {
    "duration_seconds": null,
    "end_at": "2026-08-11T18:00:00+09:00"
  }
}
```

```json
{
  "type": "parser_failure_retry",
  "failure_input": {
    "failure_path": ".\\outputs\\job-a\\run-1\\failures.jsonl",
    "job_id": "job-a",
    "stage_allowlist": ["post"]
  },
  "skip_existing_success": true
}
```

#### 2.4.3. 게시글 JSONL 한 줄 예시
```json
{
  "schema_version": "fmkorea.post.v1",
  "scraped_at": "2026-08-11T12:00:30+09:00",
  "site_id": "fmkorea",
  "board_id": "football_world",
  "board_name": "해외축구",
  "document_srl": 10196696065,
  "document_id_raw": "10196696065",
  "canonical_url": "https://www.fmkorea.com/10196696065",
  "category": {
    "category_id": "233499071",
    "name": "레알 마드리드",
    "url": "https://www.fmkorea.com/football_world?category=233499071"
  },
  "title": "[ESPN] 어제자 레알 선수단 훈련 영상",
  "author": {
    "nickname": "유느님좋아",
    "member_srl": "534126900",
    "level": "32",
    "profile_url": "https://www.fmkorea.com/index.php?mid=football_world&act=dispMemberInfo&member_srl=534126900"
  },
  "created_at": "2026-08-11T08:03:00+09:00",
  "created_at_raw": "2026.08.11 08:03",
  "views": 361,
  "reactions": {
    "up_count": 5,
    "down_count": 0,
    "score": 5
  },
  "comment_count": 0,
  "content": {
    "text": "이 게임 쵸비한테 바치겠습니다\n쵸비가 거절하면 제카한테 바치겠습니다 ㅇㅈㄹ ㅋㅋㅋㅋ",
    "blocks": [
      {
        "order": 0,
        "type": "fmk_video",
        "video_url": "https://mediak5jvqbd.fmkorea.com/files/attach/new5/20260811/10196696065_33854530_e095651ae51e9d1f50fa497abbf0a147.mp4",
        "poster_url": "https://image.fmkorea.com/files/attach/new5/20260811/10196696065_33854530_e095651ae51e9d1f50fa497abbf0a147.mp4.thumb.webp",
        "file_srl": "10196696177",
        "download_url": "https://www.fmkorea.com/?module=file&act=procFileDownload&file_srl=10196696177&sid=...",
        "name": "SnapInsta.to_example.mp4",
        "upload_date": "2026-08-11T08:03:22+09:00",
        "description": "[ESPN] 어제자 레알 선수단 훈련 영상",
        "width": 720,
        "height": 1280,
        "download": null,
        "file_path": null,
        "object_key": null
      },
      {
        "order": 1,
        "type": "youtube",
        "video_id": "IKyeLpRZaZE",
        "url": "https://youtu.be/IKyeLpRZaZE?t=456",
        "embed_url": "https://www.youtube.com/embed/IKyeLpRZaZE",
        "title": null,
        "start_seconds": 456
      },
      {
        "order": 2,
        "type": "x",
        "username": "elonmusk",
        "status_id": "1973529989002989733",
        "tweet_id": "1973529989002989733",
        "url": "https://x.com/elonmusk/status/1973529989002989733",
        "embed_url": "https://platform.twitter.com/embed/Tweet.html?id=1973529989002989733",
        "text": null
      },
      {
        "order": 3,
        "type": "image",
        "url": "https://image.fmkorea.com/files/attach/new5/20260810/example.jpg",
        "file_srl": "10195587886",
        "alt": "example.jpg",
        "width": 1280,
        "height": 960,
        "download_url": "https://www.fmkorea.com/?module=file&act=procFileDownload&file_srl=10195587886&sid=...",
        "download": null,
        "file_path": null,
        "object_key": null
      },
      {
        "order": 4,
        "type": "text",
        "text": "일반 텍스트 본문"
      },
      {
        "order": 5,
        "type": "link",
        "text": "https://m.fmkorea.com/8978626151",
        "url": "https://m.fmkorea.com/8978626151"
      }
    ]
  },
  "attachments": [
    {
      "file_name": "example.jpg",
      "url": "https://www.fmkorea.com/?module=file&act=procFileDownload&file_srl=10195587886&sid=...",
      "file_srl": "10195587886",
      "sid": "...",
      "mid": "football_world",
      "size_text": "186.5KB",
      "download_text": "다운로드 1"
    }
  ],
  "media": [
    {
      "media_id": "10196696177",
      "media_type": "fmk_video",
      "source_block_order": 0,
      "url": "https://mediak5jvqbd.fmkorea.com/files/attach/new5/20260811/example.mp4",
      "file_path": null,
      "object_key": null,
      "description": "[ESPN] 어제자 레알 선수단 훈련 영상",
      "provider": null,
      "embed_url": null,
      "file_srl": "10196696177"
    },
    {
      "media_id": "10195587886",
      "media_type": "image",
      "source_block_order": 3,
      "url": "https://image.fmkorea.com/files/attach/new5/20260810/example.jpg",
      "file_path": "media/fmkorea/football_world/10196696065/3_image.jpg",
      "object_key": null,
      "description": null,
      "provider": null,
      "embed_url": null,
      "file_srl": "10195587886"
    }
  ],
  "comments": [
    {
      "comment_srl": 10197230200,
      "comment_id_raw": "10197230200",
      "source_dom_id": "comment_10197230200_",
      "parent_comment_srl": null,
      "parent_comment_id_raw": null,
      "depth": 0,
      "source_order": 0,
      "is_best": true,
      "badges": ["BEST"],
      "author": {
        "nickname": "V5_Faker",
        "member_srl": "1324753328",
        "level": "27"
      },
      "created_at_raw": "4 시간 전",
      "created_at": null,
      "content": "댓글 내용",
      "reactions": {
        "up_count": 375,
        "down_count": 0,
        "score": 375
      },
      "permalink": "https://www.fmkorea.com/10196696065/10197230200#comment_10197230200"
    }
  ]
}
```

### 2.5. 작업 상태 및 체크포인트 저장
- 체크포인트는 **Scrapling crawler state**와 **애플리케이션 Job checkpoint**를 분리한다.
- Scrapling crawler state
  - 로컬 persistent 실행에서는 Spider 생성 시 `crawldir=<base_output_path>/.crawl/<job_id>`를 사용할 수 있다. 컨테이너의 **일반(non-realtime) exact-resume Job**에서는 `FMKOREA_CRAWDIR`이 지정된 배포에서만 `<FMKOREA_CRAWDIR>/crawl/<job_id>`를 Scrapling `crawldir`로 사용한다. realtime Job은 persistent staging을 사용할 수 있어도 scheduler-state exact resume을 사용하지 않는다.
  - Scrapling은 pending `Request`와 seen request fingerprint를 `crawldir`에 저장하며, 기본 300초 간격 또는 `crawl_config.checkpoint_interval_seconds` 간격으로 주기 저장한다.
  - graceful stop 시 최종 crawler state를 저장하고, 같은 `crawldir`로 Spider를 다시 시작하면 **Scrapling scheduler state**를 복원하며 `start_requests()`를 다시 실행하지 않는다.
  - 정상 완료 시 Scrapling crawler checkpoint는 자동 정리되는 것을 기본 동작으로 사용한다.
  - Scrapling crawler checkpoint는 pickle 기반의 내부 상태이므로 프로그램이 직접 생성한 신뢰 가능한 `crawldir`만 복원 대상으로 사용하고 사용자 임의 파일을 로드하지 않는다.
  - Browser fallback의 pending `Request`도 crawler checkpoint에 포함될 수 있다. request-local `page_action`은 pickle 가능한 모듈 최상위 callable로 구성하고, Spider instance를 캡처하는 지역 함수/클로저를 사용하지 않는다. 복원된 callable은 새 Spider process의 runtime-only identity bridge를 사용하며, cookie/UA 자체는 checkpoint에 저장하지 않는다.
  - `crawldir` resume이 복원하는 대상은 Scrapling scheduler의 pending request/seen set이며, 애플리케이션의 `ProxyRotator` index, Proxy별 `BrowserIdentityState`, HTTP/Browser cookie jar 또는 실제 Browser context 같은 runtime-only 상태까지 복원하는 것으로 표현하지 않는다.
- 애플리케이션 Job checkpoint
  - 최상위 타입은 `Checkpoint`다.
  - `schema_version="fmkorea.checkpoint.v1"`를 저장한다.
  - `job_id`: 체크포인트 대상 Job ID
  - `status`: `running`, `stopped`, `completed`
  - `reason`: `stopped`일 때만 저장하며 `blocked`, `manual_stop`, `error`, `timeout`, `lease_expired` 중 하나다.
  - `lease_expired`는 **future-reserved enum**이다. 현재 FMKorea scraper core/Spider는 lease를 생성·갱신·만료 판정하지 않으며, 현행 one-shot worker 실행 경로에서는 이 값을 직접 생성하지 않는다.
  - 향후 외부 RabbitMQ/SQS orchestrator가 Job lease를 소유하는 통합 구조를 구현할 때만 orchestrator adapter가 graceful stop을 요청하고 `reason="lease_expired"`를 기록할 수 있다. 따라서 현행 구현에서 `lease_expired`를 발생시키기 위한 lease client, heartbeat, renew loop를 scraper 내부에 추가하지 않는다.
  - `job_config`: 재구성에 필요한 원본 `JobConfig`
  - `completed_keys`: 저장 완료된 게시글 키 목록
  - `active_run_id`: 현재 crawl/staging이 속한 run ID. 활성 run이 없으면 `null`
  - `staging_ref`: 현재 run의 durable staging JSONL 또는 S3 staging/segment manifest 참조. 없으면 `null`
  - `enrichment_status`: `not_requested`, `pending`, `running`, `completed`, `completed_with_warnings`, `failed` 중 하나
  - `realtime_deadline_at`: realtime Job의 절대 종료 시각. realtime이 아니면 `null`. `duration_seconds` 입력도 최초 Job 시작 시 이 값으로 정규화해 저장한다.
  - `crawl_state_ref`: exact-resume state가 있을 때 persistent state root 기준의 crawler-state 상대 참조(예: `crawl/<job_id>` 또는 로컬의 `.crawl/<job_id>`). 없으면 `null`. 컨테이너 mount point의 absolute path 자체를 checkpoint portability 계약으로 저장하지 않는다.
  - `updated_at`: 체크포인트 갱신 시각
  - scheduler의 `queued_links`나 별도 `collector_cursor`는 중복 상태가 되므로 애플리케이션 checkpoint에 저장하지 않는다. pending request와 seen set의 source of truth는 Scrapling crawler state다.
- 저장 위치
  - 로컬 애플리케이션 체크포인트는 `<base_output_path>/Checkpoint.json` 하나에 저장한다.
  - 로컬 체크포인트 파일의 wrapper schema는 `fmkorea.local_checkpoint.v1`이고, 내부 `checkpoints` 객체가 `job_id`별 `Checkpoint`를 가진다.
  - PostgreSQL 모드에서는 `PostgresJobCheckpointStore`가 runtime에서 전달받은 PostgreSQL DSN으로 Job checkpoint를 저장한다. 컨테이너에서의 정확한 환경변수 이름은 §8.2.11을 따른다.
  - 로컬 Scrapling crawler state는 `<base_output_path>/.crawl/<job_id>`에 둔다. 로컬 output/staging도 같은 persistent filesystem 아래에 있으므로 해당 경로가 실제로 유지되는 환경에서는 scheduler-state exact resume을 사용할 수 있다.
  - 컨테이너에서 `FMKOREA_CRAWDIR=<state_root>`를 지정하면 이 값은 이름과 달리 단순 crawler directory가 아니라 **persistent state root**로 해석한다. 일반(non-realtime) Job의 실제 Scrapling crawler state는 `<state_root>/crawl/<job_id>`, 모든 해당 배포 run의 S3 미업로드 staging은 `<state_root>/staging/<job_id>/<run_id>/...` 아래에 둔다. realtime Job은 후자의 staging persistence만 resume에 활용한다.
  - exact-resume 배포에서는 `.crawl`만 persistent이고 staging은 ephemeral인 구성을 허용하지 않는다. crawler seen/pending state와 아직 S3에 commit되지 않은 staging이 함께 살아남아야 scheduler state와 output durability가 어긋나지 않는다.
  - `FMKOREA_CRAWDIR`을 지정하지 않은 일반 ephemeral container에서는 persistent crawler state를 사용하지 않고 PostgreSQL `JobConfig` + `completed_keys` + S3 committed segments를 기준으로 새 Spider를 재구성하는 reconstruction resume을 사용한다. pending queue/seen set을 byte-for-byte 복원하는 것으로 표현하지 않는다.
  - 컨테이너가 exact resume을 사용하려면 **최초 Job 실행부터** `FMKOREA_CRAWDIR`을 persistent volume mount 경로로 지정해야 한다. crash 이후 resume 시점에 처음 지정해도 이전 process의 crawler state/staging은 복원할 수 없다.
  - `FMKOREA_CRAWDIR`이 지정되었지만 root를 생성/쓰기할 수 없으면 exact resume이 가능한 것처럼 조용히 fallback하지 않고 runtime configuration error로 시작을 실패시킨다. 해당 경로가 실제 persistent volume인지 판별하는 책임은 배포 환경에 있으며 애플리케이션이 filesystem 종류를 추측하지 않는다.
- 저장 시점
  - `completed_keys`는 **해당 게시글 record가 선택한 output backend에서 durability를 확보한 시점**에만 갱신한다. 파싱 성공이나 ephemeral local spool append만으로 완료 처리하지 않는다.
  - 로컬 persistent output에서는 staging record write 후 flush가 성공한 시점을 durable commit으로 본다. 구현 환경에서 crash-consistency가 필요하면 file flush/fsync 이후에만 `completed_keys`로 승격한다.
  - S3 호환 output에서는 open local spool에 record를 append한 시점은 아직 `pending`이다. 해당 record를 포함한 immutable post segment의 upload가 성공하고 object key가 확정된 뒤에만 그 segment의 pending key들을 `completed_keys`로 승격한다.
  - S3 segment upload가 실패하면 해당 segment의 key는 `completed_keys`에 넣지 않는다. graceful stop/finalize 시에는 open segment를 먼저 flush/upload하고 ACK된 key만 checkpoint에 반영한다.
  - Scrapling crawler state의 주기 저장은 Spider의 `crawldir` + `interval` 기능에 맡긴다.
  - 수동 중단 또는 Browser 전환·재시도 이후에도 최종 Blocking이 지속되어 Job을 중단할 때는 `spider.pause()` 또는 graceful shutdown 경로로 crawler state를 먼저 보존한 뒤 `status="stopped"`와 `reason`을 저장한다.
  - Spider scheduler가 비고 in-flight request가 모두 종료되면 **crawl phase가 끝난 것**으로 판단한다. `media_mode="url_only"`이면 최종 JSONL finalize 후, `media_mode="download"`이면 post-crawl enrichment와 최종 JSONL 생성 후에만 `status="completed"`를 저장한다.
  - `MediaEnrichmentPipeline` 자체가 시작/입력 복구/최종 output 생성 단계에서 치명적으로 실패하면 `enrichment_status="failed"`, `status="stopped"`, `reason="error"`로 저장하고 `staging_ref`를 유지한다. 개별 미디어 일부의 다운로드 실패만 있는 경우에는 게시글 결과를 유지하고 `status="completed"`, `enrichment_status="completed_with_warnings"`로 종료한다.
  - resume 시 `enrichment_status`가 `pending`, `running`, `failed`이고 복구 가능한 `staging_ref`가 있으면 새 crawl run보다 이전 run의 enrichment/finalization을 먼저 시도한다.
  - graceful `stopped` run도 이미 durable staging에 저장된 게시글은 finalization/enrichment 대상으로 처리할 수 있으며, report는 partial run임을 `status="stopped"`로 유지한다.
- 재개 처리
  - CLI는 `fmkorea-scraper resume --job-id <job_id>`로 재개한다.
  - resume 시작 시 checkpoint의 `active_run_id`, `staging_ref`, `enrichment_status`를 먼저 검사한다. 이전 process가 crawl/enrichment 중 비정상 종료되어 final JSONL이 없는 경우 **이전 run의 staging을 먼저 복구/finalize**한 뒤 새 crawl run을 시작한다.
  - 로컬 persistent output에서는 durable staging file을 스캔해 checkpoint의 `completed_keys`와 union할 수 있다. S3 호환 output에서는 **upload ACK가 확인된 immutable post segment만** durable source로 인정하고, ephemeral local spool에만 있던 record는 completed로 복원하지 않는다.
  - 로컬 staging 파일이 없고 S3 immutable post segments가 남아 있으면 `staging_ref`/manifest를 이용해 staging stream을 재구성하고 각 committed segment의 post key를 `completed_keys`에 union한다.
  - checkpoint가 S3 segment upload 직후 아직 갱신되지 못하고 crash한 경우에도 resume가 committed segment를 스캔해 완료 key를 복원할 수 있어야 한다.
  - 로컬에서는 애플리케이션 checkpoint와 동일 persistent filesystem의 `crawldir`/staging이 남아 있으면 Scrapling scheduler state를 그대로 복원한다. 이를 **scheduler-state exact resume**이라고 부른다.
  - exact-resume state가 없으면 `JobConfig`로 새 Spider를 만들고 `completed_keys` 및 durable output state를 이용해 이미 저장된 게시글을 다시 저장하지 않는 reconstruction resume을 수행한다.
  - 컨테이너 환경은 `RESUME=true`, `JOB_ID=<checkpoint job_id>`로 PostgreSQL Job checkpoint를 읽는다. **일반(non-realtime) Job**은 `FMKOREA_CRAWDIR`이 지정되어 있고 동일 persistent state root의 crawler state/staging이 존재하면 scheduler-state exact resume을 수행하며, 환경변수가 없으면 reconstruction resume을 수행한다. realtime Job은 예외로 항상 reconstruction resume을 사용한다.
  - resume 시 새 process의 `ProxyRotator`와 Proxy별 `BrowserIdentityStore`는 새로 초기화한다. 기존 exact-resume pending request의 `proxy_id`/session `sid`가 유효하고 배포의 Proxy mapping이 동일하면 그 request의 logical attempt 선택 Proxy를 유지하며, Browser cookie/UA/context는 새 process에서 재획득한다.
  - exact-resume 배포의 Proxy pool은 저장된 pending request의 `proxy_id`가 다른 endpoint로 재해석되지 않도록 **기존 순서와 항목을 유지**해야 한다. 복원된 `proxy_id`가 존재하지 않거나 pool mapping이 바뀐 것으로 판명되면 다른 Proxy로 조용히 재매핑하지 않고 configuration error로 중단한다. reconstruction resume에서는 새로운 pool 순서로 선택한다.
  - realtime Job은 최초 시작 시 `RealtimeTermination`을 절대 종료 시각 `realtime_deadline_at`으로 정규화해 애플리케이션 checkpoint에 저장한다. `end_at`은 그대로 사용하고 `duration_seconds`는 **Job 최초 시작 시각 + duration**으로 한 번만 계산한다.
  - realtime resume에서는 `FMKOREA_CRAWDIR`이 지정되어 있어도 Scrapling scheduler state를 continuation source로 복원하지 않고 reconstruction resume을 수행한다. 기존 `realtime_deadline_at`을 재사용하며 새 run 시작 시 `duration_seconds`를 다시 더하지 않는다. 이미 deadline이 지난 상태로 resume되면 새 poll request를 만들지 않고 남은 staging/finalization/enrichment만 처리한다.
  - 이전 active run의 staging/finalization 복구가 끝난 뒤에는 기존 최종 JSONL에 이어 쓰지 않고 새 run을 만들며, 중복 기준은 `(site_id, board_id, document_id_raw)`를 우선 사용한다.
- 체크포인트에 저장되는 `JobConfig`
  - `JobConfig`는 수집 대상, 수집 옵션, `crawl_config`, 저장소 옵션을 저장한다.
  - Proxy credential, timeout, 실행 경로 같은 환경 종속 runtime 값은 `JobConfig`에 저장하지 않는다.
  - Browser session 사용 여부는 선택 옵션이 아니라 Spider의 고정 실행 정책이므로 `JobConfig`에 on/off 필드를 두지 않는다.
  - runtime 값은 CLI, 터미널 마법사, 컨테이너, MCP, 라이브러리 adapter가 실행 시점에 `SpiderRuntimeConfig`로 조립한다.

### 2.6. 차단 대응, Proxy, User-Agent, 속도 제어
- Blocking 포착 및 전환
  - 이 문서에서 `HTTP 단계 실패`는 Spider의 `is_blocked(response)`가 `true`를 반환한 HTTP 응답을 의미한다. DNS 오류, connection reset, 순수 transport timeout 같은 예외는 별도의 transport error로 분류한다.
  - `FMKoreaSpider.is_blocked()`의 status-code 기준은 Scrapling 기본 `BLOCKED_CODES`를 그대로 따른다: `401`, `403`, `407`, `429`, `444`, `500`, `502`, `503`, `504`.
  - 위 status code는 모두 Scrapling blocked-retry 경로와 Stealthy Browser fallback 대상이다. `500/502/503/504`도 server/transient failure로 별도 분리하지 않고 Scrapling 기본 blocked 처리에 포함한다.
  - `404`와 `410`은 삭제/비공개/존재하지 않음으로 의미가 명확하므로 Blocking으로 취급하지 않는다. 여기서 `종료`는 **해당 request/게시글 처리만 종료(skip)**한다는 뜻이며 Job/Spider 전체는 계속 실행한다.
  - 그 밖의 status code는 Scrapling 기본 status 판정만으로 Blocking 처리하지 않는다. 다만 status가 정상이어도 FMKorea 전용 차단 문구나 차단 페이지 패턴이 감지되면 `is_blocked()` override에서 추가로 `true`를 반환할 수 있다.
  - HTTP `200` 응답이라도 앞부분 4096 byte에 `<title>에펨코리아 보안 시스템</title>`에 해당하는 보안 화면 표식이 있으면 목록·검색·상세 파서보다 먼저 Blocking으로 분류한다. 차단 화면의 필수 DOM 누락을 parser-stage 실패로 기록하지 않는다.
  - Scrapling engine은 blocked request의 retry copy를 만든 뒤 `FMKoreaSpider.retry_blocked_request()`에 전달한다. 이 hook은 요청 phase에 따라 **HTTP Blocking → 같은 Proxy의 Browser fallback**, **Browser Blocking → 다음 Proxy를 선택한 새 HTTP logical attempt**를 구성한다.
  - 이 전환은 Scrapling의 built-in blocked retry 경로를 사용하며, retry request는 `dont_filter=True`로 재큐잉되는 Scrapling 기본 동작을 따른다.
  - `max_blocked_retries`는 HTTP↔Browser 사이의 전환을 모두 합산한 **해당 logical request의 blocked retry Request 개수**다. 기본값 `3`이면 `HTTP(P1) → Browser(P1) → HTTP(P2) → Browser(P2)`까지 시도한다. HTTP→Browser fallback도 하나의 retry budget을 소비한다.
  - 각 `HTTP(Pn) + Browser(Pn)` 쌍을 하나의 logical attempt로 본다. transport retry(응답 생성 전 DNS/reset/timeout)는 같은 phase/Proxy에서 처리하며 Proxy를 rotate하지 않는다. Browser까지 Blocking일 때만 다음 blocked retry에서 새 logical attempt의 Proxy를 선택한다.
  - **모든 HTTP logical attempt에 Browser fallback 기회를 보장하기 위해 `max_blocked_retries`는 1 이상의 홀수 정수만 허용**한다. 마지막 Browser까지 Blocking일 때만 terminal blocked로 분류한다.
  - Scrapling engine 자체는 blocked retry 한도 소진 시 해당 request를 더 이상 재큐잉하지 않을 뿐 Job 전체를 자동 pause하지 않는다. 따라서 애플리케이션은 public `Request.meta`에 `blocked_attempt`을 유지해 terminal blocked 상태를 명시적으로 감지한다.
  - `blocked_attempt`의 의미는 **현재 request가 몇 번째 blocked retry request인지**다. 최초 원본 request는 `blocked_attempt=0`, 첫 번째 blocked retry는 `1`, 두 번째는 `2`로 정의한다.
  - `retry_blocked_request()`는 engine이 전달한 retry request에 대해 inherited `blocked_attempt`을 정확히 한 번 증가시킨다. Scrapling private `_retry_count`를 애플리케이션 계약으로 직접 읽지 않되, 테스트에서는 `blocked_attempt`과 실제 retry 횟수가 어긋나지 않는지 검증한다.
  - 이후 `is_blocked(response)`가 `true`이고 `response.request.meta["blocked_attempt"] >= max_blocked_retries`이며 현재 phase가 Browser이면 `terminal_blocked=true`를 Spider 상태에 기록하고 `spider.pause()`를 요청한다. 예를 들어 `max_blocked_retries=3`이면 두 logical attempts를 각각 HTTP→Browser까지 시도한다.
  - `spider.pause()`는 Scrapling의 graceful pause 경로를 사용하므로 in-flight request가 끝난 뒤 `crawldir` checkpoint를 저장하고 crawl을 닫는다. application runner는 Spider 종료 후 `terminal_blocked` 상태를 확인해 `status="stopped"`, `reason="blocked"` 애플리케이션 Checkpoint를 저장한다.
  - HTTP 단계의 Blocking만으로는 중단 Checkpoint를 저장하지 않는다. 같은 Proxy의 Browser fallback과 잔여 budget 내 다음 Proxy logical attempt까지 Blocking인 경우에만 중단 Checkpoint를 저장한다.
  - 체크포인트 저장 후 터미널과 로그에 저장된 `job_id`를 출력한다.
  - HTTP 응답 본문 판정은 앞부분 4096 byte 샘플을 기준으로 수행한다.
  - Scrapling이 제공하는 `is_blocked()` 기본 status 판정과 blocked-retry engine을 그대로 기준으로 사용하고, FMKorea 전용 body/content 차단 신호만 override에서 추가한다. 기본 blocked status set을 축소하거나 별도의 server/transient status 분류로 우회하지 않는다.
  - 상세 게시글 response의 최종 분류는 다음 우선순위를 사용한다.
    1. `404` 또는 `410`이면 즉시 `post_unavailable`로 분류하고 해당 게시글만 skip한다.
    2. HTTP session의 `401/403/407/429/444/500/502/503/504`는 body marker와 관계없이 우선 Scrapling Blocking으로 처리하여 Browser fallback 기회를 부여한다.
    3. Browser session의 상세 게시글 응답에서 **strong semantic-unavailable marker**가 확인되면 status가 `403` 등 blocked code여도 `is_blocked()` override는 `false`를 반환하고 callback이 `post_unavailable`로 분류해 해당 게시글만 skip한다.
    4. Browser session에서도 strong semantic-unavailable marker가 없고 Scrapling blocked status/body 신호가 유지되면 Blocking으로 처리하며, retry budget 소진 시 `terminal_blocked`로 Job 전체를 pause한다.
    5. 위 어느 조건에도 해당하지 않으면 정상 parser 경로로 전달한다.
  - strong semantic-unavailable marker는 임의의 게시글 본문 텍스트가 아니라 사이트의 오류/알림 UI 문맥에서 확인된 `deleted`, `unavailable`, `삭제된 게시글`, `삭제된 게시물`, `존재하지 않는 게시글`, `존재하지 않는 게시물`, `권한이 없습니다`, `열람할 수 없습니다` 등을 의미한다. 일반 본문에 같은 문자열이 포함된 것만으로 unavailable로 분류하지 않는다.
  - 구현 시 `is_semantic_unavailable(response)` 공통 helper를 두고 `is_blocked()`와 상세 callback이 동일한 판정 함수를 사용한다. Browser 상세 응답에서 이 helper가 `true`이면 `is_blocked()`는 `false`를 반환하고 callback은 `post_unavailable` 처리 후 item을 yield하지 않는다.
  - `post_unavailable`은 parser-stage failure나 Job-level blocked failure가 아니며 `completed_keys`에 성공 게시글처럼 넣지 않는다. 대신 현재 run의 `UnavailableKeyTracker`에 넣어 같은 realtime run에서 반복 상세 요청을 막고, report에는 `unavailable_count` 또는 diagnostic을 기록한다.
  - storage/control-plane failure는 parser-stage failure와 분리한다. `OutputStorePort.append_post()`의 local spool write 실패, persistent staging write/flush 실패, 최종 `flush_pending()` 실패, `finalize()` 실패, 필수 Job checkpoint 저장 실패처럼 **현재 Job의 durability 또는 재개 가능성을 보장할 수 없게 만드는 오류**는 게시글 하나의 parser failure로 기록하고 계속 진행하지 않는다.
  - crawl 중 위 systemic storage/control-plane failure가 발생하면 Spider 상태에 terminal error를 기록하고 `spider.pause()`를 요청해 새 request 예약을 멈춘다. runner는 가능한 범위에서 in-flight 정리와 마지막 안전한 상태 보존을 시도한 뒤 `status="stopped"`, `reason="error"`로 종료한다. checkpoint backend 자체가 실패 원인이라 해당 상태 저장도 실패하면 이를 재귀적으로 재시도하지 않고 stderr/구조화 로그에 best-effort terminal error를 남긴다.
  - 일시적인 S3 immutable-segment upload 실패는 local spool/closed batch가 안전하게 남아 있는 동안 즉시 Job terminal error로 보지 않고 pending 상태로 유지해 다음 periodic/final flush에서 재시도할 수 있다. 그러나 정상/중단 종료 시 최종 flush에서도 durability를 확보하지 못하면 `status="stopped"`, `reason="error"`로 처리하고 `completed_keys`에는 ACK되지 않은 key를 넣지 않는다.
- Scrapling ProxyRotator 기반 HTTP → Browser logical-attempt 전환
  - 목록, 검색, 상세 HTML은 Scrapling `Selector`로 파싱한다.
  - 새 logical crawl request와 Browser-blocked 이후의 새 HTTP retry attempt는 `ProxyAttemptCoordinator`에서 **Scrapling `ProxyRotator.get_proxy()`를 정확히 한 번 호출**해 다음 Proxy를 선택한다. session에 `proxy_rotator=`를 직접 주입하여 내부 transport retry 도중 Proxy가 바뀌게 하지 않는다.
  - `configure_sessions()`에서 configured Proxy마다 `manager.add("http:<proxy_id>", FetcherSession(proxy=<that_proxy>, follow_redirects="safe", ...), lazy=True)`와 `manager.add("browser:<proxy_id>", AsyncStealthySession(proxy=<browser_transport_proxy>, headless=True, google_search=False, hide_canvas=True, block_webrtc=True, solve_cloudflare=True, allow_webgl=True, max_pages=<crawl_config.browser_max_pages>, timeout=<browser_timeout_ms>, ...), lazy=True)`를 등록한다. Proxy를 사용하지 않을 때는 direct HTTP/Browser session만 등록한다. 네 가지 stealth 전용 옵션을 모두 명시하며, `solve_cloudflare`는 Cloudflare 챌린지용이므로 FMKorea 자체 보안 화면의 해제를 보장하지 않는다.
  - 인증된 SOCKS5 Proxy는 HTTP Fetcher에 원래 `socks5://` URL을 직접 전달한다. Chromium Browser session은 SOCKS5 username/password 인증을 직접 처리하지 못하므로 Spider 실행 중 해당 Proxy마다 `127.0.0.1`의 임시 HTTP CONNECT 중계기를 열어 `browser_transport_proxy`로 전달한다. 중계기는 HTTP CONNECT를 동일한 SOCKS5 upstream으로 연결하고 Spider 종료 시 닫는다. 그 외 Proxy는 원래 URL을 Browser session에 전달한다.
  - 중계기는 `port=0`으로 OS에 임시 포트 선택을 요청한다. Windows에서 포트 예약 또는 충돌로 바인딩이 `10013`/`10048`을 반환하면 새 포트로 최대 5회 시도하고, 계속 실패하면 설정/실행 오류로 중단한다. 열린 중계기는 이후 Proxy 설정 중 오류가 나더라도 닫는다.
  - 중계기의 임시 포트는 Proxy pool 순서, exact-resume fingerprint, `proxy_id`, 영속 checkpoint에 포함하지 않는다. `proxy_log.endpoint`에는 로컬 중계 주소가 아닌 인증정보를 제거한 원래 Proxy endpoint를 기록한다. `Response.meta["proxy"]`의 실제 Browser 전송 주소를 검사할 때만 해당 Proxy의 로컬 중계 주소와 비교한다.
  - **HTTP 세션도 Proxy별로 분리**해 HTTP cookie jar가 다른 Proxy로 누출되지 않도록 한다. **Browser 세션도 Proxy별로 고정**해 persistent Browser context/cookie/UA를 같은 egress Proxy에 귀속시킨다. session-level 고정 Proxy를 사용하므로 Browser 요청마다 per-request `proxy=`로 임시 Browser context를 생성하는 경로에 의존하지 않는다.
  - 따라서 본 명세에서 `Browser mode`, `Stealthy Browser`, `Browser fallback`은 별도 언급이 없는 한 Scrapling `StealthyFetcher`/`AsyncStealthySession` 계열의 stealth browser 경로를 의미한다. 일반 `DynamicFetcher`/`AsyncDynamicSession`은 Blocking fallback 기본 경로로 사용하지 않는다.
  - `lazy=True`는 `AsyncStealthySession` 생성자 인자가 아니라 `SessionManager.add()`의 인자다.
  - 최초 HTTP request는 `proxy_attempt=1`, `blocked_attempt=0`으로 만들며 rotator가 선택한 `proxy_id`의 `http:<proxy_id>` session으로 라우팅한다. HTTP Blocking이면 URL/callback/priority/허용된 application context, `proxy_id`, `proxy_attempt`을 유지하고 `browser:<proxy_id>`로 전환한다. **Browser fallback에서는 rotate하지 않는다.**
  - Browser fallback까지 Blocking이면, 남은 blocked retry budget에서 `proxy_attempt += 1` 후 rotator가 다음 Proxy를 선택해 해당 `http:<proxy_id>` session으로 요청을 재예약한다. Proxy pool 크기가 1이면 같은 Proxy를 다시 선택한다.
  - Browser success 이후에는 해당 request의 callback을 진행하고, **새로운 후속 logical request는 다시 rotator가 선택한 Proxy의 HTTP session으로 시작**한다. 전역 `browser_sticky`와 `http_probe` 상태 및 관련 public 옵션은 제거한다.
  - HTTP/Browser의 session-level transport retry는 같은 session/Proxy를 유지한다. Proxy rotation은 오직 새로운 logical attempt가 시작될 때만 발생한다.
  - `Request.meta["proxy_log"]`에는 **현재 logical request 전체의 sanitized Proxy 이력**을 배열로 유지한다. 각 phase event에는 `proxy_id`, `endpoint`(credential 없는 scheme/host/port), `proxy_attempt`, `phase`(`http` 또는 `browser_fallback`), `blocked_attempt`, `rotated`를 넣고 응답 후 `status_code`, `blocked`, `result`를 보강한다. 최초 Proxy 선택은 `rotated=false`, 직전 attempt와 실제 Proxy가 달라졌을 때만 `rotated=true`이며 같은 Proxy Browser fallback은 `false`다.
  - Scrapling `Response.meta["proxy"]` 값이 제공되면 선택한 `proxy_id`와 실제 Proxy의 일치 여부를 확인한다. 값이 없는 경우 session-level Proxy mapping을 근거로 사용하되 네트워크 egress IP까지 검증된 것으로 표시하지 않는다. `Response.meta["proxy_log"]`에는 **해당 응답까지의 이력**이 노출되도록 request meta에서 history를 copy-on-write로 이어가며, raw credential-bearing Proxy URL은 별도 기록하지 않는다. Response가 발생하지 않은 transport error는 `on_error()`의 sanitized diagnostic event로 남긴다.
  - `sanitize_proxy_ref()`는 string proxy URL과 Playwright-style dict에서 username/password/query 등을 버리고 endpoint만 생성한다. `proxy_log` 외에도 failure JSONL/JobReport/checkpoint/구조화 로그에 credential-bearing `Response.meta["proxy"]` 원문, Proxy 암호, Cookie, 인증 header를 저장하지 않는다.
  - 본 명세의 HTML crawl request는 GET navigation으로 한정한다. HTTP request의 `method`/`params`/`data`/`json` 같은 전용 kwargs를 Browser session에 그대로 전달하지 않는다. 검색 query는 URL builder 단계에서 URL에 포함한다.
  - `retry_blocked_request()`는 HTTP request header 전체를 Browser `extra_headers`로 복사하지 않는다. 기본 Browser-safe 전달 allowlist는 **`Referer`만** 사용하고, 향후 FMKorea가 특정 custom header를 실제로 요구하는 경우에만 명시적으로 allowlist에 추가한다. `User-Agent`, `Sec-CH-UA*`, `Cookie`, `Host`, `Content-Length`, `Accept-Encoding`, `Proxy-Authorization` 등 browser/network identity 또는 전송 계층이 직접 관리해야 하는 header는 HTTP request에서 Browser로 복사하지 않는다.
  - Browser retry request의 identity capture `page_action`은 `crawldir`의 pending request로 직렬화할 수 있어야 한다. callable에는 `job_id`, `proxy_id`, `capture_seq`처럼 재구성 가능한 비밀정보 없는 식별자만 담고, Spider/Browser page를 캡처한 지역 클로저와 raw cookie/UA를 넣지 않는다. 복원된 request는 새 Spider가 등록한 identity bridge를 사용한다.
  - 위 allowlist 적용 후 HTTP 전용 kwargs를 제거하고 Browser 전용 `wait_selector`/timeout 등을 추가한다. Browser의 User-Agent/client hints/cookie는 Browser context 자체가 관리하며 HTTP 쪽 identity 값을 `extra_headers`로 억지로 덮어쓰지 않는다.
  - 향후 non-GET endpoint를 crawl 대상으로 추가한다면 Browser fallback에서 단순 `sid` 전환으로 처리하지 않고 `page_action`/XHR 호출 등 동등한 Browser 동작을 별도 정의해야 한다.
  - page 종류별 Browser 준비 조건이 필요하면 `Request.meta`에 `page_kind`/`required_selector`를 저장하고 Browser retry 시 `wait_selector` 같은 Browser 전용 인자로 변환한다.
  - Proxy별 Browser session은 `lazy=True`로 등록하고 실제 fallback 시에만 시작하여 같은 Proxy의 후속 fallback에 재사용한다. 단일 configured Proxy당 Browser page pool 상한은 `browser_max_pages`다.
  - 현재 라우팅 정책은 logical request별 HTTP→Browser 전환이며 전역 sticky/probe 상태와 관련 설정을 두지 않는다. 향후 sticky 정책이 필요하면 HTTP Blocking 비율, Proxy별 fallback 성공률, Browser 대비 HTTP 처리 비용을 근거로 Proxy/identity affinity와 함께 재설계한다.
  - 로컬 및 Docker 실행 환경은 Browser 전환이 항상 가능하도록 `scrapling[fetchers]`와 Playwright browser를 설치한다.
  - Docker image는 `.[s3,postgres]`와 기본 의존성 `scrapling[fetchers]`를 설치하고 `scrapling install`을 수행하여 Browser mode 실행 준비를 image build 단계에서 끝낸다.
  - 목록/게시판 검색 필수 DOM은 `div.bd`, 통합 검색 필수 DOM은 `.searchResult, .search_result`, 게시글 상세 필수 DOM은 `#bd_capture`를 기준으로 한다.
  - Browser session은 `headless=true`, `max_pages=<crawl_config.browser_max_pages>`를 기본으로 하며, `wait_selector`와 timeout은 요청 유형별로 적용한다.
  - public general-purpose custom/default header 설정은 제공하지 않는다. 내부 request 흐름에서 Browser에 필요한 header는 위 Browser-safe allowlist를 거친 request-local `extra_headers`만 사용한다. Proxy 활성 시 Browser session마다 해당 Proxy를 session-level로 고정하고 logical attempt에서 선택된 session으로 fallback한다.
  - Browser 의존성이 설치되지 않아 전환할 수 없는 경우 `browser_dependency_missing` terminal error로 처리한다.
  - Blocking 응답 재시도 횟수는 별도 애플리케이션 status-code retry loop로 중복 구현하지 않고 Spider의 `max_blocked_retries`를 단일 기준으로 사용한다.
  - HTTP→Browser 전환을 포함한 해당 request의 blocked-retry budget을 모두 소진한 뒤에도 Browser 응답이 최종 Blocking이면 §2.6의 `terminal_blocked → spider.pause() → application checkpoint` 경로를 사용한다. engine의 retry exhaustion 자체가 Job stop을 자동 수행한다고 가정하지 않는다.
- Browser identity → HTTP 동기화
  - Browser fallback 성공 시 identity를 즉시 `BrowserIdentityState`에 덮어쓰지 않는다. 먼저 해당 Proxy의 Browser page에서 **readiness 완료 후** `BrowserIdentityCandidate(proxy_id, ...)`를 캡처하고, Browser `Response`가 `is_blocked()==false`로 확정된 경우에만 해당 `BrowserIdentityStore[proxy_id]`로 승격한다. Blocking 상태에서 얻은 identity는 HTTP에 전파하지 않는다.
  - `BrowserPageActionComposer`는 Browser request별 site/page action과 identity capture를 하나의 `page_action`으로 조합한다. `page_kind`/`required_selector`에 따라 identity에 필요한 DOM/challenge readiness wait가 있으면 먼저 수행하고, **그 wait가 끝난 마지막 단계에서** `page.context.cookies()`와 `navigator.userAgent`를 읽어 candidate snapshot을 만든다. 외부 `wait_selector=`가 identity capture 이후에 실행될 수 있다는 전제에 의존하지 않는다.
  - parser readiness 보장을 위해 request-level `wait_selector`를 별도로 유지할 수 있지만, identity freshness에 필요한 wait는 `BrowserPageActionComposer` 내부에서 capture보다 먼저 수행한다. 동일 selector를 두 번 확인하게 되더라도 두 번째 wait가 이미 충족된 상태라면 즉시 통과할 수 있으므로 correctness를 우선한다.
  - cookie는 `page.context.cookies()`로 얻는 전체 Browser context cookie를 기준으로 한다. 응답의 `Response.cookies`만으로 challenge 과정에서 JavaScript가 설정한 cookie를 모두 표현할 수 있다고 가정하지 않는다.
  - Browser cookie 중 `fmkorea.com` 및 허용된 FMKorea 서브도메인에 해당하는 first-party cookie만 HTTP 동기화 대상으로 삼고 광고/분석 등 제3자 도메인 cookie는 버린다.
  - cookie는 Scrapling `FetcherSession`의 public `cookies=` 인자가 받는 형식으로 정규화하며 가능한 범위에서 `name`, `value`, `domain`, `path` scope를 보존한다.
  - User-Agent는 동일 Browser page에서 `navigator.userAgent`를 읽은 실제 값을 기준으로 하고, 동시에 Chrome/Chromium major version을 추출해 저장한다.
  - `BrowserIdentityCandidate`는 최소 `proxy_id`, `capture_seq`, `cookies`, `browser_user_agent`, `browser_chrome_major`, `captured_at`을 가진 runtime-only snapshot이다. candidate 생성 시 `BrowserIdentityBridge`의 단일 lock 아래 monotonic `identity_capture_seq`를 증가시켜 `capture_seq`를 발급한다.
  - Browser page pool에서 여러 request가 동시에 진행되어 response 완료 순서가 뒤집힐 수 있으므로, non-blocked response라도 **같은 proxy_id의** `candidate.capture_seq > active_identity.capture_seq`인 경우에만 해당 slot을 교체한다. 다른 Proxy slot과 sequence를 비교하지 않는다.
  - candidate capture와 active promotion은 분리된 단계다. `page_action`은 candidate만 만들고, response-level blocked 판정이 끝난 뒤 `BrowserIdentityBridge.promote_if_fresh(candidate)`가 해당 Proxy slot의 sequence 비교와 active state 교체를 원자적으로 수행한다.
  - 캡처한 cookie/User-Agent/major version은 파일, PostgreSQL Checkpoint, JSONL, report, failure record, 일반 로그에 저장하지 않고 현재 Spider process memory에서만 유지한다.
  - 새 HTTP logical attempt에서 `proxy_id`를 선택한 후에는 **오직 동일 `proxy_id`**의 active `BrowserIdentityState`에서 first-party cookie와 conditional UA/impersonation을 가져온다. 다른 Proxy의 cookie/UA를 절대로 교차 적용하지 않는다. 처음 선택된 Proxy에 Browser identity가 없다면 해당 HTTP session 자체의 Proxy별 jar와 coherent Chrome impersonation만 사용한다.
  - raw Browser User-Agent는 **Browser Chrome major와 정확히 일치하는 Scrapling/curl_cffi impersonation profile이 지원될 때만** HTTP `headers["User-Agent"]`에 적용한다. 예를 들어 Browser가 Chrome 142이고 `chrome142` profile이 지원되면 `impersonate="chrome142"`와 Browser 실제 UA를 함께 사용한다.
  - exact major impersonation profile이 없으면 Browser User-Agent를 HTTP header에 강제로 복사하지 않는다. 이 경우 HTTP는 `impersonate="chrome"` 또는 구현 시점에 선택한 지원 Chrome-family profile과 `stealthy_headers=True`가 생성하는 coherent UA/header/TLS 조합을 유지하고, Browser에서 가져온 cookie만 동기화한다.
  - `HttpIdentityOverlay`는 따라서 cookie overlay와 conditional UA overlay를 분리한다. exact profile을 사용할 때만 Scrapling의 impersonation/header 생성 이후 마지막 단계에서 Browser UA override를 적용하며, exact profile이 없으면 UA override를 생략한다.
  - 이 동기화는 Scrapling/FetcherSession의 private cookie-jar 속성을 직접 수정하는 방식으로 구현하지 않는다. public request 인자를 통한 identity overlay를 기준 계약으로 한다.
  - HTTP `FetcherSession`의 TLS fingerprint는 Browser가 Chromium 계열인 현재 설계에 맞춰 Chrome 계열 impersonation을 사용한다. **지원되지 않는 Browser major의 UA 문자열만 raw header로 덮어써 TLS/client-hint/header fingerprint와 버전이 어긋나는 구성을 금지한다.**
  - Browser `navigator.userAgent`에서 추출한 Chrome/Chromium major에 대응하는 exact impersonation profile 지원 여부는 현재 설치된 Scrapling/curl_cffi capability를 기준으로 판단한다. 지원 profile 목록을 `user-agents.txt` 같은 외부 UA 문자열 파일로 대체하지 않는다.
  - exact profile 판정은 `HttpImpersonationResolver.resolve_exact_chrome(major) -> str | None` 한 곳으로 캡슐화한다. resolver는 현재 설치된 Scrapling/curl_cffi가 노출하는 지원 profile 정보 또는 애플리케이션이 해당 설치 버전에 대해 검증한 versioned profile set만 사용하고 private 내부 enum/상수에 직접 결합하지 않는다.
  - resolver가 지원 여부를 확실히 확인하지 못하거나 version parsing/profile lookup이 실패하면 **exact profile이 없는 것으로 보수적으로 처리**한다. 즉 false positive로 잘못된 raw Browser UA를 강제 적용하는 것보다 false negative로 UA override를 생략하는 것을 우선한다.
  - `resolve_exact_chrome(142) -> "chrome142"`처럼 정확히 일치하는 versioned profile만 exact로 인정한다. `chrome`, `chrome145` 같은 다른/alias profile을 Browser major 142의 exact match라고 간주하지 않는다.
  - exact profile이 없을 때 generic `impersonate="chrome"`로 fallback하는 것은 허용하지만, 이 경우 Browser 실제 UA까지 동기화되었다고 표현하지 않는다. 동기화 대상은 cookie이며 HTTP UA/header/TLS identity는 Scrapling이 선택한 coherent Chrome profile을 따른다.
  - 본 정책의 목적은 Browser identity를 byte-for-byte 복제하는 것이 아니라 **challenge/session cookie를 HTTP로 넘기면서도 HTTP 자체의 UA/header/TLS fingerprint 일관성을 깨뜨리지 않는 것**이다.
  - 이후 Browser fallback이 다시 성공하면 **해당 Proxy slot 안에서** freshness sequence를 통과한 candidate만 active cookie, 실제 Browser UA, Browser major snapshot을 갱신하고 exact profile 여부도 다시 평가한다.
- HTTP browser impersonation
  - HTTP mode는 Scrapling `FetcherSession(stealthy_headers=True, impersonate=...)`의 browser-like header 생성과 TLS impersonation을 기본 식별자로 사용한다.
  - 별도 `user-agents.txt`, raw User-Agent 목록 rotation, request-count 기반 UA 교체 기능은 제공하지 않는다. UA 문자열만 임의 교체해 Scrapling의 TLS/header fingerprint와 불일치시키는 구성을 피한다.
  - 새 HTTP logical attempt는 Scrapling의 `impersonate="chrome"` 또는 지원되는 Chrome-family profile을 사용한다. 선택된 Proxy의 Browser identity가 있으면 **exact Browser major profile이 있을 때 exact profile + Browser actual UA**, 없으면 **동일 Proxy의 cookie sync + Scrapling coherent Chrome profile**을 사용한다.
- Proxy
  - Proxy는 **Job당 하나의 Scrapling `ProxyRotator` + configured Proxy pool**로 구성한다. 새 HTTP logical attempt에 한하여 `get_proxy()`를 호출하고, 각 Proxy에 대응하는 고정 HTTP/Browser session을 선택한다. Proxy 하나만 설정된 경우에도 같은 로직을 사용하며 동일 Proxy가 반복 선택된다.
  - configured Proxy마다 안전한 `proxy_id`(예: `proxy-0001`)를 부여한다. `proxy_id`와 session mapping은 해당 실행/ exact-resume의 Proxy pool 순서와 항목에 대해 안정적이어야 하며 비밀정보를 담지 않는다.
  - `BrowserIdentityStore: dict[proxy_id, BrowserIdentityState]`는 각 Proxy에 대응하는 cookie/UA/Chrome major만 보관한다. HTTP `FetcherSession`과 Browser `AsyncStealthySession` 모두 Proxy별로 생성해 cookie jar와 network identity를 격리한다.
  - '동일 Proxy 유지'는 애플리케이션이 동일한 proxy endpoint 및 인증/세션 설정을 선택한다는 의미다. Rotating residential gateway처럼 **같은 endpoint가 요청마다 다른 출구 IP를 할당하는 공급자**에서는 실제 egress IP가 유지된다고 보장할 수 없다. IP 고정이 필수인 Proxy 제공자는 sticky-session credential/parameter 등을 지원해야 하며 배포 환경에서 실제 egress IP 유지 여부를 확인해야 한다.
  - 애플리케이션 설정 소스로는 CLI/마법사에서 명시한 값과 실행 위치의 `.env`만 사용하며, `HTTP_PROXY`, `HTTPS_PROXY` 같은 시스템 환경변수를 구성 소스로 사용하지 않는다.
  - Proxy는 CLI/마법사에서 명시 입력한 값 또는 실행 위치의 `.env` 파일 값만 사용한다.
  - Proxy 설정은 실행 파일과 같은 위치의 `.env` 파일에 저장한다.
  - `.env`는 사용자가 직접 편집할 수 있도록 비인코딩 평문으로 저장한다.
  - 로컬 `.env`에서 사용하는 Proxy key는 컨테이너에서도 같은 public ENV 이름을 재사용하며, 정확한 이름과 기본값은 §8.2.11을 따른다.
  - 로컬 실행 Flow
    - 사용자가 터미널에서 Proxy 옵션을 켠 경우 이를 최우선으로 처리한다.
    - Proxy 옵션을 켜면 하나 이상의 host/URL과 필요 시 ID/PW를 터미널에서 입력받고, 입력한 값을 `.env`에 저장한 뒤 현재 실행에 사용한다.
    - 사용자가 Proxy 옵션을 켜지 않았고 실행 파일과 같은 위치에 `.env`가 있으면 `.env`의 Proxy 변수를 자동 사용한다.
    - `.env`가 없거나 유효한 Proxy 변수가 없으면 Proxy를 비활성화한다.
  - 컨테이너 실행 Flow
    - 컨테이너 이미지는 `docker-entrypoint.sh`를 포함한다.
    - `.dockerignore`는 `.git`, 가상환경, 테스트 캐시, 출력물, `.env` 계열 secret 후보, 문서 산출물, CDK synth 산출물을 Docker build context에서 제외한다.
    - `docker run` 시 Proxy 환경변수를 주입할 수 있으며 정확한 public ENV 이름은 §8.2.11을 따른다.
    - Docker `--env-file`을 통해 `.env` 형식 파일을 환경변수로 주입하는 방식도 허용한다.
    - `docker-entrypoint.sh`는 Docker 전용 시작 스크립트이며, 전달받은 argv를 유지한 채 `python -m fmkorea_scraper.adapters.container "$@"`로 실행을 위임한다.
    - 컨테이너 내부 프로그램은 Docker가 주입한 환경변수를 직접 읽어 Proxy를 적용한다.
  - `.env`는 문서화된 보안 예외로 평문 저장을 허용하되, `.gitignore` 포함과 터미널 경고를 필수로 한다.
  - 로그에는 Proxy password, raw `.env` 내용, 인증정보를 남기지 않는다.
- Cookie
  - 사용자가 만든 cookie 파일을 읽거나 HTTP 요청에 적용하는 기능은 제공하지 않는다.
  - CLI, 터미널 마법사, 컨테이너, MCP에는 사용자 cookie 파일 경로 입력 옵션을 두지 않는다.
  - Proxy별 HTTP `FetcherSession`과 Browser `AsyncStealthySession`은 각각 자신의 session 상태와 cookie jar를 Spider 생명주기 동안 유지한다. 다른 Proxy의 HTTP session과도 jar를 공유하지 않는다.
  - 같은 Proxy에 속한 HTTP와 Browser session의 cookie jar 역시 자동 공유된다고 가정하지 않는다. Browser fallback 성공 시 `BrowserIdentityBridge`가 해당 Proxy의 Browser cookie를 그 Proxy의 HTTP request identity overlay로만 동기화한다.
  - Browser cookie snapshot은 current Spider cycle의 Proxy별 메모리에만 존재하며, 이후 rotator가 **동일 Proxy를 재선택한** HTTP 요청에만 `cookies=`로 주입한다.
  - Job 재시작 또는 application-level reconstruction resume에서는 이전 session cookie를 재사용하지 않는다. Scrapling crawler state는 request queue를 복원하지만 Browser profile/cookie 영속 저장소로 사용하지 않는다.
  - raw cookie는 로그, checkpoint, output JSONL, report, failure record에 저장하지 않는다.
- 스크래핑 속도 조절
  - 요청 속도는 Scrapling Spider의 `download_delay`, `concurrent_requests`, `concurrent_requests_per_domain`을 기준으로 제어한다.
  - `autothrottle_enabled=true`인 경우 Scrapling AutoThrottle을 사용하고, `download_delay`는 최소 지연값(floor)으로 유지한다.
  - Blocking backoff는 Scrapling AutoThrottle의 block backoff와 `max_blocked_retries`를 사용하며 별도 HTTP status retry loop를 중복 구현하지 않는다.
  - `500/502/503/504`는 Scrapling 기본 `BLOCKED_CODES`에 포함되므로 HTTP-only transport retry로 우회하지 않고 blocked-retry 및 Browser fallback 경로를 따른다. DNS 오류, connection reset, timeout처럼 HTTP response 자체가 생성되지 않은 transport exception만 session-level retry 경로에서 처리한다.
  - Browser 동시성은 Spider global concurrency와 `AsyncStealthySession(max_pages=...)`의 page pool에 의해 함께 제한된다.
  - HTTP 단계에서 Blocking이 감지되면 같은 Proxy의 Browser fallback으로 전환한다. Browser까지 Blocking이면 남은 retry budget에서 다음 HTTP logical attempt가 새 Proxy를 선택한다. 전역 sticky/probe 없이 각 logical request를 독립적으로 처리하며 마지막 Browser phase까지 Blocking인 경우에만 Checkpoint를 저장하고 중단한다.

### 2.7. 실패 기록 및 재처리
- 실패는 `failures.jsonl`에 저장한다.
- parser-stage 실패는 추후 재처리 유닛에서 direct-post 경로로 재수집한다.
- 네트워크, 차단, 저장소 실패는 재처리 유닛 대상에서 제외하고 기존 retry/checkpoint/운영 대응 절차에서 처리한다.
- 실패 기록 필드
  - `schema_version="fmkorea.failure.v1"`
  - `job_id`
  - `stage`
  - `reason`
  - `retryable`
  - `url`
  - `post_key`
    - `site_id`
    - `board_id`
    - `document_srl`
    - `document_id_raw`
  - `safe_message`
  - `details`
  - `created_at`
- 실패 JSONL 한 줄 예시
```json
{
  "schema_version": "fmkorea.failure.v1",
  "job_id": "job-a",
  "stage": "post",
  "reason": "ParseFailureError",
  "retryable": true,
  "url": "https://www.fmkorea.com/best/10195585295",
  "post_key": {
    "site_id": "fmkorea",
    "board_id": "best",
    "document_srl": 10195585295,
    "document_id_raw": "10195585295"
  },
  "safe_message": "required detail metadata missing",
  "details": {
    "job_id": "job-a",
    "stage": "post",
    "reason": "ParseFailureError",
    "retryable": true,
    "url": "https://www.fmkorea.com/best/10195585295",
    "post_key": {
      "site_id": "fmkorea",
      "board_id": "best",
      "document_srl": 10195585295,
      "document_id_raw": "10195585295"
    }
  },
  "created_at": "2026-08-11T12:05:00+09:00",
  "metadata": {}
}
```
- 재처리 계약
  - CLI 명령: `fmkorea-scraper retry-parser-failures --failure-path <failures.jsonl>`
  - `--existing-success-path <job_YYYYMMDD_HHMMSS.jsonl>`를 지정하면 이미 성공한 게시글은 제외한다.
  - `--from-report`, `--from-checkpoint`는 재처리 입력 복원을 돕는 옵션이며 동시에 사용할 수 없다.
  - `RetryRequest`는 `failure_input`, `skip_existing_success`, `existing_success_path`, `from_report_path`, `from_checkpoint_path`, `retry_job_id`를 가진다.
  - 재처리는 `PostDetailService`와 Spider의 direct-post request 경로를 재사용한다.

### 2.8. public 실행 계약
- Python import API
  - `search_board(request, runtime=None)`
  - `search_integrated(request, runtime=None)`
  - `fetch_post(request, runtime=None)`
  - `fetch_comments(request, runtime=None)`
  - `run_job(config, runtime=None, run_id=None)`
  - `resume_job(request, runtime=None, run_id=None)`
  - `retry_parser_failures(request, retry_service=None)`
  - 위 수집 API는 별도 HTTP client 객체를 public contract로 노출하지 않고 내부적으로 `FMKoreaSpider`와 session manager를 구성한다.
  - `run_job`, `resume_job`, realtime Job 같은 **Job 실행 경로는 `Spider.stream()`을 기본으로 사용**한다. 동기 CLI/라이브러리 adapter는 내부 async runner에서 `stream()`을 소비하고, async MCP/adapter는 직접 `async for`로 소비한다.
  - Scrapling `Spider.stream()`은 `Spider.start()`와 달리 built-in SIGINT pause handler를 설치하지 않으므로, CLI/container의 application runner가 `SIGINT`/`SIGTERM`을 직접 처리한다.
  - 첫 graceful stop signal을 받으면 runner는 공용 `stop_event`를 set하고 Scrapling의 동기 메서드인 `spider.pause()`를 정확히 한 번 호출한다. `pause()` 자체를 `await`하지 않는다. realtime polling wait도 같은 `stop_event`를 감시하므로 즉시 깨어나며 새로운 poll request를 만들지 않는다.
  - `spider.pause()`는 pause 요청을 engine에 전달하고 즉시 반환하는 control 호출이다. 실제 crawl 종료 및 `crawldir` checkpoint 저장은 Scrapling engine이 남은 in-flight request/callback이 정리된 뒤 수행하므로 application runner는 `Spider.stream()` 종료까지 계속 소비/대기한다.
  - `spider.pause()` 요청 후에는 이미 실행 중인 request/callback이 종료될 때까지 기다리고 Scrapling `crawldir` checkpoint를 저장한 뒤 stream을 닫는다. 이후 이미 durable commit된 staging records의 finalization/enrichment를 수행하고 애플리케이션 checkpoint는 `status="stopped"`, `reason="manual_stop"`을 유지한다.
  - graceful stop 처리 중 사용자가 두 번째 interrupt로 강제 종료하면 즉시 process cancellation을 허용하며, 이 경우 첫 번째 graceful pause 이후 아직 완료되지 않은 추가 checkpoint/finalization은 보장하지 않는다.
  - MCP/라이브러리 adapter가 cooperative cancellation을 제공하는 경우에도 같은 `stop_event → spider.pause()` 경로를 재사용한다. process kill/container hard kill은 graceful checkpoint를 보장하지 않는다.
  - `fetch_post`, `fetch_comments`처럼 결과 규모가 작고 호출자가 즉시 collection을 원하는 convenience API는 필요할 때 `Spider.start()`를 사용할 수 있다.
  - `on_scraped_item()`은 crawl 결과를 `OutputStorePort.append_post()`에 전달한 뒤 성공한 item dict를 그대로 반환한다. binary media download는 수행하지 않는다. backend가 즉시 durable commit하지 않는 S3 batch 모드에서는 반환 item과 storage commit 시점이 분리될 수 있으며, `completed_keys`는 `StorageCommitReceipt`/`flush_pending()`의 durable commit 결과만 반영한다. `None`은 validation/filter 정책상 실제로 item을 drop할 때만 사용한다.
  - Job runner는 정상 crawl 종료 후 `media_mode="download"`이면 `MediaEnrichmentPipeline`을 실행하고, 그렇지 않으면 staging records를 바로 최종 JSONL로 finalize한다. graceful `stopped` run도 이미 durable commit된 staging records는 partial output으로 finalize/enrich할 수 있지만 Job status는 `stopped`로 유지한다.
  - Job runner가 stream item 자체를 별도로 보관할 필요가 없으면 반환 item을 소비 즉시 버리고 report/count만 갱신한다. 이 방식은 Scrapling `ItemList` 전체 누적 없이 대규모 Job을 처리한다.
  - `fetch_comments`의 public `CommentRequest`는 `url`, `board_id`, `document_srl`, `comment_mode`만 받는다. `max_comment_pages`를 직접 받지 않으므로 기본 호출은 상세 HTML에 포함된 댓글 중심으로 반환한다.
  - 추가 댓글 페이지까지 포함하려면 `fetch_post(PostRequest(..., scrape_options=ScrapeOptions(max_comment_pages=N)))` 또는 Job 실행 경로를 사용한다.
  - `run_job`, `resume_job`는 `SpiderFactory`/runtime dependency를 통해 `FMKoreaSpider`를 구성한다. CLI/Docker/MCP가 서로 다른 crawler 구현을 중복 생성하지 않는다.
  - `retry_parser_failures`는 `retry_service`를 주입하지 않으면 기본 `ParserFailureRetryService`를 사용한다.
- CLI 진입점
  - `python main.py ...`
  - `python -m fmkorea_scraper.adapters.cli ...`
  - 설치 후 `fmkorea-scraper ...`
- CLI public configuration
  - 이 절은 실행 기능의 개요만 설명한다. **CLI 옵션명, 타입, 기본값, 적용 source, validation 및 alias의 normative 계약은 §8.2.1–§8.2.10을 유일한 기준으로 사용한다.**
  - 범위, 저장 위치, 수집/파서, Spider, User-Agent, Proxy, resume, retry 관련 세부 옵션을 이 절에서 중복 열거하지 않는다.
  - Browser 전환은 고정 실행 정책이며 enable/disable CLI 옵션을 제공하지 않는다는 정책 자체는 유지하고, 관련 조정 옵션의 정확한 계약은 §8.2.5를 따른다.
- CLI subcommand
  - `run`
  - `resume`
  - `retry-parser-failures`
  - `configure-proxy`
  - `wizard`
- `run --source` 값
  - `direct-post`
  - `board-list`
  - `board-search`
  - `integrated-search`
  - `deep-search`
  - `no-keyword-range`
  - `realtime-keyword`
  - `realtime-all`
- `run` source별 주요 옵션
  - `direct-post`: `--url`을 1회 이상 입력한다.
  - `board-list`: `--board-id`, `--category-id`, `--page-start`, `--page-end`, 범위 옵션을 사용한다.
  - `board-search`: `--board-id`, `--keyword`, `--search-target`, `--category-id`, `--page-start`, `--page-end`, 범위 옵션을 사용한다.
  - `integrated-search`: `--keyword`, `--search-target`, `--page-start`, `--page-end`, 범위 옵션을 사용한다.
  - `deep-search`: `--keyword`, `--search-target`, `--seed-page-limit`, `--board-candidate`, 범위 옵션을 사용한다. `--board-candidate`는 통합 검색 seed에서 발견한 게시판 외에 사용자가 직접 추가할 게시판 ID를 반복 입력하는 옵션이다.
  - `no-keyword-range`: `--board-id`, `--category-id`, 범위 옵션을 사용한다.
  - `realtime-keyword`: `--scope`, `--keyword`, 필요 시 `--board-id`, `--search-target`, `--poll-interval-seconds`, 그리고 종료 조건인 `--duration-seconds` 또는 `--end-at` 중 하나를 사용한다.
  - `realtime-all`: `--board-id`, 선택적 `--category-id`, `--poll-interval-seconds`, 그리고 종료 조건인 `--duration-seconds` 또는 `--end-at` 중 하나를 사용한다.
- `retry-parser-failures` 주요 옵션
  - 필수: `--failure-path`
  - 선택: `--existing-success-path`, `--from-report`, `--from-checkpoint`, `--retry-job-id`, `--job-id`, `--stage`
  - `--stage`를 생략하면 `post` stage만 재처리 대상으로 삼는다.
  - 기본값은 `--skip-existing-success`이며, `--no-skip-existing-success`로 기존 성공 결과 중복 제외를 끌 수 있다.
  - `--from-report`와 `--from-checkpoint`는 동시에 사용할 수 없다.
- MCP tool
  - `search_board`
  - `search_integrated`
  - `fetch_post`
  - `fetch_comments`
  - `run_job`
  - `resume_job`
  - `retry_parser_failures`
  - 모든 MCP tool은 성공 시 `{"ok": true, "data": ...}`를 반환한다.
  - 실패 시 `{"ok": false, "error": <safe_error_payload>}`를 반환한다.
  - MCP error payload에는 stage, reason, retryable 여부, safe message만 포함하고 secret, cookie, raw header, raw body는 포함하지 않는다.
- 컨테이너 계약
  - Docker entrypoint는 `docker-entrypoint.sh`다.
  - entrypoint는 전달받은 argv를 유지해 `python -m fmkorea_scraper.adapters.container "$@"`를 실행한다.
  - 컨테이너는 `run`, `retry-parser-failures`, `RESUME=true` 흐름을 지원한다.
  - 컨테이너 환경변수의 이름, 필수 여부, 기본값 및 validation은 **§8.2.11을 유일한 기준**으로 사용하며 이 절에서 목록을 중복 정의하지 않는다.
  - 실시간 컨테이너 Job도 일반 Job과 동일하게 CLI argv 또는 동등한 `JobConfig.source`로 source/종료 조건을 전달한다. 컨테이너 환경변수는 §8.2.11의 storage/runtime 계약만 사용한다.
  - CLI `resume`은 기본적으로 로컬 `Checkpoint.json`을 읽는 흐름이다.
  - 컨테이너 `resume`은 `RESUME=true`와 `JOB_ID=<checkpoint job_id>`를 환경변수로 받고 PostgreSQL의 애플리케이션 Job checkpoint를 읽는다.
  - 컨테이너 실행은 결과 JSONL/미디어를 S3 호환 저장소에 쓰고 애플리케이션 Job checkpoint를 PostgreSQL에 쓰는 구성을 기본 계약으로 한다. Scrapling crawler state는 별도 persistent `crawldir`가 있을 때만 scheduler-state exact resume이 가능하다.

### 2.9. core model 및 port 계약
- 공통 모델 규칙
  - public contract는 Pydantic v2 모델로 정의한다.
  - 모든 public model은 `extra="forbid"`로 선언되지 않은 필드를 거부한다.
  - 확장이 필요한 모델은 공통 `metadata: dict[str, Any]` 필드를 가진다.
  - `metadata`는 JSON 직렬화 가능 값만 허용하고, 깊이와 byte 크기를 제한한다.
- `JobConfig`
  - 필드: `job_id`, `job_type`, `site_id`, `source`, `scrape_options`, `crawl_config`, `storage_config`, `created_at`
  - `site_id`는 `"fmkorea"` 고정이다.
  - `job_type.value`는 `source.type`과 같아야 한다.
- source 모델
  - `DirectPostSource`: `type="direct_post"`, `urls`
  - `BoardListSource`: `type="board_list"`, `board_id`, `category_id`, `page_start`, `page_end`, `range_filter`
  - `BoardSearchSource`: `type="board_search"`, `board_id`, `category_id`, `keyword`, `search_target`, `page_start`, `page_end`, `range_filter`
  - `IntegratedSearchSource`: `type="integrated_search"`, `keyword`, `search_target`, `page_start`, `page_end`, `range_filter`
  - `DeepSearchSource`: `type="deep_search"`, `keyword`, `seed_search_target`, `seed_page_limit`, `board_ids`, `range_filter`
  - `NoKeywordRangeSource`: `type="no_keyword_range"`, `board_id`, `category_id`, `range_filter`
  - `RealtimeKeywordSource`: `type="realtime_keyword"`, `scope`, `board_id`, `keyword`, `search_target`, `poll_interval_seconds`, `termination`
  - `RealtimeAllSource`: `type="realtime_all"`, `board_id`, `category_id`, `poll_interval_seconds`, `termination`
  - `ParserFailureRetrySource`: `type="parser_failure_retry"`, `failure_input`, `skip_existing_success`
  - `deep_search`와 `no_keyword_range`는 기간 또는 글번호 범위 중 하나 이상을 요구한다.
  - realtime source의 `poll_interval_seconds` 기본값은 30.0초다.
  - realtime source의 종료 조건은 `duration_seconds` 또는 `end_at` 중 정확히 하나다.
  - `duration_seconds`는 각 resume run마다 다시 시작되는 상대 timer가 아니다. 최초 Job 시작 시 `job_started_at + duration_seconds`를 `realtime_deadline_at`으로 계산하고 이후 resume에서도 같은 deadline을 사용한다.
  - `RealtimeKeywordSource.scope`는 `board` 또는 `integrated`다. `board`이면 `board_id`가 필수이고, `integrated`이면 `board_id`는 사용하지 않는다.
  - realtime source는 `RealtimeCoordinator`가 Job당 하나의 장기 실행 `FMKoreaSpider`를 시작하고, 해당 Spider 내부 polling callback이 종료 조건에 도달할 때까지 다음 poll request를 이어서 생성하는 계약이다. polling마다 Spider/SessionManager를 재생성하지 않는다.
- 실행 옵션 모델
  - `ScrapeOptions` 기본값: `comment_mode="all"`, `media_mode="url_only"`, `media_download_concurrency=2`, `include_attachments=true`
  - `ScrapeOptions` 제한값: `max_comment_pages`, `max_content_blocks`, `max_comments`; `media_download_concurrency`는 1 이상의 정수다.
  - `CrawlConfig` 기본값: `concurrent_requests=4`, `concurrent_requests_per_domain=2`, `download_delay=1.0`, `max_blocked_retries=3`, `autothrottle_enabled=false`, `autothrottle_start_delay=2.0`, `autothrottle_max_delay=30.0`, `browser_max_pages=2`, `checkpoint_interval_seconds=300.0`, `robots_txt_obey=false`
  - `CrawlConfig.max_blocked_retries`는 1 이상의 홀수 정수다. `0`은 HTTP→Browser fallback을 비활성화하며 짝수는 마지막 blocked retry가 HTTP에서 끝나 HTTP/Browser logical-attempt 쌍을 완성할 수 없으므로 허용하지 않는다.
  - `RangeFilter`: `date_from`, `date_to`, `min_document_srl`, `max_document_srl`
- 저장소 모델
  - `StorageConfig`: `output_mode`, `job_checkpoint_mode`, `base_output_path`, `s3`, `postgres`, `s3_flush_records`, `s3_flush_bytes`, `s3_flush_seconds`
  - `output_mode="local"`이면 로컬 JSONL/미디어 저장소를 사용한다.
  - `output_mode="s3_compatible"`이면 `S3StorageConfig(endpoint_url, bucket, prefix, region)`가 필수다.
  - `job_checkpoint_mode="local"`이면 `<base_output_path>/Checkpoint.json`을 사용한다.
  - `job_checkpoint_mode="postgres"`이면 `PostgresJobCheckpointConfig(dsn)`이 필수다.
  - S3 JSONL batch flush 기본값은 `s3_flush_records=100`, `s3_flush_bytes=8388608`, `s3_flush_seconds=30.0`이다. 세 조건 중 먼저 도달하면 immutable segment를 업로드한다.
- Spider 런타임 모델
  - `SpiderRuntimeConfig`는 `JobConfig`에 저장하지 않는 실행 시점 설정이다.
  - 필드: `http_timeout_seconds`, `browser_timeout_ms`, `http_transport_retries`, `browser_transport_retries`, `proxies`, `proxy_env_path`, `execution_dir`, `persistent_state_root`
  - public 컨테이너 환경변수 이름은 기존 `FMKOREA_CRAWDIR`을 유지하지만 container adapter가 이를 내부 `SpiderRuntimeConfig.persistent_state_root`로 정규화한다. 이 값 자체를 Scrapling `Spider(crawldir=...)`에 그대로 넘기지 않는다.
  - 일반(non-realtime) exact-resume Job에서 `SpiderFactory`는 `persistent_state_root`로부터 실제 Scrapling `crawldir=<root>/crawl/<job_id>`과 persistent staging `<root>/staging/<job_id>/<run_id>`을 파생한다. 로컬 실행에서는 `base_output_path`로부터 기존 `.crawl/<job_id>`/output staging 경로를 파생할 수 있다. realtime Job은 persistent staging을 사용할 수 있어도 scheduler exact-resume에는 이 crawler state를 사용하지 않는다.
  - 일반-purpose `default_headers` runtime 설정은 제공하지 않는다. HTTP browser identity 관련 `User-Agent`, `Sec-CH-UA*`, `Cookie` 등은 Scrapling impersonation과 `BrowserIdentityBridge`가 관리하고, referer/Browser `extra_headers`처럼 요청별로 필요한 헤더만 해당 request 생성 경로에서 명시적으로 구성한다.
  - HTTP와 Browser timeout 단위가 다를 수 있으므로 하나의 `TimeoutConfig(connect/read/write/pool)`로 추상화하지 않는다.
  - Blocking response의 재시도는 `CrawlConfig.max_blocked_retries`가 담당하고, session-level retry는 transport exception 처리에만 사용한다.
  - `BrowserIdentityState`는 Proxy별 runtime-only mutable state이며 `proxy_id`, `cookies`, `browser_user_agent`, `browser_chrome_major`, `http_impersonation`, `http_user_agent_override_enabled`, `last_browser_success_at`, `capture_seq`를 가진다. Spider는 `BrowserIdentityStore: dict[proxy_id, BrowserIdentityState]`를 유지한다.
  - 각 `BrowserIdentityStore[proxy_id]`의 초기 `capture_seq`는 `0`이며, 같은 Proxy에서 더 큰 sequence의 non-blocked candidate만 해당 slot을 교체한다.
  - `BrowserIdentityCandidate`는 runtime-only temporary model이며 `proxy_id`, `capture_seq`, `cookies`, `browser_user_agent`, `browser_chrome_major`, `captured_at`을 가진다. blocked response의 candidate와 동일 Proxy 내 stale candidate는 폐기한다.
  - `BrowserPageActionComposer`는 request-specific Browser readiness와 identity candidate capture의 실행 순서를 책임지고, `BrowserIdentityBridge`는 monotonic sequence 발급과 response 후 promotion을 책임진다. 둘을 분리해 `page_action`에서 검증 전 active identity를 직접 갱신하지 않는다.
  - `http_user_agent_override_enabled=true`는 `browser_chrome_major`와 정확히 일치하는 supported impersonation profile을 선택한 경우에만 허용한다. exact profile이 없으면 `http_impersonation`은 generic/supported Chrome profile을 가리키고 `http_user_agent_override_enabled=false`로 둔다.
  - `HttpImpersonationResolver`는 Browser Chrome major에 정확히 대응하는 versioned impersonation profile을 확인하는 runtime helper다. 확실한 exact match를 찾지 못하면 `None`을 반환하고 generic Chrome impersonation + UA override disabled 경로를 사용한다.
  - `BrowserIdentityState`는 `JobConfig`, Scrapling `crawldir`, 애플리케이션 Job checkpoint에 직렬화하지 않는다.
  - `BrowserIdentityCandidate`, `identity_capture_seq`도 process-local이며 checkpoint/crawldir에 직렬화하지 않는다. process restart/resume에서는 sequence를 초기화하고 새 Browser context에서 identity를 다시 획득한다.
  - `ProxyAttemptCoordinator`는 Spider/Job당 하나의 `ProxyRotator`, `proxy_id`↔session mapping, logical request의 `proxy_attempt`/phase 전환, sanitized `proxy_log` 생성을 담당한다. 별도의 worker pool이나 scheduler/queue를 추가하지 않는다.
  - `ScheduledPostKeyTracker`는 현재 process/run에서만 유지되는 logical reservation state다. `claim(post_key) -> bool`은 별도 `anyio.Lock` 아래 atomic하게 동작하며 첫 claim만 `true`를 반환한다. checkpoint/crawldir에 별도 직렬화하지 않고 reconstruction resume 시 비운 상태로 시작하며, `CompletedKeyTracker`가 durable 완료 게시글을 다시 거른다.
- runtime port
  - `OutputStorePort`: `append_post`, `flush_pending`, `write_report`, `finalize`
  - `append_post`는 backend별 staging에 record를 추가하고 `StorageCommitReceipt`를 반환한다. 로컬 persistent backend에서는 즉시 committed receipt가 될 수 있지만 S3 batch backend에서는 아직 `pending`일 수 있다.
  - `flush_pending()`은 현재 open batch/segment를 durable backend에 반영하고 새로 commit된 post key 목록을 반환한다. Job runner는 이 반환값을 기준으로 `completed_keys`를 갱신한다.
  - `StorageCommitReceipt`는 최소 `post_key`, `durable`, `staging_ref`를 가지며, batch flush가 여러 record를 함께 commit할 때는 `committed_keys`를 반환할 수 있다. `durable=false`인 receipt만으로 `completed_keys`를 갱신하지 않는다.
  - `MediaStorePort`: `save_media`, `save_metadata_only`
  - `FailureStorePort`: `write_failure`, `read_failures`
  - `JobCheckpointStorePort`: `save`, `load`, `load_latest`
  - `LoggerPort`: `debug`, `info`, `warning`, `error`
  - output/checkpoint port에서 durability/control-plane 보장을 깨뜨리는 예외는 parser failure로 변환하지 않고 runner의 terminal storage/control-plane error 경로로 전파한다. 단, recoverable S3 segment upload failure는 closed pending batch로 유지할 수 있으면 다음 flush까지 보류할 수 있다.
- report 모델
  - `JobReport`: `schema_version="fmkorea.job_report.v1"`, `job_id`, `status`, `started_at`, `finished_at`, `collected_count`, `stored_count`, `unavailable_count`, `failure_count`, `media_downloaded_count`, `media_failure_count`, `enrichment_status`, `output_refs`
  - `enrichment_status="completed_with_warnings"`는 개별 media 다운로드 실패가 일부 존재하지만 최종 게시글 JSONL 생성은 성공한 상태이며 Job `status="completed"`와 함께 사용할 수 있다.
  - `enrichment_status="failed"`는 enrichment pipeline 자체가 최종 output을 만들지 못한 상태이며 Job `status="stopped"`, `reason="error"`와 함께 사용한다. 복구 가능한 `staging_ref`는 유지한다.
  - `RetryReport`는 `selected_count`, `skipped_count`를 추가한다.
  - `StoredObjectRef`: `storage_mode`, `path`, `bucket`, `object_key`
- `JobReport` JSON 예시
```json
{
  "schema_version": "fmkorea.job_report.v1",
  "job_id": "job-a",
  "status": "completed",
  "started_at": "2026-08-11T12:00:00+09:00",
  "finished_at": "2026-08-11T12:10:00+09:00",
  "collected_count": 120,
  "stored_count": 117,
  "unavailable_count": 1,
  "failure_count": 2,
  "media_downloaded_count": 35,
  "media_failure_count": 1,
  "enrichment_status": "completed_with_warnings",
  "output_refs": [
    {
      "storage_mode": "local",
      "path": "D:\\HongDev\\ELSAPHABA\\FMkorea-Scraper\\outputs\\job-a\\run-1\\job_20260811_120000.jsonl",
      "bucket": null,
      "object_key": "outputs/job-a/run-1/job_20260811_120000.jsonl",
      "metadata": {}
    },
    {
      "storage_mode": "local",
      "path": "D:\\HongDev\\ELSAPHABA\\FMkorea-Scraper\\outputs\\job-a\\run-1\\failures.jsonl",
      "bucket": null,
      "object_key": "outputs/job-a/run-1/failures.jsonl",
      "metadata": {}
    }
  ],
  "metadata": {}
}
```
- `RetryReport` JSON 예시
```json
{
  "schema_version": "fmkorea.job_report.v1",
  "job_id": "retry-job-a",
  "status": "completed",
  "started_at": "2026-08-11T13:00:00+09:00",
  "finished_at": "2026-08-11T13:05:00+09:00",
  "collected_count": 10,
  "stored_count": 9,
  "unavailable_count": 0,
  "failure_count": 1,
  "media_downloaded_count": 0,
  "media_failure_count": 0,
  "enrichment_status": "not_requested",
  "output_refs": [],
  "selected_count": 10,
  "skipped_count": 3,
  "metadata": {}
}
```

## 3. 기술 스택
- Python
  - 지원 버전: Python `>=3.11`
  - 패키지 방식: `pyproject.toml` + hatchling
- 기본 의존성
  - `anyio>=4,<5`
  - `pydantic>=2,<3`
  - `python-dotenv>=1,<2`
  - `scrapling[fetchers]>=0.4.15,<0.5`
  - Browser fallback이 고정 정책이므로 fetcher/browser 의존성은 선택 extra가 아니라 기본 런타임 의존성으로 취급한다.
- 선택 extra
  - `s3`: `boto3>=1.34,<2`
  - `postgres`: `psycopg[binary]>=3,<4`
  - `mcp`: `mcp>=1,<2`
  - `analysis`: `polars>=1,<2`
  - `test`: `hypothesis>=6,<7`, `pytest>=8,<9`
- Spider 세션 런타임
  - Scrapling `FetcherSession`을 기본 HTTP 요청 경로로 사용한다.
  - Blocking fallback의 Browser 엔진은 Scrapling `StealthyFetcher` 계열로 고정한다.
  - Spider 내부에서는 `StealthyFetcher`를 one-off로 반복 생성하지 않고, 해당 계열의 persistent async session인 `AsyncStealthySession`을 사용한다.
  - Browser session의 lazy start는 `manager.add("browser", session, lazy=True)`로 설정한다.
  - Proxy는 HTTP/Browser 두 session에 동일 정책으로 주입한다.
  - `.env` 파일 기반 Proxy 환경변수 로드를 지원한다.
  - HTTP와 Browser session은 각각 독립적인 cookie/session state를 유지한다. Browser fallback에서는 readiness 완료 후 candidate identity를 캡처하고, `BrowserIdentityBridge`가 non-blocked/fresh candidate만 active state로 승격한다. HTTP에는 active first-party cookie를 항상 동기화하고 raw Browser User-Agent는 exact Chrome-major impersonation profile이 확인된 경우에만 conditional overlay한다.
  - request routing은 전역 adaptive state 대신 각 logical request의 `HTTP(Pn) → Browser(Pn)` 체인을 사용한다. Browser phase도 Blocking이면 남은 blocked retry budget으로 `HTTP(Pn+1)`를 시작한다. 전역 sticky/probe는 사용하지 않는다.
- 파싱 런타임
  - Scrapling `Selector` 기반 HTML 파싱
  - Spider callback에 전달된 `Response`는 이미 Scrapling `Selector` 기능을 제공하므로 `Response.body`/HTML 문자열을 BeautifulSoup, lxml, 새 `Selector(...)`로 다시 파싱하지 않는다.
  - `MetadataParser`, `ContentParser`, `CommentParser`는 `Response | Selector` 또는 해당 응답에서 얻은 subtree selector를 입력으로 받아 `.css()`, `.xpath()`, `.find()`, `.find_all()` 등을 직접 사용한다.
  - 공통 DOM helper: `src/fmkorea_scraper/dom.py`
  - URL 후보 필터링, pagination 링크 추출처럼 DOM metadata 결합이 필요 없는 링크 작업에는 Scrapling `LinkExtractor`를 부분적으로 사용한다. 목록 행의 제목/날짜/말머리처럼 구조화 metadata가 필요한 부분은 기존 row parser가 담당한다.
  - HTTP 단계에서 `is_blocked()`에 의해 Blocking으로 판정된 요청은 `retry_blocked_request()`에서 Browser session으로 전환한다.
- 병렬 실행
  - Scrapling Spider scheduler와 concurrency 설정으로 요청 병렬성을 제어한다.
  - 목록/검색 callback이 상세 `Request`를 yield하고, 상세 callback이 후속 댓글 요청 또는 저장 item을 yield한다. binary 미디어 다운로드는 §2.4에 정의된 별도 정책을 따른다.
  - Browser 요청 동시성은 `AsyncStealthySession(max_pages=...)` page pool로 추가 제한한다.
  - 별도 `asyncio.Queue`, post worker pool, media worker pool을 두지 않는다.
- 저장 및 분석
  - 표준 `json` 기반 JSONL 저장
  - Polars 기반 JSONL 가공 및 분석
  - 애플리케이션 Job checkpoint는 로컬 `Checkpoint.json` 또는 PostgreSQL에 저장한다.
  - Scrapling crawler state는 `crawldir`에 저장한다.
  - 컨테이너 JSONL/미디어 출력은 S3 호환 객체 저장소
  - 기본 운영 방향은 Garage이며, SeaweedFS는 대량 미디어/소파일 저장, FUSE, Iceberg 요구가 생길 때 확장 후보로 유지
- 배포
  - Docker worker image는 `python:3.11-slim` 기반이다.
  - image build 시 `.[s3,postgres]`와 기본 의존성 `scrapling[fetchers]`를 설치하고 `scrapling install`로 Browser runtime을 설치한다.
  - AWS CDK는 VPC, S3 output bucket, RDS PostgreSQL, ECS Fargate worker, CloudWatch Logs/EventBridge 기반 실패 감지를 구성한다.
  - CDK는 Docker image build/push, 중앙 Job Queue, 통합 운영 UI, EKS 배포를 포함하지 않는다.

## 4. 디렉토리 구조
```
FMKorea-Scraper-Scrapling/
├── main.py
├── pyproject.toml
├── requirements.lock
├── Dockerfile
├── docker-entrypoint.sh
├── .dockerignore
├── README.md
├── docs/
│   └── FMKorea-Scraper_Function_Specification.md
├── infra/
│   └── cdk/
│       ├── app.py
│       ├── cdk.json
│       ├── requirements.txt
│       ├── config/
│       │   ├── dev.yaml
│       │   ├── staging.yaml
│       │   └── prod.yaml
│       ├── stacks/
│       │   ├── config.py
│       │   ├── naming.py
│       │   ├── shared_infra_stack.py
│       │   └── worker_stack.py
│       └── tests/
├── src/
│   └── fmkorea_scraper/
│       ├── __init__.py
│       ├── core/
│       │   ├── api.py
│       │   ├── errors.py
│       │   ├── normalization.py
│       │   ├── ports.py
│       │   ├── schema.py
│       │   └── validation.py
│       ├── models/
│       │   ├── base.py
│       │   ├── job_checkpoint.py
│       │   ├── comment.py
│       │   ├── content.py
│       │   ├── enums.py
│       │   ├── failure.py
│       │   ├── job.py
│       │   ├── media.py
│       │   ├── post.py
│       │   └── report.py
│       ├── spider/
│       │   ├── __init__.py
│       │   ├── fmkorea.py
│       │   ├── sessions.py
│       │   ├── requests.py
│       │   ├── blocking.py
│       │   ├── adaptive.py
│       │   ├── identity.py
│       │   ├── factory.py
│       │   └── runtime.py
│       ├── application/
│       │   ├── jobs.py
│       │   ├── media_enrichment.py
│       │   ├── realtime.py
│       │   └── resume.py
│       ├── collect/
│       │   ├── models.py
│       │   ├── parsers.py
│       │   ├── policies.py
│       │   ├── service.py
│       │   └── urls.py
│       ├── parse/
│       │   ├── comments.py
│       │   ├── content.py
│       │   ├── media.py
│       │   ├── metadata.py
│       │   ├── models.py
│       │   ├── service.py
│       │   └── warnings.py
│       ├── outputs/
│       │   ├── checkpoint.py
│       │   ├── errors.py
│       │   ├── factory.py
│       │   ├── jsonl.py
│       │   ├── layout.py
│       │   ├── local.py
│       │   ├── postgres_job_checkpoint.py
│       │   ├── redaction.py
│       │   ├── s3.py
│       │   └── s3_output.py
│       ├── retry/
│       │   └── parser_failures.py
│       ├── adapters/
│       │   ├── cli.py
│       │   ├── container.py
│       │   ├── mcp.py
│       │   └── safe_errors.py
│       └── dom.py
├── tests/
│   ├── fixtures/
│   ├── snapshots/
│   ├── strategies/
│   └── test_*.py
├── .env                   # 사용자 로컬 Proxy 설정 파일, 커밋 금지
├── outputs/               # 로컬 실행 결과, 커밋 금지
├── .crawl/                 # Scrapling crawler state, 커밋 금지
└── Checkpoint.json         # 애플리케이션 Job checkpoint, 커밋 금지
```

구현 주의:

- 실제 Python package는 `src/fmkorea_scraper` 아래에 있다.
- 루트 `main.py`는 `src` 경로를 추가한 뒤 CLI `main()`을 호출하는 호환 진입점이다.
- `.dockerignore`는 `.git`, 캐시, `.env`, 출력물, 문서, 테스트, CDK synth 결과를 Docker build context에서 제외한다.
- Docker image에는 `docs`, `tests`, `infra/cdk`, 로컬 `outputs`, `.crawl`, `Checkpoint.json`을 복사하지 않는다.
- `spider/`는 Scrapling 실행 엔진과 session/blocking 정책만 담당하고, Job 반복 실행·resume orchestration은 `application/`이 담당한다.

## 5. 실행 형태
- CLI 직접 실행
  - 명령형 실행: `python -m fmkorea_scraper.adapters.cli run ...`
  - DCinside-Scraper와 같은 루트 호환 실행: `python main.py ...`
  - 설치 후 console script 실행: `fmkorea-scraper ...`
  - 터미널 마법사 실행: `fmkorea-scraper wizard`
  - CLI는 `run`, `resume`, `retry-parser-failures`, `configure-proxy`, `wizard`를 제공한다.
  - `run --source`는 `direct-post`, `board-list`, `board-search`, `integrated-search`, `deep-search`, `no-keyword-range`, `realtime-keyword`, `realtime-all`을 지원한다.
  - 실시간 수집도 일반 수집과 동일한 `run --source`/`JobConfig.source` 계약을 사용하며 별도 enable 환경변수를 사용하지 않는다.
- 라이브러리 import
  - 설정 객체와 Job 객체를 코드에서 생성해 수집기를 직접 호출할 수 있게 구성
  - 공개 함수는 `search_board`, `search_integrated`, `fetch_post`, `fetch_comments`, `run_job`, `resume_job`, `retry_parser_failures`다.
  - import 시 스크래핑, 프롬프트, 파일 생성이 자동 실행되면 안 된다.
- Docker 워커 컨테이너
  - source/수집 옵션은 CLI argv 또는 동등한 `JobConfig`로 전달하고, 저장소/Proxy/runtime 설정만 환경변수로 주입한다. public source/CLI 계약은 §8.2.1–§8.2.10, 컨테이너 ENV 계약은 §8.2.11을 따른다.
  - JSONL/미디어는 S3 호환 저장소로 출력
  - 애플리케이션 Job checkpoint는 PostgreSQL에 저장한다.
  - Scrapling scheduler state를 정확히 resume하려면 최초 Job 실행부터 §8.2.11의 `FMKOREA_CRAWDIR`을 persistent volume root로 지정한다. worker는 그 root 아래 `crawl/<job_id>`와 `staging/<job_id>/<run_id>`를 함께 사용한다. `FMKOREA_CRAWDIR`을 제공하지 않는 일반 ephemeral worker는 PostgreSQL `JobConfig` + `completed_keys` + S3 committed segments 기반 reconstruction resume을 수행한다. Browser cookie/UA/adaptive runtime state는 어느 경우에도 새 process에서 재획득/재구성한다.
  - Dockerfile은 `python:3.11-slim` 기반으로 `.[s3,postgres]`와 기본 의존성 `scrapling[fetchers]`를 설치하고 `scrapling install`을 수행한다.
  - `docker-entrypoint.sh`는 `python -m fmkorea_scraper.adapters.container "$@"`로 실행을 위임한다.
  - 컨테이너는 장기 실행 worker daemon이 아니라 전달받은 Job 하나를 수행하고 종료하는 one-shot worker다. 단, 실시간 Job에서는 Job 종료 조건까지 하나의 장기 실행 Spider/SessionManager를 유지한 뒤 컨테이너가 종료한다.
  - RabbitMQ/SQS 메시지 소비는 컨테이너 내부가 아니라 향후 외부 오케스트레이터가 담당한다.
- MCP
  - 같은 core service를 MCP tool wrapper에서 호출한다.
  - `python -m fmkorea_scraper.adapters.mcp`로 MCP server를 실행한다.
  - MCP tool은 safe error payload만 반환하며 secret, cookie, raw header를 노출하지 않는다.

대표 실행 예시:

```powershell
# 직접 게시글 수집
fmkorea-scraper run --source direct-post --url "https://www.fmkorea.com/best/10195585295"

# 게시판 내 검색
fmkorea-scraper run --source board-search --board-id "stock" --keyword "삼전" --search-target "title_content" --page-start 1 --page-end 5

# 통합 검색
fmkorea-scraper run --source integrated-search --keyword "삼전" --search-target "title_content" --page-start 1 --page-end 10

# 실패 재처리
fmkorea-scraper retry-parser-failures --failure-path ".\outputs\job-a\run-1\failures.jsonl"

# 체크포인트 재개
fmkorea-scraper resume --job-id "job-a"

# 터미널 마법사
fmkorea-scraper wizard
```

컨테이너 실행 예시:

```powershell
docker run --rm `
  -e DATABASE_URL="postgresql://user:pass@postgres/fmkorea" `
  -e S3_ENDPOINT_URL="http://garage:3900" `
  -e S3_BUCKET="scraper" `
  -e S3_ACCESS_KEY_ID="access" `
  -e S3_SECRET_ACCESS_KEY="secret" `
  -e S3_PREFIX="fmkorea" `
  fmkorea-scraper:latest `
  run --source board-search --board-id "stock" --keyword "삼전"
```

## 6. 향후 확장 계획

### 6.1. 데이터베이스 확장
- PostgreSQL 기반 Job 이력/실패 요약 테이블 확장
- S3 호환 객체 저장소 기반 JSONL/미디어 보존 정책 확장
- 임베딩 처리 및 벡터 유사도 검색
- Polars를 활용한 데이터 분석 및 시각화 기능 추가

### 6.2. 검색 기능 확장
- 검색 옵션 추가
  - 특정 키워드 포함/제외
  - 정규식 검색
  - 작성자/말머리/추천수/댓글 수 조건 조합
- 검색 결과 필터링 및 정렬 기능 강화

### 6.3. 작업 예약 및 알림 시스템
- 텔레그램 봇 또는 통합 운영 UI를 활용한 작업 예약 및 알림 시스템 구축
- 작업 상태 알림 및 결과 전송 기능
- 터미널에서 봇 토큰 또는 webhook URL 입력

### 6.4. 통합 작업 관리 시스템
- 단일 스크래퍼의 AWS 배포는 `infra/cdk`의 CDK 구성으로 관리 가능
- CDK는 VPC, S3 output bucket, RDS PostgreSQL, ECS cluster, Fargate worker task, CloudWatch Logs/EventBridge 기반 실패 감지를 구성
- CDK는 중앙 Job Queue, 통합 운영 UI, Docker image build, EKS 배포를 포함하지 않음
- RabbitMQ 같은 Job Queue는 스크래퍼 내부가 아니라 외부 오케스트레이터가 소비
- Queue 메시지는 게시글 링크 단위가 아니라 Job 단위이며, 외부 오케스트레이터가 `JobConfig`를 worker 컨테이너 환경 변수 또는 실행 인자로 변환
- Worker 컨테이너는 전달받은 Job 하나를 실행하고 종료하며, Job 내부 request scheduling은 Scrapling Spider scheduler가 담당
- RabbitMQ broker, consumer, ack/nack, DLQ, lease, 중앙 retry/scheduler는 통합 플랫폼 단계에서 구현
- 향후 통합 플랫폼에서 마스터 컨테이너 - 워커 컨테이너 구조 기반으로 여러 커뮤니티 스크래퍼를 관리
- 작업 컨테이너 스케줄링 및 모니터링 기능
- 작업 상태 시각화 및 알림 기능
- 작업 실패 시 재시도 및 오류 로그 관리
- SQS 기반 작업 큐와 lease 관리는 향후 외부 orchestrator 책임으로 구현한다. `lease_expired`는 현재 schema에 future-reserved enum으로만 유지하며, 실제 lease 만료 시 외부 orchestrator adapter가 worker graceful stop을 요청하고 해당 reason을 기록하는 구조를 사용한다.

### 6.4.1. Scrapling Spider 기반 실행 엔진
- 크롤링 실행 엔진은 Scrapling `Spider`를 기준으로 구현한다.
- Spider는 request scheduling, callback routing, concurrency, deduplication, blocked retry, pause/resume을 담당한다. 일반 Job은 scheduler가 빌 때 종료되는 1회 crawl이고, 실시간 Job은 하나의 장기 실행 Spider가 polling callback을 통해 다음 poll request를 이어서 생성한다.
- HTTP와 Browser는 별도 프로세스가 아니라 같은 Spider 안의 multi-session으로 구성한다.
- Proxy가 설정된 경우 `ProxyRotator`가 고른 `proxy_id`에 대응하는 `manager.add("http:<proxy_id>", FetcherSession(proxy=<that_proxy>, ...), lazy=True)` 및 `manager.add("browser:<proxy_id>", AsyncStealthySession(proxy=<browser_transport_proxy>, ...), lazy=True)`를 사용한다. 인증된 SOCKS5의 Browser 전송 주소는 위의 loopback 중계 주소다. 첫 HTTP session을 manager의 default로 등록할 수 있지만 새 logical request의 `sid`는 항상 선택된 Proxy에 맞게 명시한다. Proxy가 없으면 direct `http`/`browser` session만 사용한다.
- `StealthyFetcher.fetch()` 같은 one-off 호출은 standalone 단발성 진단에는 사용할 수 있으나, Spider fallback 계약에서는 사용하지 않는다. 동일 Proxy의 Browser context/cookie/UA를 유지하기 위해 Proxy별 `AsyncStealthySession`을 lazy 재사용한다.
- `FMKoreaSpider.is_blocked()`의 status-code 기준은 Scrapling 기본 `BLOCKED_CODES={401,403,407,429,444,500,502,503,504}`를 그대로 따른다. FMKorea 전용 차단 body/content 패턴만 추가 판정하고, `404/410`은 삭제·비공개 결과로 별도 처리한다.
- 새 logical request는 `ProxyAttemptCoordinator`가 `ProxyRotator.get_proxy()`로 Proxy 하나를 선택한 HTTP session에서 시작한다. HTTP Blocking이면 `retry_blocked_request()`가 원 logical context/`proxy_attempt`/`proxy_id`를 유지하여 **동일 Proxy의 Browser session**으로 전환한다.
- Browser도 Blocking이면 잔여 blocked retry budget에서 **새 HTTP logical attempt**를 시작하며 rotator가 다음 Proxy를 선택한다. 기본 `max_blocked_retries=3`이면 `HTTP(P1) → Browser(P1) → HTTP(P2) → Browser(P2)`까지 시도한다. transport retry는 선택된 Proxy를 유지한다.
- Browser fallback에서는 `BrowserPageActionComposer`가 readiness 완료 후 `proxy_id`, cookie, UA, Chrome major를 `BrowserIdentityCandidate`로 캡처하고, `BrowserIdentityBridge`는 non-blocked이며 해당 Proxy slot에서 최신 `capture_seq`인 candidate만 `BrowserIdentityStore[proxy_id]`로 승격한다.
- Browser fallback 성공 후의 새로운 logical request도 rotator를 통한 HTTP 요청으로 시작한다. 전역 Browser sticky/http probe는 사용하지 않는다.
- `max_blocked_retries`는 HTTP/Browser 전환을 합산한 1 이상의 홀수 blocked retry Request budget이다. 마지막 Browser도 Blocking이면 `terminal_blocked` 후 `pause()`로 crawler state를 보존하고 runner는 `status="stopped"`, `reason="blocked"`를 저장한다.
- Scrapling `crawldir`는 pending request/seen set을 저장하되 rotator 순서와 Proxy별 cookie/UA/Browser context를 복원하지 않는다. exact resume에서 pending request의 `proxy_id` mapping은 배포 Proxy pool과 일치해야 한다.
- 각 `Response.meta["proxy_log"]`는 secret이 제거된 Proxy 이력과 phase/Blocking 판정을 포함한다. raw `Response.meta["proxy"]`의 credential은 영속 로그에 기록하지 않는다.
- 실시간 Job은 polling timer 자체가 scheduler pending request가 아니므로 **`FMKOREA_CRAWDIR`이 설정되어 있어도** process 재시작 시 Scrapling scheduler-state exact resume을 사용하지 않는다. `JobConfig` + `completed_keys` + durable S3 segment state + checkpoint의 `realtime_deadline_at` 기반 reconstruction resume을 사용한다. persistent state root가 있으면 staging 복구에는 활용할 수 있으며, 같은 process 안에서는 HTTP/Browser session과 Browser context를 polling 사이에 계속 유지한다.
- parser callback의 데이터 추출/validation 실패는 `failures.jsonl`의 parser-stage failure 및 parser retry 흐름과 연결한다. storage/control-plane failure는 parser failure로 변환하지 않고 terminal error 경로로 분리한다.
- CLI/Docker/MCP/Python public API는 Spider 구현 세부사항을 노출하지 않고 동일한 `JobConfig` 및 결과 계약을 사용한다.

### 6.5. 커뮤니티 통합 스크래퍼
- 통합 스크래핑 시스템의 에펨코리아용 워커 컨테이너로 스케줄링 및 스케일링
- 후속 커뮤니티별 스크래퍼를 추가할 때 각 커뮤니티의 원본 게시글 구조는 개별 JSON 스키마로 저장
- 개별 JSON 스키마는 수집 원본 보존을 우선하며, 커뮤니티별 특수 필드는 그대로 유지

### 6.6. 공통 분석 스키마 및 Polars 정규화
- 목적: 커뮤니티별 개별 JSON 스키마를 통합 커뮤니티 전략분석 플랫폼에서 함께 분석할 수 있는 Polars 가공용 공통 스키마로 변환
- 범위: 향후 개발 참고사항이며, 에펨코리아 원본 JSONL 저장 구조를 변경하지 않음
- 처리 위치
  - 스크래핑 단계: 커뮤니티별 개별 JSONL 저장
  - 정규화 단계: 개별 JSONL을 읽어 공통 분석 테이블로 변환
  - 분석 단계: Polars로 공통 테이블을 로드해 커뮤니티 간 비교/집계 수행
- 정규화 원칙
  - 원본 JSONL은 변경하지 않고 보존
  - 공통 스키마에 없는 커뮤니티별 특수 필드는 `extra` 또는 별도 확장 컬럼으로 보존
  - 커뮤니티에 없는 값은 `null`로 채움
  - 날짜, 숫자, 추천/비추천 수, 조회수, 작성자 식별자 등 분석 필드는 타입을 통일
- 공통 테이블 기본 구조
  - `posts`: 게시글 단위 테이블
  - `comments`: 댓글/덧글 단위 테이블
  - `media`: 이미지, 동영상, YouTube, X, 링크 등 본문 미디어 단위 테이블
- 댓글 정규화 규칙
  - 수집 JSON에서는 댓글을 게시글 하위에 중첩 저장할 수 있음
  - Polars 공통 스키마 정규화 시 게시글과 댓글을 별도 테이블로 분리
  - 일반 댓글은 `depth=0`, `parent_comment_id=null`
  - 덧글은 `depth=1`, `parent_comment_id=<상위 댓글 ID>`
  - BEST 댓글 여부와 추천/비추천 수는 `is_best`, `badges`, `reaction_up`, `reaction_down`, `reaction_score` 컬럼으로 보존
  - 댓글 정렬은 원본 배열 순서가 아니라 `created_at`, `comment_id`, `source_order` 중 사용 가능한 값을 기준으로 수행
- 정규화 출력 형식
  - `posts.jsonl` (향후 분석용 정규화 테이블의 이름이며 원본 scraper run의 `job_YYYYMMDD_HHMMSS.jsonl` 명명 규칙과 별개다.)
  - `comments.jsonl`
  - `media.jsonl`
  - 대용량 분석이 필요해지면 Parquet 출력 추가 검토

### 6.7. Parser-stage 게시글 실패 재처리 유닛
- 목적: 전체 검색/수집 Job을 다시 실행하지 않고, 기존 실패 기록에서 parser-stage 게시글 실패만 골라 direct-post 경로로 재수집
- 대상 실패
  - `failures.jsonl`의 `stage == "post"`
  - 제목/작성자/게시글 식별자/metadata/content/comment 파싱 실패 계열
- 제외 실패
  - 차단, 네트워크, 저장소, 체크포인트, 수동 중단, 취소 계열 실패
- 출력
  - 새 run에 `job_YYYYMMDD_HHMMSS.jsonl`, `failures.jsonl`, `report.json` 기록

## 7. 외부 통합 및 어댑터 구조
- MCP
  - 외부 에이전트가 게시판 검색, 통합 검색, 상세 게시글 수집, 실패 재처리를 호출하는 도구로 제공
  - MCP layer는 입력 검증과 권한/실행 제한만 담당하고 core 수집 로직은 재사용
- Python 라이브러리
  - 통합 플랫폼, 분석 파이프라인, 테스트 코드에서 직접 import해 사용할 수 있도록 공개 API를 유지
- 워커 컨테이너
  - 통합 플랫폼이 Job Queue를 소비한 뒤 실행시키는 one-shot Job 처리 단위
  - 컨테이너별 Proxy/속도 설정을 환경변수로 주입하고 HTTP browser identity는 Scrapling impersonation 정책에 맡긴다.
- KubeRay
  - 대규모 분산 수집/분석이 필요해질 때 검토
  - 초기 구현 범위에는 포함하지 않음

## 8. 복제 구현 부록

이 장은 기능 설명이 아니라 본 기능명세의 동작을 재구현하기 위한 세부 계약이다.

### 8.1. 파서별 상세 알고리즘

#### 8.1.1. 공통 DOM/URL 처리
- HTML 파싱은 Spider callback에 전달된 Scrapling `Response` 또는 그 하위 `Selector`를 그대로 사용한다. `Response.body`/HTML 문자열을 BeautifulSoup, lxml, 새 `Selector(...)` 인스턴스로 다시 파싱하지 않는다.
- Spider callback에서 **새로운 logical request**를 생성할 때는 공통 `follow_routed(response, url, callback, *, meta=None, priority=None)` helper를 사용한다.
  - `follow_routed()`는 URL join/referer/meta merge를 자체 재구현하는 범용 helper가 아니다. 새 child logical request의 초기 phase를 HTTP로 설정하고 `ProxyAttemptCoordinator`에서 rotator의 다음 Proxy를 선택한다.
  - 새 logical request는 이전 request의 Proxy/session identity를 상속할 수 없으므로 일반적으로 `response.urljoin(url)` 후 **clean `Request`**를 생성한다. Proxy 없이 동일 session이고 상속할 위험한 kwargs/meta도 없는 특수 경우만 Scrapling `response.follow()`을 사용할 수 있다.
  - Scrapling `response.follow()`은 원 request의 session kwargs를 상속하고 기존 `meta`를 child meta와 merge할 수 있으므로, fetcher kwargs뿐 아니라 transient application control meta까지 검사해야 한다. `page_action`, `page_setup`, `wait_selector`, `capture_xhr`, `solve_cloudflare`, request-local `extra_headers`, request-local `cookies`, request-local `impersonate`, identity용 request-local `headers`, per-request `proxy` 중 하나라도 있으면 same-session이라도 clean `Request`를 생성한다.
  - child request에 자동 상속하지 않는 transient meta에는 최소 `blocked_attempt`, `proxy_attempt`, `proxy_id`, `proxy_log`, terminal/error control flag, 현재 page 전용 `page_kind`, `required_selector`가 포함된다. 새 logical request의 `blocked_attempt=0`, `proxy_attempt=1`이며 선택된 Proxy에서 새 `proxy_log`를 시작한다. page-kind/readiness metadata도 child target에 맞게 명시적으로 다시 부여한다.
  - `follow_routed()`는 상속 가능한 application context를 **명시적 allowlist**로 다룬다. Job/source/board/candidate/post-key처럼 child에도 의미가 유지되는 context만 복사하고, allowlist 밖의 meta를 관성적으로 merge하지 않는다.
  - target session이 HTTP↔Browser로 바뀌거나 same-session request라도 ephemeral fetcher kwargs를 제거해야 하는 경우 clean `Request`를 직접 생성한다. 이때 `response.urljoin(url)`로 URL을 절대화하고 필요한 callback/priority/meta/referer만 명시적으로 옮기며 이전 request의 fetcher-specific kwargs는 자동 상속하지 않는다.
  - 새 logical request에서는 `ProxyRotator.get_proxy()`로 Proxy를 선택해 `sid="http:<proxy_id>"`로 요청을 만들고, Proxy 비활성화 시에는 `sid="http"`를 사용한다. Scrapling scheduler가 callback/scheduling/dedup을 유지하므로 추가 request queue를 만들지 않는다.
  - safe same-session `response.follow()`은 현재 request에 transient kwargs/meta가 없고 child에 전달할 context가 모두 allowlist-safe인 경우에만 사용한다. 그 외 clean `Request`에서는 sanitized meta를 새로 구성하고 callback/priority/referer를 명시한다.
  - HTTP request를 만들 때 **선택된 Proxy와 동일한 `BrowserIdentityStore[proxy_id]`가 있을 때만** 그 Proxy의 first-party cookie를 주입하고, exact Browser major impersonation profile이 지원되는 경우에만 그 Proxy의 Browser User-Agent overlay를 추가한다.
  - cross-session request의 referer가 필요하면 target session 형식에 맞춰 HTTP `headers` 또는 Browser `extra_headers`에 현재 `response.url`을 넣는다.
  - 이전 request의 Browser 전용 kwargs, request-local identity overlay, `sid`, `proxy_id`, `proxy_attempt`, `proxy_log`, `blocked_attempt`, terminal/error control meta를 새 logical child에 상속하지 않는다. 반대로 **동일 logical request의 blocked retry**에서는 proxy history/attempt를 보존해 이어간다.
  - `response.follow()`은 **safe same-session follow**에만 사용한다. cross-session 전환이나 ephemeral request-local fetcher option이 존재하는 경우에는 기존 request의 session kwargs 자동 병합을 피하기 위해 clean `Request`를 사용한다.
- `BrowserPageActionComposer`는 Browser request의 page-specific readiness를 먼저 만족시킨 뒤 같은 `page_action`의 마지막 단계에서 `page.context.cookies()`와 `navigator.userAgent`를 읽어 `BrowserIdentityCandidate`를 만든다. candidate capture 시 monotonic `capture_seq`를 발급한다.
- `BrowserIdentityBridge`는 `page_action`에서 active identity를 직접 바꾸지 않는다. Browser `Response`가 `is_blocked()==false`로 확정된 뒤 **같은 `proxy_id` slot에서** candidate의 `capture_seq`가 더 클 때만 lock 아래 `BrowserIdentityStore[proxy_id]`를 갱신한다. blocked/stale candidate는 폐기한다.
- `HttpIdentityOverlay`는 선택된 HTTP Proxy와 동일한 `proxy_id`에 보관된 FMKorea first-party cookie만 적용한다. 같은 slot의 `browser_chrome_major`와 일치하는 Scrapling/curl_cffi impersonation profile이 지원될 때만 exact `http_impersonation`과 Browser actual UA override를 함께 적용한다. exact profile이 없으면 same-proxy cookie만 동기화하고 Scrapling coherent Chrome profile을 유지한다.
- `ProxyAttemptCoordinator`는 Scrapling public `ProxyRotator.get_proxy()`와 명시적인 session id mapping을 사용하고 private `FetcherSession` internals는 사용하지 않는다. `Response.meta["proxy"]`는 실행 Proxy 확인용, `proxy_log`는 credential이 없는 진단 metadata로 취급한다.
- 별도 `user-agents.txt`에서 유사한 UA 문자열을 고르거나 Browser UA를 임의의 다른 impersonation profile과 조합하지 않는다.
- 목록/검색 callback이 상세 게시글 request를 만들기 전에는 `ScheduledPostKeyTracker.claim((site_id, board_id, document_id_raw))`를 호출한다. `false`이면 같은 run에서 이미 동일 logical post 상세 request가 예약된 것이므로 새 request를 yield하지 않는다. claim은 Scrapling scheduler의 URL/session fingerprint dedup을 대체하지 않고 그 위에 logical-post 수준의 중복 방지만 추가한다.
- claim은 detail request가 parser failure/post_unavailable/transport retry로 끝났다고 즉시 해제하지 않는다. 동일 run에서 같은 post를 여러 discovery path로 반복 요청하지 않는 것이 목적이며, transport/blocked retry는 원 logical request의 Scrapling retry 경로가 담당한다. process 재시작/reconstruction resume에서는 tracker가 비워지고 durable `completed_keys`만 복원되므로 미완료 게시글은 다시 발견·수집될 수 있다.
- URL 기준 base는 기본 `https://www.fmkorea.com`이다.
- `//example.com/path` 형태의 protocol-relative URL은 `https://example.com/path`로 정규화한다.
- 상대 URL은 현재 요청 URL 또는 기본 base URL을 기준으로 절대 URL로 변환한다.
- `document_srl` 추출은 query parameter `document_srl`을 먼저 본다.
- query에 `document_srl`이 없으면 path 또는 텍스트에서 3자리 이상 숫자 토큰을 찾아 글번호로 사용한다.
- `board_id` 추출은 query parameter `mid`를 먼저 본다.
- `mid`가 없으면 path 첫 세그먼트를 사용하되, path가 순수 숫자이면 게시판 ID로 보지 않는다.
- 후보 작성일은 다음 패턴을 순서대로 찾는다.

| 우선순위 | 포맷 | 예시 |
| --- | --- | --- |
| 1 | `%Y.%m.%d %H:%M` | `2026.08.11 12:30` |
| 2 | `%Y-%m-%d %H:%M` | `2026-08-11 12:30` |
| 3 | `%Y.%m.%d` | `2026.08.11` |
| 4 | `%Y-%m-%d` | `2026-08-11` |

#### 8.1.2. 게시판 목록/게시판 검색 후보 파서
- 필수 DOM selector는 `div.bd`다.
- 게시글/페이지 링크의 URL 후보 추출·allow/deny filtering·canonicalization에는 Scrapling `LinkExtractor`를 부분적으로 사용할 수 있다. 단, `LinkExtractor`가 목록 행의 제목·작성일·말머리·조회수 같은 구조화 metadata를 대신하지 않으므로 row selector 기반 parser는 유지한다.
- 후보 행 selector는 `div.bd table tbody tr`을 먼저 사용한다.
- 위 selector 결과가 없으면 fallback으로 `tbody tr`을 사용한다.
- 행의 class 문자열을 소문자로 만든 값에 `notice`, `ad`, `power` 중 하나가 포함되면 무시한다.
- 행 텍스트 소문자 값에 `공지`, `광고` 중 하나가 포함되면 무시한다.
- 후보 링크는 다음 순서로 찾는다.

| 우선순위 | selector |
| --- | --- |
| 1 | `a.hx[href]` |
| 2 | `td.title a[href]` |
| 3 | `a[href]` |

- 후보 링크에서 `document_srl`을 추출하지 못하면 해당 행은 `CollectorFailure(stage="board_row")`로 기록하고 다음 행을 처리한다.
- `PostCandidate`는 다음 값을 저장한다.

| 필드 | 값 |
| --- | --- |
| `board_id` | 링크 URL에서 추출한 `mid` 또는 호출자가 전달한 board hint |
| `document_srl` | 추출한 글번호 정수 |
| `document_id_raw` | 추출한 글번호 원문 문자열 |
| `url` | 절대 URL |
| `candidate_title` | 후보 링크 텍스트. 비어 있으면 `null` |
| `candidate_created_at` | 행 전체 텍스트에서 파싱한 후보 작성일 |
| `metadata.source` | `board_page` |
| `metadata.candidate_text` | 행 전체 텍스트 앞 1000자 |

- 다음 페이지 여부는 `a[href*='page=']` 링크의 query `page` 값 중 현재 page보다 큰 값이 하나라도 있으면 `has_next=true`로 판단한다.
- 목록/검색 단계의 후보 값은 필터링과 진단용이다. 최종 저장용 제목, 작성자, 작성일, 조회수, 추천수, 댓글 수, 말머리, 본문, 댓글은 상세 페이지에서 다시 파싱한다.

#### 8.1.3. 통합 검색 후보 파서
- 필수 DOM selector는 `.searchResult, .search_result`다.
- 통합 검색 item 내부 URL 후보 filtering에도 `LinkExtractor`를 사용할 수 있으나 `BoardCandidate`/`PostCandidate` metadata 구성은 item parser가 담당한다. 전체 Spider를 `CrawlSpider`로 바꾸지 않는다.
- 후보 item selector는 `.searchResult li, .search_result li, ul li`를 먼저 사용한다.
- 위 selector 결과가 없으면 fallback으로 `article, tr`을 사용한다.
- item 안의 모든 `a[href]`를 순회하며, `document_srl`을 추출할 수 있는 첫 링크를 게시글 링크로 사용한다.
- 후보 생성 중 예외가 발생하면 `CollectorFailure(stage="integrated_item")`로 기록한다.
- `PostCandidate`는 다음 값을 저장한다.

| 필드 | 값 |
| --- | --- |
| `board_id` | 링크 URL에서 추출한 board ID. 없으면 `null` |
| `document_srl` | 추출한 글번호 정수 |
| `document_id_raw` | 추출한 글번호 원문 문자열 |
| `url` | 절대 URL |
| `candidate_title` | 후보 링크 텍스트. 비어 있으면 `null` |
| `candidate_created_at` | item 전체 텍스트에서 파싱한 후보 작성일 |
| `metadata.source` | `integrated_search` |
| `metadata.candidate_text` | item 전체 텍스트 앞 1000자 |

- 통합 검색에서 board 후보는 item 내부 `a[href]` 중 board ID는 추출되지만 document ID는 추출되지 않는 첫 링크를 우선 사용한다.
- board 링크를 찾지 못하면 게시글 후보의 `board_id`를 fallback으로 사용한다.
- board 후보는 `BoardCandidate(board_id, board_name, board_url)`로 저장한다.
- 다음 페이지 여부는 게시판 목록/검색 파서와 같은 `page` query 비교 규칙을 사용한다.

#### 8.1.4. 상세 메타데이터 파서
- 기준 root는 `#bd_capture`다.
- `#bd_capture`가 없으면 전체 page를 root로 사용하고 `missing_detail_root` parse warning을 남긴다.
- canonical URL은 `link[rel="canonical"][href]`, `.document_address a.a[href]`를 먼저 사용한다.
- canonical 후보가 없으면 source URL을 사용한다.
- 상세 글번호는 canonical URL query `document_srl`을 먼저 본다.
- query에 없으면 canonical path의 `/(best/)?<digits>` 패턴을 사용한다.
- 그래도 없으면 후보/요청의 `document_id_hint`를 사용한다.
- `document_id_raw`, `canonical_url`, `title` 중 하나라도 비어 있으면 `ParseFailureError(reason="required_metadata")`를 발생시킨다.

| 항목 | selector/규칙 |
| --- | --- |
| 제목 | `.rd_hd h1 .np_18px_span` 우선, 없으면 `.rd_hd h1` |
| board_id | canonical query `mid`, 없으면 context `board_id_hint` |
| board_name | `.bd_name`, `.board_name`, `h1 a` 순서 |
| 작성자 | `.rd_hd .member_plate` |
| member_srl | 작성자 노드 class의 `member_<digits>` |
| level | 작성자 노드 하위 `img.level`의 `alt`, 없으면 `title` |
| profile_url | 작성자 노드 자체가 `a` 태그인 경우 `href` |
| created_at_raw | `.date`, `.regdate`, `.rd_hd .btm_area` 순서 |
| created_at | `YYYY.MM.DD HH:MM` 또는 `YYYY-MM-DD HH:MM` 정규식 |
| views | `조회 수` label이 포함된 span의 숫자 |
| reactions.up_count | `추천 수` label이 포함된 span의 숫자. 없으면 0 |
| comment_count | `댓글` label이 포함된 span의 숫자 |
| category | `a.category[href*="category="]` |

- counter 파싱은 `.rd_hd .btm_area .side.fr span`, `.btm_area span`, `span`을 순회하며 label이 포함된 첫 span에서 `[\d,]+` 숫자를 추출한다.
- category URL query `category` 값을 `category.category_id`로 저장한다.

#### 8.1.5. 첨부파일 파서
- 첨부파일 후보 selector는 `.rd_file a[data-href]`다.
- URL은 `data-href`를 우선 사용하고, 없으면 `href`를 사용한다.
- query parameter에서 `file_srl`, `sid`, `mid`를 추출한다.

| 필드 | 값 |
| --- | --- |
| `file_name` | 링크 텍스트. 없으면 `title` |
| `url` | 정규화된 다운로드 URL |
| `size_text` | `title` |
| `file_srl` | query `file_srl` |
| `sid` | query `sid` |
| `mid` | query `mid` |
| `download_text` | `title` |
| `file_path`, `object_key` | 다운로드 저장 후 media/output 단계에서 필요 시 반영 |

- 첨부파일은 `file_srl` 기준 dictionary로 만들어 image/link block과 매칭한다.

#### 8.1.6. 본문 콘텐츠 파서
- 기준 컨테이너는 `#bd_capture .rd_body article > .xe_content`를 먼저 사용한다.
- 없으면 `#bd_capture .rd_body .xe_content`를 사용한다.
- 둘 다 없으면 `missing_content_container` warning을 남기고 빈 `BodyContent`를 반환한다.
- 컨테이너 내부 `.//node()`를 DOM 순서대로 순회한다.
- block 하나를 저장할 때마다 `order`를 1 증가시킨다.
- `max_content_blocks`가 있고 현재 block 수가 제한값 이상이면 `max_content_blocks_exceeded` warning을 남기고 중단한다.
- node별 파싱 중 예외가 발생하면 `content_block_parse_failed` warning을 남기고 다음 node를 처리한다.

텍스트 노드 처리:

- parent가 없으면 무시한다.
- parent tag가 `script`, `style`, `video`, `source`, `iframe`이면 무시한다.
- ancestor tag에 `a`, `video`, `iframe`이 있으면 무시한다.
- 공백 정규화 후 빈 문자열이면 무시한다.
- 값이 `BeforeDocument`, `AfterDocument`, `__LAZY__`이면 무시한다.
- 값이 `serverLog:`로 시작하면 무시한다.
- 통과한 텍스트는 `TextBlock(order, type="text", text=<normalized>)`로 저장한다.

태그 노드 처리:

| tag | 처리 |
| --- | --- |
| `img` | `ImageBlock` 후보. `mejs__poster-img` class는 제외 |
| `video` | `FmkVideoBlock` 후보 |
| `iframe` | YouTube, X 순서로 전용 block을 시도하고 실패하면 `UnknownEmbedBlock(provider="iframe")` |
| `a` | highslide면 제외. `data-href`가 첨부파일과 매칭되면 `AttachmentBlock`. 그 외 YouTube, X 전용 block을 먼저 시도하고 실패하면 `LinkBlock` |
| 기타 tag | block 생성 없음 |

- media dedupe key는 `(media_type, file_srl 또는 url)`이다.
- image dedupe type은 `image`, fmk video dedupe type은 `fmk_video`, YouTube/X는 각 block의 `type`을 사용한다.
- dedupe key가 이미 있으면 같은 media block은 저장하지 않는다.
- `content.text`는 `TextBlock.text`와 `LinkBlock.text`만 줄바꿈으로 합쳐 만든다.

#### 8.1.7. 미디어 block 파서
- 이미지 파서
  - `img` class에 `mejs__poster-img`가 있으면 `null`을 반환한다.
  - 부모 ancestor 중 `a.highslide`를 찾고, 있으면 그 `href`를 `original_url`로 사용한다.
  - `url`은 `original_url`을 우선 사용하고, 없으면 `img[src]`를 사용한다.
  - `file_srl`은 `data-file-srl`, 없으면 `file_srl` attribute를 사용한다.
  - 같은 `file_srl`의 첨부파일이 있으면 `download_url`에 첨부파일 URL을 넣는다.
- 펨코 자체 영상 파서
  - `video` 하위 `source[src]`를 우선 사용하고, 없으면 `video[src]`를 사용한다.
  - URL이 없으면 `null`을 반환한다.
  - 설명은 video parent 하위 `.video_infobox .box div` 텍스트를 사용한다. 없거나 빈 값이면 `null`이다.
  - `poster_url`은 `video[poster]`를 사용한다.
- `download_url`은 `video_url`과 동일하게 둔다.
  - `name`, `upload_date`, `width`, `height`는 video parent 하위 `meta[itemprop="<name>"][content]`에서 읽는다.
- YouTube 파서
  - URL 정규화 후 `youtube.com/watch?v=`, `youtube.com/embed/`, `youtu.be/` 패턴에서 video ID를 추출한다.
  - `embed_url`은 `https://www.youtube.com/embed/<video_id>`로 생성한다.
  - query `t` 또는 `start`가 정수이면 `start_seconds`로 저장한다.
- X 파서
  - URL 정규화 후 `x.com/<user>/status/<digits>` 또는 `twitter.com/<user>/status/<digits>` 패턴을 찾는다.
  - `username`, `status_id`, `tweet_id`를 저장한다.
  - `embed_url`은 `https://platform.twitter.com/embed/Tweet.html?id=<tweet_id>`로 생성한다.

#### 8.1.8. 댓글 파서
- 기준 selector는 `.fdb_lst_ul > li`다.
- 각 댓글 item은 DOM 등장 순서대로 `source_order`를 받는다.
- `max_comments`가 있고 현재 저장 댓글 수가 제한값 이상이면 `max_comments_exceeded` warning을 남기고 중단한다.
- 댓글 1개 파싱 중 예외가 발생하면 `comment_parse_failed` warning을 남기고 다음 댓글을 처리한다.

| 항목 | selector/규칙 |
| --- | --- |
| `comment_srl` | `li[id]`에서 `comment_<digits>` 정규식 |
| `comment_id_raw` | `comment_srl` 문자열. 없으면 `null` |
| `source_dom_id` | `li[id]` 원문 |
| BEST 여부 | item class에 `comment_best`가 있거나 `.icon-hit` 텍스트가 정확히 `BEST` |
| `badges` | BEST이면 `["BEST"]`, 아니면 빈 배열 |
| parent | `data-parent`, 없으면 `data-parent-comment-srl` |
| depth | parent가 있거나 item class에 `reply`가 있으면 1, 아니면 0 |
| 작성자 | `.meta .member_plate`, 없으면 `.member_plate` |
| 작성자 member_srl | 작성자 노드 class의 `member_<digits>` |
| 날짜 원문 | `.meta .date`, 없으면 `.date` |
| 날짜 정규화 | `YYYY.MM.DD HH:MM` 또는 `YYYY-MM-DD HH:MM` 정규식 |
| 본문 | `.xe_content`, `.comment_content`, `.comment-content` 순서 |
| 추천 | `a[title="추천"] .voted_count`, 없으면 `.voted_count` |
| 비추천 | `a[title="비추천"] .blamed_count`, 없으면 `.blamed_count` |

- 댓글 본문은 본문 노드 하위 text node를 공백으로 합친다.
- ancestor class에 `imagecon-buy-icon`이 있는 text node는 제외한다.
- 본문 안의 `img[alt]` 값은 댓글 본문 뒤에 함께 보존한다.
- 추천/비추천 숫자가 없거나 파싱되지 않으면 0으로 저장한다.
- `ReactionInfo.score`는 `up_count - down_count`로 계산된다.

#### 8.1.9. 추가 댓글 페이지 파서
- 상세 HTML에 포함된 댓글은 항상 먼저 파싱한다.
- `comment_mode="none"`이면 댓글은 빈 배열이다.
- `max_comment_pages`가 `null`이면 추가 댓글 페이지 요청을 하지 않는다.
- `max_comment_pages`가 있으면 다음 selector로 댓글 페이지 링크를 찾는다.

```css
.fdb_nav.comment_page a[href],
.comment_page a[href],
.comment-page a[href]
```

- `href`가 `#`로 시작하면 제외한다.
- `href`, link class, link text를 합친 소문자 문자열에 `cpage`, `comment_page`, `commentpage` 중 하나가 포함되어야 댓글 페이지 링크로 본다.
- fragment를 제거한 URL이 현재 상세 URL과 같으면 제외한다.
- 같은 URL은 한 번만 요청한다.
- URL의 `document_srl` 또는 path 숫자가 현재 게시글과 다르면 제외한다.
- 추가 댓글 파싱 시 `source_order`는 상세 HTML 댓글 개수 이후부터 이어서 부여한다.
- 기존 댓글과 추가 페이지 댓글은 `comment_srl`이 있으면 `comment_srl`, 없으면 `source_dom_id` 기준으로 중복 제거한다.
- 중복 제거 후 `comment_mode="best_only"`이면 BEST 댓글만 남긴다.

### 8.2. CLI/env 전체 옵션 표

- **본 §8.2는 CLI 옵션과 public 환경변수에 대한 유일한 normative source of truth다.** 다른 장의 옵션/환경변수 언급은 기능 설명 또는 사용 예시이며, 이름·타입·기본값·필수 여부·validation·상호 배타 조건이 충돌할 경우 §8.2의 정의를 따른다.
- CLI 계약은 §8.2.1–§8.2.10, 컨테이너/runtime 환경변수 계약은 §8.2.11이 담당한다. 실시간 수집도 CLI/`JobConfig.source` 계약에 포함한다.
- 새 public CLI 옵션이나 환경변수를 추가·변경·삭제할 때는 먼저 해당 §8.2 표와 validation 규칙을 수정하고, 다른 장에서는 필요한 기능 의미만 설명한 뒤 해당 절을 참조한다. 같은 public configuration 계약을 여러 장에 복제하지 않는다.

#### 8.2.1. CLI 진입점 및 종료 코드

| 항목 | 값 |
| --- | --- |
| 모듈 실행 | `python -m fmkorea_scraper.adapters.cli ...` |
| 루트 호환 실행 | `python main.py ...` |
| console script | `fmkorea-scraper ...` |
| 성공 종료 코드 | `0` |
| 런타임 오류 종료 코드 | `1` |
| 입력 검증 오류 종료 코드 | `2` |
| 전역 debug 옵션 | `--debug` |

#### 8.2.2. CLI subcommand

| subcommand | 용도 |
| --- | --- |
| `run` | 새 scrape Job 실행. 일반 source와 장기 실행 realtime source를 모두 포함 |
| `resume` | 로컬 checkpoint에서 재개 |
| `retry-parser-failures` | parser-stage 실패 게시글만 재처리 |
| `configure-proxy` | Proxy 값을 `./.env`에 평문 저장 |
| `wizard` | 번호 선택식 터미널 마법사 실행 |

#### 8.2.3. `run` 옵션

| 옵션 | 타입/값 | 기본값 | 적용 source | 설명 |
| --- | --- | --- | --- | --- |
| `--source` | choice | 없음 | 전체 | `direct-post`, `board-list`, `board-search`, `integrated-search`, `deep-search`, `no-keyword-range`, `realtime-keyword`, `realtime-all` |
| `--job-id` | string | 자동 생성 | 전체 | Job ID |
| `--url` | repeatable string | 없음 | `direct-post` | 수집할 게시글 URL. 1개 이상 필요 |
| `--board-id` | string | 없음 | `board-list`, `board-search`, `deep-search`, `no-keyword-range`, `realtime-keyword(scope=board)`, `realtime-all` | 게시판 ID |
| `--category-id` | string | `null` | `board-list`, `board-search`, `no-keyword-range`, `realtime-all` | 말머리/category ID |
| `--keyword` | string | 없음 | `board-search`, `integrated-search`, `deep-search`, `realtime-keyword` | 검색어 |
| `--search-target` | string | `title_content` | 검색 source 및 `realtime-keyword` | 사이트 검색 옵션 |
| `--page-start` | int | `1` | `board-list`, `board-search`, `integrated-search` | 시작 페이지 |
| `--page-end` | int | `null` | `board-list`, `board-search`, `integrated-search` | 종료 페이지. 없으면 `has_next=false`까지 |
| `--seed-page-limit` | int | `20` | `deep-search` | 통합 검색 seed 페이지 상한 |
| `--board-candidate` | repeatable string | `null` | `deep-search` | seed 검색에서 찾은 게시판 외에 추가할 게시판 ID |

- `realtime-keyword`, `realtime-all`의 추가 옵션과 validation은 §8.2.10을 따른다.

#### 8.2.4. 범위 옵션

| 옵션 | 타입 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `--date-from` | `YYYY-MM-DD` | `null` | 후보 작성일 하한 |
| `--date-to` | `YYYY-MM-DD` | `null` | 후보 작성일 상한 |
| `--min-document-srl` | int | `null` | 글번호 하한 |
| `--max-document-srl` | int | `null` | 글번호 상한 |
| `--min-post-number` | int | `null` | `--min-document-srl` alias |
| `--max-post-number` | int | `null` | `--max-document-srl` alias |

- `date_from > date_to`이면 검증 실패다.
- `min_document_srl > max_document_srl`이면 검증 실패다.
- `deep-search`, `no-keyword-range`는 기간 또는 글번호 범위 중 하나 이상이 반드시 필요하다.

#### 8.2.5. 수집/파서/Spider 옵션

| 옵션 | 타입/값 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `--output-path` | path string | 현재 작업 디렉토리 기준 | 로컬 출력 및 로컬 checkpoint 기준 경로 |
| `--comment-mode` | `none`, `all`, `best_only` | `all` | 댓글 수집 모드 |
| `--media-mode` | `url_only`, `download` | `url_only` | 미디어 URL만 저장 또는 파일 다운로드 |
| `--include-attachments` | flag | `true` | 첨부파일 포함 |
| `--no-attachments` | flag | 없음 | 첨부파일 제외 |
| `--download-delay` | float | `1.0` | Scrapling Spider 요청의 최소 지연 시간(초) |
| `--concurrent-requests` | int | `4` | Spider 전체 최대 in-flight request 수 |
| `--concurrent-requests-per-domain` | int | `2` | 도메인별 최대 in-flight request 수 |
| `--max-blocked-retries` | odd int | `3` | HTTP↔Browser logical-attempt blocked retry Request 상한. `3`이면 `HTTP(P1) → Browser(P1) → HTTP(P2) → Browser(P2)` |
| `--autothrottle` | flag | 비활성 | Scrapling AutoThrottle 활성화 |
| `--no-autothrottle` | flag | 기본 | AutoThrottle 비활성화 |
| `--autothrottle-start-delay` | float | `2.0` | AutoThrottle 시작 지연 시간(초) |
| `--autothrottle-max-delay` | float | `30.0` | AutoThrottle 최대 지연 시간(초) |
| `--browser-max-pages` | int | `2` | `AsyncStealthySession`이 유지할 최대 Browser page 수 |
| `--media-download-concurrency` | int | `2` | `media_mode=download`일 때 post-crawl `MediaEnrichmentPipeline`의 binary 다운로드 동시성 제한 |
| `--checkpoint-interval-seconds` | float | `300.0` | Scrapling `crawldir` crawler state 주기 저장 간격 |
| `--robots-txt-obey` | flag | 비활성 | Scrapling robots.txt 준수 기능 활성화 |
| `--http-timeout-seconds` | float | `30.0` | `FetcherSession` timeout(초) |
| `--http-transport-retries` | int | `3` | HTTP transport exception/session-level 재시도 수 |
| `--browser-timeout-ms` | int | `30000` | Browser 동작/wait timeout(밀리초) |
| `--browser-transport-retries` | int | `3` | Browser transport/session-level 재시도 수 |
| `--max-comment-pages` | int | `null` | 추가 댓글 페이지 요청 상한. `null`이면 상세 HTML 댓글만 |
| `--max-comments` | int | `null` | 댓글 저장 개수 상한 |
| `--max-content-blocks` | int | `null` | 본문 block 저장 개수 상한 |

- `concurrent_requests`, `concurrent_requests_per_domain`, `browser_max_pages`, `media_download_concurrency`는 1 이상의 정수여야 한다. 단 realtime source는 polling callback이 active task 하나를 유지하므로 `concurrent_requests >= 2`여야 한다.
- `max_blocked_retries`는 1 이상의 **홀수 정수**여야 한다. `0`은 필수 Browser fallback을 비활성화하고 짝수는 마지막 retry가 HTTP에서 끝나 logical-attempt pair를 완성하지 못하므로 거부한다.
- `download_delay`, `checkpoint_interval_seconds`, timeout 값은 0보다 커야 한다.
- Blocking response 재시도와 transport exception 재시도는 서로 다른 계층이다. `--max-blocked-retries`를 session transport retry와 합산하지 않는다.
- ProxyRotator 기반 logical-attempt routing과 동일 Proxy 내 Browser identity → HTTP 동기화는 고정 실행 정책이며 별도 enable/disable CLI 옵션을 두지 않는다. 전역 sticky 설정은 제공하지 않는다.

#### 8.2.6. Proxy CLI 옵션

| 옵션 | 타입/값 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `--proxy-host` | repeatable string | `.env` fallback | Proxy host 또는 URL. 여러 번 입력하면 순서대로 하나의 Proxy pool 구성 |
| `--proxy-id` | string | `.env` fallback | Proxy ID |
| `--proxy-pw` | string | `.env` fallback | Proxy password |

- CLI에서 `--proxy-host`를 하나 이상 명시하면 `.env`보다 우선하며 입력 순서대로 Proxy pool을 구성한다.
- `--proxy-host`가 없고 실행 위치에 `.env`가 있으면 `.env`의 Proxy 값을 자동 사용한다.
- 시스템 환경변수의 `HTTP_PROXY`, `HTTPS_PROXY`는 자동 사용하지 않는다.
- 기존 `FMKOREA_PROXY_HOST`만 확장하며 새 환경변수를 추가하지 않는다. 단일 URL은 그대로 지원하고 여러 Proxy는 `;`로 구분한다. 예: `socks5://p1:1080;socks5://p2:1080`. SOCKS5 전용 서비스는 `socks5://`를 명시해야 한다. scheme을 생략한 host는 기존 호환성을 위해 `http://`로 해석한다. 빈 항목/중복 Proxy는 입력 validation에서 거부한다.
- `--proxy-id` 및 `FMKOREA_PROXY_ID`는 **인증용 username**으로서 내부의 safe `proxy_id=proxy-0001`과 다른 개념이다. `--proxy-pw`/`FMKOREA_PROXY_PW`는 인증정보가 없는 URL에만 적용되는 shared password다. Proxy별 credentials가 다르면 해당 URL에 포함해 제공하되 URL 자체를 로그에 남기지 않는다.
- Proxy pool의 항목/순서는 한 exact-resume Job의 pending request `proxy_id` mapping과 일관되게 유지한다. Proxy pool이 비활성화되어 있으면 direct HTTP/Browser만 사용한다. Pool이 하나 이상이면 Scrapling `ProxyRotator`를 생성한다.

#### 8.2.7. `resume` 옵션

| 옵션 | 타입/값 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `--job-id` | string | 최신 로컬 checkpoint | 재개할 Job ID |
| `--output-path` | path string | 현재 작업 디렉토리 기준 | `Checkpoint.json` 탐색 기준 |
| runtime 공통 옵션 | 위 표 참조 | 각 기본값 | 재개 실행 시 새 `SpiderRuntimeConfig`로 조립 |

- 로컬 `resume`은 `<base_output_path>/Checkpoint.json`을 사용한다.
- resume는 새 crawl run을 즉시 시작하기 전에 이전 `active_run_id`의 `staging_ref`와 `enrichment_status`를 먼저 검사한다. 복구 가능한 pending/running/failed enrichment 또는 미완료 finalization이 있으면 해당 run을 먼저 복구한다.
- 로컬 persistent 실행에서는 같은 Job의 `<base_output_path>/.crawl/<job_id>`과 대응 staging이 존재하면 scheduler-state exact resume을 사용할 수 있다. 컨테이너에서는 `FMKOREA_CRAWDIR`이 지정된 경우에만 `<state_root>/crawl/<job_id>`과 `<state_root>/staging/<job_id>/...`을 exact-resume state로 사용한다.
- resume process는 새 `ProxyRotator`와 빈 `BrowserIdentityStore`로 시작한다. exact-resume pending request는 동일 Proxy mapping을 사용하되 이전 cookie/UA/context는 재사용하지 않고 Browser fallback 성공 후 새로 획득한다.
- exact-resume state가 없거나 컨테이너에서 `FMKOREA_CRAWDIR`이 지정되지 않았으면 `JobConfig`, `completed_keys`, durable S3 segment state를 사용해 새 Spider를 재구성하는 reconstruction resume으로 동작한다.
- realtime Job은 위 exact-resume 조건의 예외다. `FMKOREA_CRAWDIR`이 존재해도 scheduler pending queue/seen set을 continuation source로 사용하지 않고 항상 reconstruction resume을 수행한다. persistent root의 staging은 이전 run 복구에 사용할 수 있다.
- 이전 active run의 staging/finalization 복구가 끝난 뒤에는 기존 최종 JSONL에 이어 쓰지 않고 새 run을 만든다.
- checkpoint의 `completed_keys`를 사용해 이미 완료된 게시글을 중복 저장하지 않는다.
- realtime Job은 checkpoint의 `realtime_deadline_at`을 그대로 재사용한다. resume 시점이 이미 deadline 이후라면 새 realtime poll을 시작하지 않고 이전 staging/finalization/enrichment 처리만 수행한다.

#### 8.2.8. `retry-parser-failures` 옵션

| 옵션 | 타입/값 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `--failure-path` | path 또는 S3 object key | 필수 | 읽을 `failures.jsonl` |
| `--existing-success-path` | path | `null` | 이미 성공한 `job_YYYYMMDD_HHMMSS.jsonl` 경로 |
| `--from-report` | path | `null` | report에서 재처리 입력 복원 |
| `--from-checkpoint` | path | `null` | checkpoint에서 재처리 입력 복원 |
| `--retry-job-id` | string | 자동 | 재처리 Job ID |
| `--job-id` | string | `null` | 특정 원본 Job ID 실패만 필터 |
| `--stage` | repeatable string | `post` | 재처리 대상 stage allowlist |
| `--skip-existing-success` | flag | `true` | 기존 성공 결과 제외 |
| `--no-skip-existing-success` | flag | 없음 | 기존 성공 결과 제외 비활성화 |

- `--from-report`와 `--from-checkpoint`는 동시에 사용할 수 없다.
- 기본 stage allowlist는 `["post"]`다.
- 재처리 Job은 `ParserFailureRetrySource`에서 direct-post source로 변환되어 실행된다.

#### 8.2.9. `configure-proxy`, `wizard`

| 명령 | 옵션/입력 | 동작 |
| --- | --- | --- |
| `configure-proxy --host ... --id --password` | `--host` 반복 지정 또는 prompt | Proxy URL 목록을 `;`로 직렬화한 `FMKOREA_PROXY_HOST`와 shared `FMKOREA_PROXY_ID`, `FMKOREA_PROXY_PW`를 `./.env`에 저장 |
| `wizard` | 번호 입력 | run, resume, retry, Proxy configure를 대화형으로 선택 |

- `configure-proxy`는 기존 `.env`의 다른 줄은 보존하고 Proxy 관련 key만 갱신한다.
- `wizard`의 run source 선택지는 `direct_post`, `board_list`, `board_search`, `integrated_search`, `deep_search`, `no_keyword_range`, `realtime_keyword`, `realtime_all`로 내부 정규화된다.
- `wizard`의 realtime source 선택 시에도 §8.2.10과 동일한 source option/validation을 적용한다.

#### 8.2.10. 실시간 source 옵션

- 실시간 수집은 별도 enable 환경변수를 사용하지 않고 `run --source realtime-keyword` 또는 `run --source realtime-all`로 실행한다. Docker/container에서도 동일한 CLI argv 또는 동등한 `JobConfig.source`를 사용한다.

| 옵션 | 타입/값 | 기본값 | 적용 source | 설명 |
| --- | --- | --- | --- | --- |
| `--scope` | `board`, `integrated` | 없음 | `realtime-keyword` | 키워드 검색 범위 |
| `--poll-interval-seconds` | float | `30.0` | realtime 전체 | 다음 poll까지 대기 시간 |
| `--duration-seconds` | int | `null` | realtime 전체 | Job 최초 시작 기준 실행 시간. 최초 시작 시 절대 deadline으로 정규화 |
| `--end-at` | ISO-8601 datetime | `null` | realtime 전체 | 절대 종료 시각 |

- `realtime-keyword`는 `--keyword`와 `--scope`가 필수다.
- `realtime-keyword --scope board`이면 `--board-id`가 필수다.
- `realtime-keyword --scope integrated`이면 `--board-id`, `--category-id`를 사용하지 않는다.
- `realtime-keyword`의 `--search-target` 기본값은 `title_content`다.
- `realtime-all`은 `--board-id`가 필수이며 `--category-id`는 선택이다.
- `--duration-seconds`와 `--end-at`는 정확히 하나만 지정해야 한다. 둘 다 없거나 둘 다 있으면 `invalid_realtime_termination` 검증 실패다.
- `--poll-interval-seconds`는 0보다 커야 하고 `--duration-seconds`는 지정 시 1 이상의 정수여야 한다.
- 실시간 모드에서는 `RealtimeCoordinator`가 Job당 하나의 장기 실행 Spider를 시작하고, 전용 realtime polling callback이 `stop_event`와 poll timeout을 함께 기다리는 interruptible wait를 수행한 뒤 조건이 허용할 때만 다음 poll `Request`를 `dont_filter=True`로 yield한다. polling 사이에도 같은 SessionManager/HTTP session/Browser context를 유지한다.
- 최초 realtime Job 시작 시 `--duration-seconds` 또는 `--end-at`을 `realtime_deadline_at`으로 정규화하고 checkpoint에 저장한다. resume에서는 기존 deadline을 재사용한다.
- realtime resume은 `FMKOREA_CRAWDIR` 유무와 무관하게 reconstruction 방식이며, existing persistent staging이 있으면 먼저 복구/finalize한 뒤 checkpoint의 기존 `realtime_deadline_at`을 사용해 필요한 경우 새 realtime Spider를 시작한다.
- realtime polling callback이 active task 하나를 유지하므로 `concurrent_requests >= 2`여야 하며, polling wait 동안 실제 crawl request에 사용할 수 있는 global slot은 최대 `concurrent_requests - 1`개다.

#### 8.2.11. 컨테이너 환경변수

- 이 절은 컨테이너/storage/runtime 환경변수의 **유일한 normative 정의**다.
- 실시간 컨테이너 Job의 source/종료 조건은 §8.2.10의 CLI argv 또는 동등한 `JobConfig.source`로 전달한다. 본 절은 storage/runtime 환경변수만 정의한다.

| 환경변수 | 필수 여부 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `DATABASE_URL` | 필수 | 없음 | PostgreSQL 애플리케이션 Job checkpoint DSN |
| `S3_ENDPOINT_URL` | 필수 | 없음 | S3 호환 저장소 endpoint |
| `S3_BUCKET` | 필수 | 없음 | 출력 bucket |
| `S3_ACCESS_KEY_ID` | 필수 | 없음 | S3 access key |
| `S3_SECRET_ACCESS_KEY` | 필수 | 없음 | S3 secret key |
| `S3_REGION` | 선택 | `null` | S3 region |
| `S3_PREFIX` | 선택 | `null` | 출력 object key prefix. 없으면 layout 기본값 사용 |
| `S3_FORCE_PATH_STYLE` | 선택 | `false` | true 계열이면 boto3 path-style addressing 사용 |
| `RESUME` | 선택 | `false` | true 계열이면 argv 대신 PostgreSQL 애플리케이션 Job checkpoint를 기준으로 재개 |
| `JOB_ID` | 선택 | `null` | resume 대상 Job ID |
| `FMKOREA_CRAWDIR` | 선택 | `null` | persistent state root. 일반(non-realtime) Job은 지정 시 `<root>/crawl/<job_id>` crawler state + `<root>/staging/<job_id>/<run_id>` staging을 사용해 exact resume 가능. realtime Job은 staging persistence에만 활용하고 resume 자체는 항상 reconstruction |
| `FMKOREA_PROXY_HOST` | 선택 | `null` | Proxy host/URL 또는 `;`로 구분한 순서 있는 Proxy pool |
| `FMKOREA_PROXY_ID` | 선택 | `null` | URL에 개별 인증정보가 없을 때 공통 적용할 Proxy username |
| `FMKOREA_PROXY_PW` | 선택 | `null` | URL에 개별 인증정보가 없을 때 공통 적용할 Proxy password |

- 컨테이너는 `run`, `retry-parser-failures`, `RESUME=true` 흐름만 지원한다.
- 컨테이너의 출력 저장소는 항상 S3 호환 저장소다.
- 컨테이너의 애플리케이션 Job checkpoint 저장소는 항상 PostgreSQL이다.
- `FMKOREA_CRAWDIR`은 별도 resume-mode 환경변수를 추가하지 않고 일반(non-realtime) 배포의 resume 방식을 결정하는 기존 설정으로 사용한다. **일반 Job은 미지정이면 reconstruction resume, 지정이면 scheduler-state exact resume**이다. realtime Job은 명시적 예외로 항상 reconstruction resume이다.
- `FMKOREA_CRAWDIR`을 지정한 exact-resume 배포는 최초 Job 실행부터 같은 persistent volume root를 사용해야 한다. worker는 crawler state와 S3 미업로드 staging을 반드시 그 root 아래에 함께 배치한다.
- `FMKOREA_CRAWDIR`이 없으면 컨테이너 재개는 PostgreSQL `JobConfig` + `completed_keys` + S3 committed segments 기반 reconstruction resume이며, Scrapling pending queue/seen set의 정확 복원으로 간주하지 않는다.
- `FMKOREA_CRAWDIR`이 지정되었지만 root가 생성/쓰기 불가능하면 startup validation error로 종료한다. 애플리케이션은 해당 경로가 실제 EFS/NFS/PVC 등 persistent volume인지 자동 판별하지 않으며 이는 배포 구성의 책임이다.
- boolean 환경변수 true 값은 `1`, `true`, `yes`, `on`이다.
- boolean 환경변수 false 값은 `0`, `false`, `no`, `off`이다.
- 컨테이너 실행 결과는 stdout/stderr에 JSON 한 줄로 출력한다.
- 성공 시 `{"ok": true, "report": ...}` 형식이다.
- 실패 시 `{"ok": false, "error": <safe_error_payload>}` 형식이다.
