# DANH SÁCH CÔNG VIỆC - K4-Track02-Day17-DataPipelineEngineering

## Quy tắc cần nhớ

- [ ] Không sửa `scripts/verify.py`, `tests/`, `data/`, hoặc logic tính checksum
- [ ] Verify sẽ fail trên bản seed gốc — đây là dự kiến của đề bài
- [ ] Chỉ sửa mã nguồn trong `pipeline/`
- [ ] Chạy tất cả các lệnh từ thư mục gốc repo
- [ ] Bản seed có 3 lỗi cố tình; bạn phải tìm và sửa cả ba

---

## CP1 — Đọc đề và dựng baseline (20 phút)

Mục tiêu: hiểu bài toán, thiết lập môi trường, chạy pipeline bản chưa sửa, ghi lại triệu chứng.

**Lệnh chạy:**
- [x] `make setup` (tạo .venv, cài dependencies)
- [x] `make run` (reset Silver/Gold, backfill mọi ngày từ Bronze)
- [x] `make verify` — ghi lại các check FAIL
- [x] `make test` — ghi lại các test fail (có thể còn fail)
- [x] `make lateness` — ghi P50, P95, P99 độ trễ event từ Bronze

**Nơi cần sửa:**
- Không sửa ở CP1

**Tiêu chí tự kiểm tra:**
- Đọc hiểu ba nguồn dữ liệu: CDC Debezium, Kafka events, transcripts
- Ghi nhận ví dụ: T-91 (thay đổi trạng thái qua các ngày), T-97 (bị xoá), u05 (event đến muộn 3 ngày)
- P99 lateness đo được (cần giá trị này cho CP3)
- Verify có fail (điều này là bình thường)

**Ghi vào REPORT.md:**
- Danh sách các check fail từ `make verify`
- Các test fail (tên test)
- Giá trị P50, P95, P99 lateness

---

## CP2 — Sửa khoá Silver (25 phút)

Mục tiêu: mỗi ticket_id chỉ có một hàng, batch cũ không ghi đè trạng thái mới nhất.

**Lệnh chạy:**
- [ ] `make test` — kiểm tra test liên quan ticket

**Nơi cần sửa:**
- **Tệp:** `pipeline/silver.py`
- **Vấn đề:** Cách ghi ticket không dùng khoá; không dedup giữa các batch
- **Cách sửa:** UPSERT/MERGE với `unique_key = ticket_id`, dùng LSN để quyết định cập nhật
  - LSN cao hơn = thay đổi mới nhất, ghi đè thay đổi cũ
  - LSN giống = giữ giá trị hiện tại (idempotent)

**Tiêu chí tự kiểm tra:**
- `make test` — các test ticket pass
- T-91 trạng thái cuối: `high` / `closed` / `bug`
- `make verify` — số lượng fail giảm (không phải tất cả pass)

**Ghi vào REPORT.md:**
- **Triệu chứng:** silver_tickets có nhiều hàng cho một ticket_id, trạng thái không nhất quán
- **Nguyên nhân:** Không dùng unique_key, không dedup giữa batch
- **Cách sửa:** MERGE với LSN guard: cập nhật chỉ khi `before_lsn < after_lsn`
- **Khái niệm:** SCD2, idempotent write, dedup, LSN (log sequence number)

---

## CP3 — Sửa dữ liệu đến muộn (25 phút)

Mục tiêu: event đến muộn so event_time phải được xử lý (u05 trễ 3 ngày).

**Lệnh chạy:**
- [ ] `make lateness` — lấy giá trị P99 từ Bronze (CP1)
- [ ] `make test` — kiểm tra feature
- [ ] `make verify` — kiểm tra các check liên quan

**Nơi cần sửa:**
- **Tệp:** `pipeline/gold.py`
- **Vấn đề:** Lookback không đủ, không xử lý event đến muộn
- **Cách sửa:**
  - Đo P99 lateness từ Bronze (từ CP1)
  - Đặt `lookback = P99` (số ngày event có thể đến muộn)
  - Tính feature theo `event_time`, không phải `_ingested_at`
  - Khi batch chạy, recompute các partition cũ để nhận event đến muộn

**Tiêu chí tự kiểm tra:**
- `make lateness` in P99 (ví dụ: 3 ngày, 72 giờ)
- u05 ngày 2026-08-12: có 5 events, 3 clicks, 1 feedback down
- `make verify` — feature check pass

**Ghi vào REPORT.md:**
- Giá trị P99 lateness đo được
- Giá trị lookback đã chọn
- Lý do: recompute partition + lookback = xử lý late arrival

---

## CP4 — Sửa CDC delete và chứng minh rerun (25 phút)

Mục tiêu: xử lý delete khi `after = null`, truyền xoá đến Silver, training snapshot và RAG index.

**Lệnh chạy:**
- [ ] `make verify` — phải đạt 18/18 ALL PASS
- [ ] `make test` — tất cả test phải pass
- [ ] `make rerun3` — chạy lại 2026-08-12 ba lần, so checksum

**Nơi cần sửa:**
- **Tệp 1:** `pipeline/staging.py`
  - **Vấn đề:** Xử lý delete (`op = 'd'`) bị bỏ sót hoặc sai
  - **Cách sửa:** Khi `op = 'd'`, lưu `before` (khoá), LSN, set `after = null`; không bỏ qua bản ghi

- **Tệp 2:** `pipeline/silver.py`
  - **Vấn đề:** Delete không được áp dụng cho silver_tickets
  - **Cách sửa:** Xoá hàng khi `after = null`, hoặc giữ tombstone với `_deleted_at`

- **Tệp 3:** `pipeline/gold.py`
  - **Vấn đề:** T-97 vẫn có trong training snapshot cũ và doc chunks
  - **Cách sửa:**
    - `gold_training_set`: loại ticket khi deleted
    - `gold_doc_chunks`: xoá cache (khóa `chunk_hash`)

**Tiêu chí tự kiểm tra:**
- `make verify` → **18/18 ALL PASS** (đạt từ 18 check đầu tiên)
- T-97 ở Silver: tombstone, không có `user_id`, `subject`, `body`
- T-97 **không có** trong `gold_training_set` (snapshot mới nhất)
- T-97 **không có** trong `gold_doc_chunks` (RAG index)
- `make rerun3` → **C0 = C1 = C2 = C3** (checksum)
  - C0 = fresh build (reset Silver/Gold, backfill mọi ngày)
  - C1 = chạy lại 2026-08-12 lần 1
  - C2 = chạy lại 2026-08-12 lần 2
  - C3 = chạy lại 2026-08-12 lần 3
  - **PASS**: ba checksum bằng nhau và bằng C0

**Ghi vào REPORT.md:**
- CDC delete: `op = 'd'`, `after = null`, khoá ở `before`
- Cách truyền xoá: Silver tombstone → training set exclude → RAG index delete
- T-97: không có ở Silver (tombstone), training set, chunks
- **Output:** `make rerun3` kết quả (C0, C1, C2, C3)
- Khái niệm: Tombstone, idempotency, data lineage, point-in-time snapshot

---

## CP5 — dbt và parity (25 phút)

Mục tiêu: build dbt model, so checksum với pipeline Python.

**Lệnh chạy:**
- [ ] `make setup-dbt` (cài requirements-dbt.txt)
- [ ] `make dbt` — build dbt models
- [ ] `make parity` — so sánh silver_tickets và gold_feature_daily

**Nơi cần sửa:**
- Không cần sửa ở CP5; hai cách cài đặt đã sẵn sàng

**Tiêu chí tự kiểm tra:**
- `make dbt` → **PASS = 19** (19 contract, data test, unit test pass)
- `make parity` → **PARITY** (silver_tickets checksum = gold_feature_daily checksum)

**Ghi vào REPORT.md:**
- Output: `make dbt` (PASS=19)
- Output: `make parity` (PARITY)
- Khái niệm: keyed merge, LSN guard, microbatch, lookback, data contract

---

## CP6 — Hoàn thiện bài nộp (30 phút)

Mục tiêu: viết REPORT, kiểm tra repo, commit, push, nộp lên LMS.

**Viết submission/REPORT.md:**
- [ ] **Thông tin học viên:**
  - Họ tên đầy đủ (không dấu)
  - MSSV
  - URL repo GitHub

- [ ] **Phần phân tích (≤ 1 trang, không tính output):**
  - Lỗi 1 (Silver): triệu chứng, nguyên nhân, cách sửa, khái niệm từ slide
  - Lỗi 2 (Gold): triệu chứng, nguyên nhân, cách sửa, khái niệm
  - Lỗi 3 (Delete): triệu chứng, nguyên nhân, cách sửa, khái niệm

- [ ] **Output thực tế (dán từ terminal):**
  - `make verify`: 18/18 ALL PASS
  - `make test`: (số test pass)
  - `make lateness`: P50, P95, P99
  - `make rerun3`: C0 = C1 = C2 = C3 kết quả PASS
  - `make dbt`: PASS=19
  - `make parity`: PARITY

- [ ] **Trả lời hai câu hỏi suy ngẫm** (nếu có trong RUBRIC.md)

**Kiểm tra trước nộp:**
- [ ] Verify: **18/18 ALL PASS**
- [ ] Pytest: **0 fail** (tất cả pass)
- [ ] Rerun: **PASS** (C0 = C1 = C2 = C3)
- [ ] dbt: **PASS = 19**
- [ ] parity: **PARITY**
- [ ] submission/checksums.txt: sinh bởi `make rerun3`, commit
- [ ] submission/REPORT.md: đầy đủ

**Tên repo và nộp bài:**
- [ ] Tên repo: `K4-Track02-Day17-HoVaTen-MSSV-DataPipelineEngineering`
  - Ví dụ: `K4-Track02-Day17-NguyenVanAn-20260001-DataPipelineEngineering`
  - Họ tên không dấu, không khoảng trắng
  - Phân cách bằng `-`
  - Dùng MSSV thực

- [ ] Repo PUBLIC trên GitHub
  - [ ] Mở được khi chưa đăng nhập
  - [ ] Lưu URL: `https://github.com/username/repo-name`

- [ ] Commit tất cả thay đổi:
  - `git add pipeline/` (các file đã sửa)
  - `git add submission/REPORT.md submission/checksums.txt`
  - `git commit -m "sửa 3 lỗi: Silver khoá, Gold lateness, CDC delete"`
  - `git push origin main`

- [ ] Kiểm tra: repo mở được trong trình duyệt khi chưa đăng nhập

- [ ] **Nộp URL repo vào LMS:**
  - Ô bài tập: K4 / Track 02 / Day 17
  - Format: `https://github.com/username/K4-Track02-Day17-NguyenVanAn-20260001-DataPipelineEngineering`
  - **Deadline: 23:59 ngày lab, múi giờ Asia/Ho_Chi_Minh (UTC+7)**

**Kiểm tra an toàn:**
- [ ] Repo không chứa `.env`, secret, API key
- [ ] Repo không chứa dữ liệu khách hàng thật
- [ ] Nếu sử dụng AI trong quá trình làm: tiết lộ trong REPORT.md

---

## Bonus (không bắt buộc, tối đa +10 điểm)

### B1 — Bước LLM có cache (+5)

- [ ] Mở `pipeline/llm_label.py`
- [ ] Thêm cache với khoá = `hash(input) + model_name + prompt_version`
- [ ] Đảm bảo lần chạy lại: 0 lần gọi LLM (dùng cache)
- [ ] Nếu sửa prompt: gắn nhãn lại (invalidate cache)
- [ ] Output sai schema: quarantine event
- [ ] Chạy: `make bonus-llm` → in **BONUS PASS**
- [ ] Ghi vào REPORT.md: output `BONUS PASS`

### B2 — Chọn một lựa chọn dưới đây (+5)

**B2a — Airflow 3 (thực thi daily run trên Airflow thật):**
- [ ] Chạy: `make docker-up` (khởi động container)
- [ ] Đọc [docs/AIRFLOW.md](docs/AIRFLOW.md)
- [ ] Chụp ảnh: 7 run thành công trên Airflow UI
- [ ] Ghi log: checksum từ mỗi run
- [ ] Nộp: tạo thư mục `bonus/airflow/`, đặt ảnh + log
- [ ] Ghi vào REPORT.md: tóm tắt hoặc reference

**B2b — Brainstorm bài toán thật:**
- [ ] Đọc [docs/bonus/BONUS-CHALLENGE.md](docs/bonus/BONUS-CHALLENGE.md)
- [ ] Viết `bonus/DESIGN.md` (~1-2 trang)
- [ ] Đề xuất: bài toán, nguồn dữ liệu, thiết kế pipeline, metrics
- [ ] Ghi vào REPORT.md: tóm tắt hoặc reference

**Chỉ làm B2a HOẶC B2b, không cả hai.**

---

## Danh sách kiểm tra cuối cùng — Trước khi nộp bài

### Đã đọc tài liệu
- [ ] README.md
- [ ] CHECKPOINTS.md
- [ ] RUBRIC.md
- [ ] RULES.md

### Đã chạy và kiểm tra (CP1-CP5)
- [ ] `make setup` thành công
- [ ] `make run` thành công
- [ ] `make verify` → **18/18 ALL PASS**
- [ ] `make test` → **0 fail** (tất cả pass)
- [ ] `make lateness` → có P50, P95, P99
- [ ] `make setup-dbt` thành công
- [ ] `make dbt` → **PASS = 19**
- [ ] `make parity` → **PARITY**

### submission/REPORT.md
- [ ] Họ tên, MSSV, URL repo
- [ ] Phân tích 3 lỗi (≤ 1 trang)
- [ ] Output từ verify, pytest, rerun, lateness, dbt, parity
- [ ] Trả lời 2 câu hỏi suy ngẫm (nếu có)
- [ ] Nếu làm B1: output `BONUS PASS`
- [ ] Nếu làm B2: reference hoặc tóm tắt

### submission/checksums.txt
- [ ] File được sinh bởi `make rerun3`
- [ ] Nội dung: C0 = C1 = C2 = C3 kết quả PASS
- [ ] Đã commit

### Sửa đúng và toàn bộ
- [ ] Sửa pipeline/silver.py (khoá ticket)
- [ ] Sửa pipeline/gold.py (lateness)
- [ ] Sửa pipeline/staging.py (CDC delete)
- [ ] **KHÔNG sửa:** scripts/verify.py, tests/, data/, logic checksum

### Repo GitHub
- [ ] Tên: `K4-Track02-Day17-HoVaTen-MSSV-DataPipelineEngineering`
- [ ] Ví dụ: `K4-Track02-Day17-NguyenVanAn-20260001-DataPipelineEngineering`
- [ ] PUBLIC: mở được khi chưa đăng nhập
- [ ] Commit tất cả thay đổi
- [ ] Push lên origin main

### Nộp trên LMS
- [ ] Sao chép URL repo (không clone URL)
- [ ] Nộp vào ô K4 / Track 02 / Day 17
- [ ] Kiểm tra: URL mở được khi nhấn từ LMS
- [ ] **Deadline: 23:59 ngày lab, UTC+7**

---

## Nếu bị kẹt — Gợi ý từng lần

Mở từng gợi ý khi cần; không mở hết một lúc.

<details><summary><b>Lỗi ở Silver — silver_tickets có nhiều hàng cho một ticket</b></summary>

- Slide: "Silver — Có khoá"
- Câu hỏi: cách ghi ticket để mỗi `ticket_id` chỉ có một hàng?
- Câu tiếp: khi chạy lại batch cũ sau batch mới, trạng thái nào phải thắng?
- Tìm: cột LSN (log sequence number) từ CDC, dùng làm điều kiện merge
- Cách: MERGE/UPSERT với `unique_key = ticket_id`, cập nhật chỉ khi `after.lsn > before.lsn`

</details>

<details><summary><b>Lỗi ở Gold — gold_feature_daily không khớp full recompute</b></summary>

- Chạy: `make lateness` trước
- Ghi: giá trị P99 (số giây / giờ / ngày)
- Slide: "Data về muộn" — đo từ Bronze, không đoán
- Tìm: lookback trong gold.py, đặt bằng P99
- Cách: recompute partition cũ khi batch mới, dùng event_time không phải _ingested_at

</details>

<details><summary><b>Lỗi xoá — T-97 vẫn còn ở Silver, training set, RAG index</b></summary>

- Mở: `data/cdc/tickets/2026-08-15.jsonl`
- Tìm: bản ghi có `"op": "d"` (delete)
- Ghi nhận: `after = null`, khoá ở `before.ticket_id`
- Slide: "CDC log-based" và "Xoá phải lan"
- Sửa:
  - staging.py: lưu before khi op='d'
  - silver.py: xoá hàng hoặc tombstone
  - gold.py: loại ticket từ training_set, xoá khóa từ chunks

</details>

---

## Bảng tóm tắt: Kiến thức từng checkpoint

| CP | Chủ đề | Khái niệm chính | Tiêu chí |
|---|---|---|---|
| 1 | Baseline | CDC, lateness | Verify fail, P99 lateness |
| 2 | Silver khoá | Dedup, unique_key, LSN | T-91 cuối = high/closed/bug |
| 3 | Late data | Event time vs ingest time, lookback | u05: 5 events, 3 clicks, 1 down |
| 4 | Delete | Tombstone, propagation | T-97 absent; C0=C1=C2=C3 |
| 5 | dbt | Keyed merge, microbatch | PASS=19, PARITY |
| 6 | Submit | Report, repo, LMS | URL, REPORT, checksums.txt |

---

Chúc bạn hoàn thành bài lab!
