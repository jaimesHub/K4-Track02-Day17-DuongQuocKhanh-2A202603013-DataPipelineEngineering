# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Dương Quốc Khánh / 2A202603013
**Repo:** https://github.com/jaimesHub/K4-Track02-Day17-DuongQuocKhanh-2A202603013-DataPipelineEngineering
**Commit bài nộp:** d2e3558 (commit "CP6: hoàn thiện REPORT và checklist"; chứa toàn bộ sửa lỗi trong pipeline/ từ CP2–CP5)
**AI đã dùng và phạm vi hỗ trợ:** Claude Code (Anthropic) — được sử dụng để hỗ trợ quy trình CP1-CP5: đọc yêu cầu, chỉ ra các vị trí cần sửa trong pipeline/ (silver.py MERGE+LSN guard, config.py LOOKBACK_DAYS=3, staging.py CDC delete key từ before), chạy các lệnh verify/test/rerun3/dbt/parity lần đầu, và soạn thảo REPORT. Tôi đã tự chạy verify/test/rerun3/dbt/parity một cách độc lập để xác nhận kết quả và đã kiểm tra toàn bộ thay đổi trong code. Ghi chú: một subagent trước đó đã sửa dbt_project/ (vi phạm luật), đã được revert; bản sửa cuối cùng chỉ ảnh hưởng pipeline/ thôi.
**Nguồn tham khảo khác:** docs/ và slide của lab; không có nguồn ngoài

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify fail: silver_tickets có 24 hàng cho 12 ticket_id. T-91 hiển thị 3 trạng thái khác nhau [('low','open',None),('high','open',None),('high','closed','bug')] thay vì chỉ high/closed/bug. test_silver_tickets_one_row_per_ticket, test_silver_tickets_latest_state_wins fail. | gold_feature_daily checksum không khớp full recompute (c50b8851affe ≠ 8630e04a61d1). u05 ngày 08-12 có (2,0) chứ không phải (5,1). LOOKBACK_DAYS=0 < 3 (phải >= ceil(p99)=3). | T-97 không phải tombstone: is_deleted=False, còn user_id u06, subject, body. Còn 1 dòng trong gold_training_set mới nhất. Còn chunk của T-97 trong gold_doc_chunks. |
| **Nguyên nhân gốc** | silver.py dùng INSERT thuần, không có unique_key và không dedup qua batch. Mỗi batch (hoặc redelivery) append thêm hàng cho ticket_id → 24 hàng tích lũy, nhiều trạng thái mâu thuẫn tồn tại, trạng thái cuối không xác định. | config.py LOOKBACK_DAYS=0 nên mỗi ngày chỉ recompute ngày đó. Event đến muộn 3 ngày (event_time 08-12 nhưng _ingested_at 08-15) không nằm trong cửa sổ [08-15, 08-15] → không được tính vào event_date 08-12. | staging.py lấy ticket_id chỉ từ after.ticket_id, bỏ qua before. CDC delete có after=null → ticket_id=NULL → bị WHERE ticket_id IS NOT NULL lọc bỏ → delete không tới silver.py, T-97 không được đánh dấu xoá, không loại khỏi training/RAG. |
| **Cách sửa** (file, dòng) | silver.py dòng 64-101: thay INSERT thành MERGE ON ticket_id. WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE 12 cột; WHEN NOT MATCHED THEN INSERT. In-batch dedup (dòng 80): QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn DESC)=1 bắt buộc (MERGE không tự dedup khoá trùng trong batch). LSN guard làm idempotent: batch cũ replay không lùi trạng thái. | config.py dòng 28: LOOKBACK_DAYS = 3 (match ceil(p99)=3). gold.py dùng event_time để tính feature (dòng 30, 63). Mỗi ngày recompute partition [D-3, D] bằng DELETE+INSERT (overwrite), idempotent — late events thuộc event_date đúng được tính lại. | staging.py dòng 41: ticket_id = COALESCE(after.ticket_id, before.ticket_id). Các cột khác (user_id, subject, body, priority, status, category, created_at, updated_at) giữ nguyên lấy from after → NULL cho delete. silver.py dòng 78: is_deleted=(_op='d') → MERGE UPDATE PII thành NULL tạo tombstone. gold.py dòng 109 filter _op<>'d' exclude deleted khỏi training; dòng 148 WHERE NOT is_deleted exclude khỏi chunks. |
| **Khái niệm trên slide** | Keyed merge/upsert; dedup trong batch vs upsert giữa batch; idempotent write; LSN (log sequence number từ CDC); SCD Type 1 vs Type 2. | Event time vs ingest time; late-arriving data; LOOKBACK_DAYS = ceil(p99 lateness); recompute-partition / overwrite-partition cho idempotency. | CDC delete (op='d', after=null, key từ before); Debezium record structure; tombstone pattern (logical delete + NULL metadata); LSN ordering + delete propagation qua tầng. |

## 2. Các con số

- P99 lateness đo từ Bronze: **3.00 ngày** → LOOKBACK_DAYS = 3
- `submission/checksums.txt`: **PASS** — Gold combined checksum: **39e115c510ecdf526800eac227158a4f** (C0 = C1 = C2 = C3)
- `make parity`: **PARITY** — silver_tickets: 3c15dfd43701, gold_feature_daily: 8630e04a61d1

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: vì MERGE theo khoá + LSN guard làm ghi idempotent và chặn batch cũ làm lùi trạng thái mới; recompute lại các partition trong cửa sổ lookback ghi đè idempotent nên event đến muộn được tính vào đúng event_time mà chạy lại không nhân đôi.
- Tombstone thay vì xoá hẳn hàng trong Silver: để giữ lại LSN guard cho downstream (nếu batch cũ replay, LSN cũ < LSN tombstone nên không hồi sinh ticket đã xoá), và để có bằng chứng lineage/audit của việc xoá, thay vì che dấu sự việc bằng xóa vật lý.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: vì snapshot bất biến cho audit trail và tái lập kết quả huấn luyện/so sánh; phiên bản mới cho mỗi lần dữ liệu muộn/feedback thay đổi, không ghi đè lịch sử.
- DuckDB (lite) / dbt cho bài toán cỡ này thay vì Spark: vì dữ liệu nhỏ (vài chục dòng/ngày), chạy trên một máy không cần cluster; dbt cung cấp data contract, test, incremental model, microbatch khai báo, dễ audit lineage của từng bước và tái lập idempotent hơn script Python thuần.

## 4. Hai câu hỏi suy ngẫm

**1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?**

Snapshot cũ v2026-08-12..14 nên giữ bất biến cho audit trail. Nhưng để tuân thủ quyền được xoá (right to erasure), tôi sẽ áp dụng một trong ba cách: (a) retention policy + crypto-shredding — snapshot cũ giữ, nhưng mã hoá metadata/body theo user_key; sau khoảng thời gian lưu giữ (ví dụ 1 năm), tự động purge; (b) rebuild/redact snapshot cũ — khi có yêu cầu xoá, rebuild lại snapshot đó từ Bronze (loại T-97), ghi log purge để audit; (c) hybrid — snapshot mới nhất (v2026-08-16) exclude T-97 và RAG dùng v2026-08-16; snapshot cũ giữ nguyên nhưng có note/tag "historical — do not serve to users". Trong lab này, chỉ loại T-97 khỏi snapshot mới và RAG; ghi chú trade-off: tái lập vs quyền xoá, khoảng lưu giữ, quy trình purge snapshot cũ theo lịch sẽ cần thêm chính sách riêng.

**2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?**

Regex che email/phone là chặn trễ (che ở Silver, nhưng downstream LLM/RAG vẫn thấy tên). Tôi sẽ: (a) đặt chốt PII sớm ở ranh giới Bronze→Silver, trước mọi tầng phía sau; (b) thêm bước NER (Named Entity Recognition) hoặc LLM-classifier (hoặc Presidio-like rule engine) để phát hiện tên riêng (người, tổ chức, địa điểm), không chỉ regex email/phone; (c) quarantine records khi nghi ngờ PII (tên ngoài danh sách whitelist từ ticket.created_by, hoặc tên lạ), ghi log để review; (d) đo bằng: precision/recall của NER trên bộ test có nhãn, tỷ lệ false negative (PII bỏ sót) / false positive (text sạch bị flag) trên mẫu kiểm tra thủ công, alert khi rò rỉ phát hiện. Nguyên tắc: minimization — không đưa body thô vào RAG/training nếu không cần; nếu cần feedback từ user, chỉ trích text sau khi PII removal.

## 5. Output (Copy nguyên văn từ CP6)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 3.51s

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make dbt
[0m11:16:15  Running with dbt=1.12.5
[0m11:16:16  Registered adapter: duckdb=1.11.0
[0m11:16:17  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
[0m11:16:17  Concurrency: 1 threads (target='dev')
[0m11:16:17  1 of 19 START sql view model main.stg_events
[0m11:16:17  1 of 19 OK created sql view model main.stg_events [OK in 0.13s]
[0m11:16:17  2 of 19 START sql view model main.stg_ticket_changes
[0m11:16:17  2 of 19 OK created sql view model main.stg_ticket_changes [OK in 0.05s]
[0m11:16:17  3 of 19 START sql incremental model main.silver_events
[0m11:16:17  3 of 19 OK created sql incremental model main.silver_events [OK in 0.21s]
[0m11:16:17  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone
[0m11:16:18  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone [PASS in 0.23s]
[0m11:16:18  8 of 19 START sql incremental model main.silver_tickets
[0m11:16:18  8 of 19 OK created sql incremental model main.silver_tickets [OK in 0.26s]
[0m11:16:18  5 of 19 START test not_null_silver_events_event_id
[0m11:16:18  5 of 19 PASS not_null_silver_events_event_id [PASS in 0.08s]
[0m11:16:18  6 of 19 START test not_null_silver_events_user_id
[0m11:16:18  6 of 19 PASS not_null_silver_events_user_id [PASS in 0.03s]
[0m11:16:18  7 of 19 START test unique_silver_events_event_id
[0m11:16:18  7 of 19 PASS unique_silver_events_event_id [PASS in 0.03s]
[0m11:16:18  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other
[0m11:16:18  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other [PASS in 0.05s]
(rút gọn các dòng START/OK ở giữa)
[0m11:16:19  16 of 19 OK created sql microbatch model main.gold_feature_daily [SUCCESS in 0.42s]
[0m11:16:19  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date
[0m11:16:19  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date [PASS in 0.05s]
[0m11:16:19  18 of 19 START test not_null_gold_feature_daily_event_date
[0m11:16:19  18 of 19 PASS not_null_gold_feature_daily_event_date [PASS in 0.03s]
[0m11:16:19  19 of 19 START test not_null_gold_feature_daily_user_id
[0m11:16:19  19 of 19 PASS not_null_gold_feature_daily_user_id [PASS in 0.03s]

Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 2.15 seconds (2.15s).

Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

**Baseline CP1 (trước khi sửa):** verify 8/18, test 9 failed/25 passed, lateness p99 3.00

Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
