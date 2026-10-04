# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Dương Quốc Khánh
**Repo:** https://github.com/jaimesHub/K4-Track02-Day17-DuongQuocKhanh-2A202603013-DataPipelineEngineering
**Commit bài nộp:** TBD
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** TBD
**Nguồn tham khảo khác (nếu có):** TBD

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify fail: silver_tickets has exactly one row per ticket_id (24 rows cho 12 tickets). T-91 got [('low','open',None),('high','open',None),('high','closed','bug')] thay vì high/closed/bug. test_silver_tickets_one_row_per_ticket, test_silver_tickets_latest_state_wins fail. | gold_feature_daily không khớp full recompute (c50b8851affe != 8630e04a61d1). u05 ngày 08-12 got (2,0) expected (5,1). LOOKBACK_DAYS=0 < 3 (phải >= ceil(p99)=3). | T-97 không phải tombstone (is_deleted=False, còn user_id u06, subject, body). Còn 1 dòng trong training snapshot mới nhất. Còn 1 chunk trong RAG. |
| **Nguyên nhân gốc** | Ticket được ghi bằng INSERT thuần, không có unique_key và không có LSN guard, nên mỗi batch (và mỗi lần chạy lại/redelivery) append thêm hàng trùng cho cùng ticket_id → 24 hàng cho 12 ticket, nhiều trạng thái mâu thuẫn cùng tồn tại nên trạng thái cuối không xác định. | LOOKBACK_DAYS = 0 nên mỗi ngày chỉ recompute ngày đó; event đến muộn 3 ngày (event_time 08-12 nhưng _ingested_at 08-15) không nằm trong cửa sổ [08-15, 08-15] khi chạy 08-15 → không được tính vào ngày 08-12. | pipeline/staging.py dòng ~40, ~45-51 lấy ticket_id từ after.ticket_id chỉ (không lấy before.ticket_id), nên CDC delete với after=null → ticket_id=NULL → bị lọc bỏ bởi WHERE ticket_id IS NOT NULL → delete không bao giờ tới silver.py, T-97 không được đánh dấu xoá. |
| **Cách sửa** (file, vài dòng) | MERGE ON ticket_id trong pipeline/silver.py (upsert_silver_tickets, dòng 64-101), WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE, WHEN NOT MATCHED THEN INSERT; LSN bằng nhau thì giữ hàng hiện tại (idempotent), batch cũ chạy lại không làm lùi trạng thái. Thêm mệnh đề: dedup trong cùng batch bằng QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn DESC)=1 (dòng 80) là bắt buộc vì MERGE không tự dedup khoá trùng trong nguồn (dedup trong batch ≠ upsert giữa các batch). | Set LOOKBACK_DAYS = 3 trong pipeline/config.py (dòng 28) để match ceil(p99) = 3, nhằm recompute các partition [D - 3, D] chứa event đến muộn, tính lại vào event_date của chúng idempotent (ghi đè, không append). | Sửa pipeline/staging.py dòng ~40, ~45-51: dùng COALESCE(after.ticket_id, before.ticket_id) cho ticket_id và metadata (priority, status, category, created_at, updated_at), giữ PII (user_id, subject, body) từ after (null cho delete). Điều này cho phép delete records với ticket_id từ before đi qua staging, đến silver.py nơi dòng 78 đặt is_deleted=(_op='d'), MERGE UPDATE các cột PII thành NULL tạo tombstone. |
| **Khái niệm trên slide** | Keyed merge/upsert; dedup trong batch vs upsert giữa batch; idempotent write; LSN (thứ tự thay đổi CDC); SCD1 cho silver_tickets (SCD2 nằm ở silver_ticket_history). | Event time vs ingest time; late-arriving data; LOOKBACK_DAYS = ceil(p99 lateness); recompute-partition / overwrite-partition cho idempotency. | CDC delete operation (op='d'); Debezium record structure (after=null for delete, lấy metadata từ before); tombstone pattern (logical delete marker); LSN ordering. |

## 2. Các con số

- P99 lateness đo từ Bronze: 3 ngày → LOOKBACK_DAYS = 3
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: vì MERGE theo khoá + LSN guard làm ghi idempotent và chặn batch cũ làm lùi trạng thái mới; recompute lại các partition trong cửa sổ lookback ghi đè idempotent nên event đến muộn được tính vào đúng event_time mà chạy lại không nhân đôi.
- Tombstone thay vì xoá hẳn hàng trong Silver: để giữ lại LSN guard cho downstream (nếu batch cũ replay, LSN cũ < LSN tombstone nên không hồi sinh ticket đã xoá), và để có bằng chứng lineage/audit của việc xoá, thay vì che dấu sự việc bằng xóa vật lý.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark:

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

```text
$ make run
=== Day 17 pipeline: fresh build 2026-08-10 .. 2026-08-16 ===
  2026-08-10  bronze+0   tickets:4   events:6   quarantined:0 features[2026-08-10..2026-08-10] snapshot v2026-08-10:1 chunks:2 (embedded 2)
  2026-08-11  bronze+0   tickets:7   events:11  quarantined:0 features[2026-08-11..2026-08-11] snapshot v2026-08-11:2 chunks:5 (embedded 2)
  2026-08-12  bronze+0   tickets:12  events:17  quarantined:0 features[2026-08-12..2026-08-12] snapshot v2026-08-12:5 chunks:8 (embedded 1)
  2026-08-13  bronze+0   tickets:15  events:21  quarantined:1 features[2026-08-13..2026-08-13] snapshot v2026-08-13:6 chunks:10 (embedded 1)
  2026-08-14  bronze+0   tickets:18  events:25  quarantined:0 features[2026-08-14..2026-08-14] snapshot v2026-08-14:7 chunks:12 (embedded 1)
  2026-08-15  bronze+0   tickets:20  events:32  quarantined:1 features[2026-08-15..2026-08-15] snapshot v2026-08-15:8 chunks:15 (embedded 1)
  2026-08-16  bronze+0   tickets:24  events:39  quarantined:0 features[2026-08-16..2026-08-16] snapshot v2026-08-16:10 chunks:22 (embedded 3)

Gold checksums:
  gold_feature_daily   c50b8851affeb418fcb824e65099d9be
  gold_training_set    bd80ed585cda944fc9f9d63a63f00316
  gold_doc_chunks      b49150795e84b6016aa19297b2707a03
  gold (combined)      f90edc98a9c0d5c6f5ef168dd422fe88

$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [XX ] Silver  silver_tickets has exactly one row per ticket_id  (24 rows for 12 tickets)
  [XX ] Silver  T-91 shows its latest state: high / closed / bug  (got [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')])
  [XX ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left  (got [(False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.'), (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.')])
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [XX ] Gold    gold_feature_daily reconciles with a full recompute from Silver  (c50b8851affe != 8630e04a61d1)
  [XX ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12  (got (2, 0), expected (5, 1))
  [XX ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)  (LOOKBACK_DAYS=0 < 3)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [XX ] Gold    latest training snapshot excludes the deleted ticket T-97  (1 row(s))
  [XX ] Gold    deletes propagate to the RAG index: no chunk of T-97  (2 chunk(s))
  [XX ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks  (22 rows / 9 chunks, embedded 0)
  [XX ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build  (see submission/checksums.txt)

RESULT: 8/18 checks — FAILURES ABOVE

$ make test
FAILED tests/test_contracts.py::test_silver_tickets_one_row_per_ticket - assert 24 == 12
FAILED tests/test_contracts.py::test_silver_tickets_latest_state_wins - AssertionError: assert [('low', 'ope...osed', 'bug')] == [('high', 'closed', 'bug')]
FAILED tests/test_contracts.py::test_cdc_delete_becomes_tombstone - AssertionError: assert [(False, 'u06...ệu của tôi.')] == [(True, None, None, None)]
FAILED tests/test_contracts.py::test_feature_daily_reconciles_with_full_recompute - AssertionError: assert 'c50b8851affe...824e65099d9be' == '8630e04a61d1...7a49e148926b0'
FAILED tests/test_contracts.py::test_late_events_land_in_their_event_day - assert [(2, 1, 0)] == [(5, 3, 1)]
FAILED tests/test_contracts.py::test_lookback_covers_measured_lateness - assert 0 >= 3
FAILED tests/test_contracts.py::test_deleted_ticket_leaves_training_and_rag - assert [(1,)] == [(0,)]
FAILED tests/test_contracts.py::test_doc_chunks_unique - assert 22 == 9
FAILED tests/test_rerun.py::test_rerun_old_day_three_times_keeps_gold_checksum - gold checksums differ from the fresh build
(9 failed, 25 passed in 6.45s)

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 0

$ make rerun3

$ make dbt

$ make parity
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
