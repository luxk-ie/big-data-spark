# Kế hoạch Đồ án 4 — Benchmark và tối ưu pipeline

**Nhóm 13 — Nguyễn Phú Vinh.** Trạng thái: baseline **VERIFIED** ngày 16/11/2026; thí nghiệm after và quyết định KEEP/REVERT còn **PENDING**. Đã đối chiếu brief, bài giảng Buổi 10 và mẫu báo cáo.

**PENDING:** Chưa hoàn tất vòng lập kế hoạch và review qua ChatGPT trong phiên này; bản kế hoạch được rút trực tiếp từ tài liệu giảng viên và repo, cần đối chiếu thêm khi kết nối trở lại.

## Phạm vi và nguồn yêu cầu

Giữ pipeline Yellow Taxi 01/2025 của Checkpoint 3: đúng một manifest `success`, Taxi Zone Lookup, Spark `local[*]`, Gold theo `(pickup_date, pickup_hour, pickup_borough)` với `trip_count`. Không đổi KPI, quy tắc lọc, raw, lookup hoặc cách ghi Gold trong thí nghiệm tối ưu đầu tiên.

| Yêu cầu | Nguồn giảng viên | Cách thực hiện dự kiến | Bằng chứng nghiệm thu |
| --- | --- | --- | --- |
| Freeze baseline, chạy ≥3 lần và báo median | `BigData_DoAn4_Brief_Checkpoint4_Template.docx` §1, §2, §4; slide Buổi 10 số 3, 6–8, 21 | Commit/tag code đo lường trước tối ưu; cùng manifest, lookup, máy, tài nguyên, config và trạng thái warm-up; chạy 1 lần warm-up rồi 3 lần đo | `reports/benchmark.csv`, commit/tag baseline, config và từng runtime |
| Chứng minh một bottleneck | Brief §1, §2, §7; slide 10–12, 25 | Lưu plan và stage/task metrics; xác định operator/job chiếm thời gian trước khi chọn thay đổi | `reports/explain-before.txt`, ảnh/metric Spark UI hoặc event log, `reports/experiment-log.md` |
| Một giả thuyết, một thay đổi chính | Brief §2, §4–5, §9; slide 13–19 | Stage 35/37 của full-row distinct có spill; thử giảm `spark.sql.files.maxPartitionBytes` từ mặc định xuống 2 MiB để tăng partition đầu vào | Giả thuyết viết trước tại `reports/experiment-log.md`, chỉ một config đổi, expected signal |
| Before/after công bằng | Brief §2, §4, §7; slide 7, 20–22, 28 | 3 run đo cho mỗi variant với tài nguyên và input cố định; ghi median và độ dao động, không chọn run nhanh nhất | Bảng runtime, stage/task/shuffle/cache metrics, `reports/explain-after.txt` |
| Correctness không regression | Brief §2, §6–7; slide 9, 25, 31 | So schema, grain duy nhất, row counts và `sum(trip_count)`, zone status và hash logic Gold đã sắp xếp | ≥3 assertions có kết quả thực đo; fail thì REVERT |
| Quyết định và báo cáo | Brief §2, §7, §10; slide 24, 29–30; mẫu slide Checkpoint 4 số 1–6 | Ghi KEEP/REVERT, giới hạn đo trên máy hiện tại, báo cáo và pitch ≤4 phút | `reports/checkpoint4.md`, `reports/Checkpoint4_Report.pptx`, commit/tag optimized nếu KEEP |

## Hiện trạng trước Checkpoint 4

- **VERIFIED ở Checkpoint 3:** full run 24,322 s và rerun 23,733 s trên cùng snapshot; 3.475.226 dòng raw, 3.475.204 dòng trong phạm vi, 4.652 nhóm Gold. Nguồn: `evidence/checkpoint3/run1.json`, `run2.json`, `rerun_comparison.json`.
- **VERIFIED cho Checkpoint 4:** tag `checkpoint4-baseline` (`e4dc1f0`), một warm-up và 3 run đo 23,884 / 23,791 / 24,084 s; median 23,884 s. Cùng raw/lookup/code/Gold SHA-256 và máy/config; nguồn `reports/benchmark.csv`, `evidence/checkpoint4/baseline/run1`–`run3`.
- `evidence/checkpoint3/plan.txt` có `BroadcastHashJoin` sẵn, nên không chọn broadcast làm thử nghiệm đầu. Plan có `Exchange` với 200 partition dự kiến nhưng AQE đang bật và Gold quan sát 1 partition; **chưa đủ** để gọi shuffle là bottleneck thực tế.
- **VERIFIED bottleneck ưu tiên:** action `raw.distinct().count()` mất median 7,381 s (~31% full runtime). Ở run1, stage 35 ghi 194.391.139 shuffle bytes và spill 158.828.820 disk bytes; stage 37 đọc shuffle đó và spill 180.322.746 bytes. Chi tiết và giả thuyết đã đăng ký tại `reports/experiment-log.md`.
- Tag baseline chứa đúng pipeline và harness; working tree còn thay đổi của các công việc khác, được để riêng. Không lấy hai lần chạy Checkpoint 3 làm baseline mới.
- Đã thêm tùy chọn `--stage-evidence` để lưu `stages.json`, `jobs.json` và thời gian từng nhóm action từ Spark UI đang chạy. Spark event log trực tiếp không khởi tạo được trên Windows hiện tại vì thiếu `winutils.exe`; dùng cùng cách lấy Spark UI cho cả baseline và after. Nếu thiếu execution evidence ở after, dừng thí nghiệm.

## Giao thức benchmark

1. **VERIFIED — cố định phép đo trước tối ưu.** Một manifest success và lookup; lưu SHA-256, kích thước, row count, code commit, Python/Spark, `local[*]`, logical cores, RAM/driver memory, AQE, `spark.sql.shuffle.partitions` và đường dẫn output. Giữ nguyên cách đo trong hai variant. Java version và cache state cần ghi rõ thêm trong báo cáo; không suy ra từ hai lần chạy Checkpoint 3.
2. **VERIFIED — baseline.** Tag `checkpoint4-baseline` sau kiểm thử và probe. Mỗi process Spark độc lập; 1 run warm-up không tính, sau đó 3 run đo liên tiếp. Harness tạo `--output data/processed/checkpoint4/baseline/<batch>/<run-id>` riêng cho từng run. `evidence.json`, `plan.txt`, `jobs.json`, `stages.json` của 3 run đo đã sao lưu gọn vào `evidence/checkpoint4/baseline/`; median lấy từ `reports/benchmark.csv`.
3. **VERIFIED — chẩn đoán và đăng ký giả thuyết.** Jobs 22–23 / stages 35–37 trùng cửa sổ thời gian của full-row distinct; shuffle và disk spill là tín hiệu ưu tiên. Giả thuyết một config và expected signal được viết tại `reports/experiment-log.md` **trước** phép đo after. Không chọn cache Gold vì baseline không chỉ ra Gold là bottleneck ưu tiên.
4. **PROPOSED — đúng một thay đổi.** Chỉ đặt `spark.sql.files.maxPartitionBytes=2097152` qua Spark submit args cho after; không đổi code pipeline, AQE, shuffle partitions, join, filter, KPI, resource hoặc quy tắc output. Probe xác nhận raw partitions tăng 15 → 29; hiệu quả runtime/spill còn phải đo.
5. **PENDING — after.** Trên cùng máy/input/resources/config, 1 warm-up và ≥3 run đo; ghi từng runtime, median, độ dao động, số job/scan lặp, cache hit, shuffle/spill/task metrics và plan. Mỗi variant có output/evidence riêng; tính speedup từ **median**, không từ run nhanh nhất.
6. **PENDING — correctness và quyết định.** Kiểm `raw_input_rows = invalid_pickup + out_of_scope + in_scope`, `joined_rows = in_scope_rows = sum(trip_count)`, schema/4 cột Gold, key `(date,hour,borough)` duy nhất, `zone_status`, và hash nội dung Gold sau sắp xếp. Nếu sai, REVERT. Nếu đúng nhưng chênh lệch runtime nằm trong độ dao động của các run hoặc cache làm tăng spill/GC, ghi **INCONCLUSIVE/REVERT**, không tuyên bố cải thiện. Chỉ KEEP khi tín hiệu execution đúng giả thuyết và giảm median lặp lại được; nêu giới hạn trên một máy, một tháng dữ liệu. Với câu hỏi dữ liệu tăng 10× của brief §8, chỉ nêu dự đoán và rủi ro bộ nhớ/cache; không gọi đó là kết quả benchmark nếu chưa chạy 10×.

## Cách chạy khi bắt đầu đo

Baseline đã chạy bằng lệnh sau từ root repo. Giữ cùng manifest, lookup và máy cho after; chỉ đổi một Spark config đã đăng ký. Script tự gọi `scripts/run.ps1` cho từng Spark process và thu `jobs.json`/`stages.json` trước khi Spark tắt.

```powershell
./scripts/benchmark_checkpoint4.ps1 -Variant baseline -Runs 3
```

**PENDING after:** `$env:PYSPARK_SUBMIT_ARGS='--conf spark.sql.files.maxPartitionBytes=2097152 pyspark-shell'`, rồi chạy `./scripts/benchmark_checkpoint4.ps1 -Variant after -Runs 3` trong cùng PowerShell session. Ghi và kiểm giá trị config này trước khi kết luận.

Mỗi `--output` là một root riêng; script tự tạo thư mục con `raw_<hash>_zone_<hash>_code_<hash>`. Bản sao Git chỉ chứa kết quả gọn và đường dẫn/evidence đã đối chiếu; không commit raw hoặc Gold lớn. Nếu raw/manifest của máy cũ không tồn tại, ingest lại một snapshot rồi cố định **một** manifest success cho toàn bộ thí nghiệm. Trước khi đo after, kiểm tra input/lookup hash và cấu hình khớp baseline; nếu không khớp, hủy cặp so sánh và đo lại.

## File và trình tự bàn giao

1. Tạo `reports/benchmark.csv` với một hàng cho mỗi run, bao gồm variant, commit, input/lookup hash, config/resources, warm-up, elapsed, correctness metrics và Gold checksum.
2. Tạo `reports/experiment-log.md` ghi observation → hypothesis → một change → expected/actual signal → KEEP/REVERT; lưu `reports/explain-before.txt`, `reports/explain-after.txt` và bằng chứng stage/task tương ứng.
3. Viết `reports/checkpoint4.md` theo mục A–F của brief; điền **mẫu 6 slide giảng viên** ở `reports/Checkpoint4_Report.pptx` bằng số đo thật. Gắn commit/tag optimized chỉ sau correctness và quyết định.

Không đưa con số minh họa trong slide bài giảng vào báo cáo như kết quả của nhóm. Sau khi giảng viên giao thêm điều kiện hoặc feedback, cập nhật bảng yêu cầu và thí nghiệm theo nguồn thực tế.
