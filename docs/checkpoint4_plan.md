# Kế hoạch Đồ án 4 — Benchmark và tối ưu pipeline

**Nhóm 13 — Nguyễn Phú Vinh.** Trạng thái: benchmark, correctness, quyết định **KEEP** và slide theo mẫu giảng viên **VERIFIED** ngày 16/11/2026; pitch trên lớp còn **PENDING**. Kết quả và giới hạn tại `reports/checkpoint4.md`.

**PENDING:** Chưa hoàn tất vòng lập kế hoạch và review qua ChatGPT trong phiên này; bản kế hoạch được rút trực tiếp từ tài liệu giảng viên và repo, cần đối chiếu thêm khi kết nối trở lại.

## Phạm vi và nguồn yêu cầu

Giữ pipeline Yellow Taxi 01/2025 của Checkpoint 3: đúng một manifest `success`, Taxi Zone Lookup, Spark `local[*]`, Gold theo `(pickup_date, pickup_hour, pickup_borough)` với `trip_count`. Không đổi KPI, quy tắc lọc, raw, lookup hoặc cách ghi Gold trong thí nghiệm tối ưu đầu tiên.

| Yêu cầu | Nguồn giảng viên | Cách thực hiện dự kiến | Bằng chứng nghiệm thu |
| --- | --- | --- | --- |
| Freeze baseline, chạy ≥3 lần và báo median | `BigData_DoAn4_Brief_Checkpoint4_Template.docx` §1, §2, §4; slide Buổi 10 số 3, 6–8, 21 | Commit/tag code đo lường trước tối ưu; cùng manifest, lookup, máy, tài nguyên, config và trạng thái warm-up; chạy 1 lần warm-up rồi 3 lần đo | `reports/benchmark.csv`, commit/tag baseline, config và từng runtime |
| Chứng minh một bottleneck | Brief §1, §2, §7; slide 10–12, 25 | Lưu plan và stage/task metrics; xác định operator/job chiếm thời gian trước khi chọn thay đổi | `reports/explain-before.txt`, ảnh/metric Spark UI hoặc event log, `reports/experiment-log.md` |
| Một giả thuyết, một thay đổi chính | Brief §2, §4–5, §9; slide 13–19 | Sau hai thử nghiệm REVERT, KEEP thuật toán `xxhash64` với fallback exact cho full-row duplicate check; từng thử nghiệm chỉ một lever | Giả thuyết viết trước và quyết định từng lần tại `reports/experiment-log.md` |
| Before/after công bằng | Brief §2, §4, §7; slide 7, 20–22, 28 | 3 run đo cho mỗi variant với tài nguyên và input cố định; ghi median và độ dao động, không chọn run nhanh nhất | Bảng runtime, stage/task/shuffle/cache metrics, `reports/explain-after.txt` |
| Correctness không regression | Brief §2, §6–7; slide 9, 25, 31 | So schema, grain duy nhất, row counts và `sum(trip_count)`, zone status và hash logic Gold đã sắp xếp | ≥3 assertions có kết quả thực đo; fail thì REVERT |
| Quyết định và báo cáo | Brief §2, §7, §10; slide 24, 29–30; mẫu slide Checkpoint 4 số 1–6 | Ghi KEEP/REVERT, giới hạn đo trên máy hiện tại, báo cáo và pitch ≤4 phút | `reports/checkpoint4.md`, `reports/Checkpoint4_Report.pptx`, commit/tag optimized nếu KEEP |

## Hiện trạng trước Checkpoint 4

- **VERIFIED ở Checkpoint 3:** full run 24,322 s và rerun 23,733 s trên cùng snapshot; 3.475.226 dòng raw, 3.475.204 dòng trong phạm vi, 4.652 nhóm Gold. Nguồn: `evidence/checkpoint3/run1.json`, `run2.json`, `rerun_comparison.json`.
- **VERIFIED cho Checkpoint 4:** tag `checkpoint4-baseline` (`e4dc1f0`), một warm-up và 3 run đo 23,884 / 23,791 / 24,084 s; median 23,884 s. Cùng raw/lookup/code/Gold SHA-256 và máy/config; nguồn `reports/benchmark.csv`, `evidence/checkpoint4/baseline/run1`–`run3`.
- `evidence/checkpoint3/plan.txt` có `BroadcastHashJoin` sẵn, nên không chọn broadcast làm thử nghiệm đầu. Plan có `Exchange` với 200 partition dự kiến nhưng AQE đang bật và Gold quan sát 1 partition; **chưa đủ** để gọi shuffle là bottleneck thực tế.
- **VERIFIED bottleneck ưu tiên:** action `raw.distinct().count()` mất median 7,381 s (~31% full runtime). Ở run1, stage 35 ghi 194.391.139 shuffle bytes và spill 158.828.820 disk bytes; stage 37 đọc shuffle đó và spill 180.322.746 bytes. Chi tiết và giả thuyết đã đăng ký tại `reports/experiment-log.md`.
- **VERIFIED optimized:** code commit `afdbdce` dùng fingerprint nhanh với fallback exact; batch ba run đầu median 23,217 s (baseline 23,884 s), batch năm run sau median 20,951 s và baseline đo lại gần thời điểm median 25,250 s. Shuffle giảm, spill về 0, correctness khớp. Xem `reports/checkpoint4.md` để không diễn giải một speedup chung cho các batch khác thời điểm.
- Tag baseline chứa đúng pipeline và harness; working tree còn thay đổi của các công việc khác, được để riêng. Không lấy hai lần chạy Checkpoint 3 làm baseline mới.
- Đã thêm tùy chọn `--stage-evidence` để lưu `stages.json`, `jobs.json` và thời gian từng nhóm action từ Spark UI đang chạy. Spark event log trực tiếp không khởi tạo được trên Windows hiện tại vì thiếu `winutils.exe`; dùng cùng cách lấy Spark UI cho cả baseline và after. Nếu thiếu execution evidence ở after, dừng thí nghiệm.

## Giao thức benchmark

1. **VERIFIED — cố định phép đo trước tối ưu.** Một manifest success và lookup; lưu SHA-256, kích thước, row count, code commit, Python/Spark, `local[*]`, logical cores, RAM/driver memory, AQE, `spark.sql.shuffle.partitions` và đường dẫn output. Giữ nguyên cách đo trong hai variant. Java version và cache state cần ghi rõ thêm trong báo cáo; không suy ra từ hai lần chạy Checkpoint 3.
2. **VERIFIED — baseline.** Tag `checkpoint4-baseline` sau kiểm thử và probe. Mỗi process Spark độc lập; 1 run warm-up không tính, sau đó 3 run đo liên tiếp. Harness tạo `--output data/processed/checkpoint4/baseline/<batch>/<run-id>` riêng cho từng run. `evidence.json`, `plan.txt`, `jobs.json`, `stages.json` của 3 run đo đã sao lưu gọn vào `evidence/checkpoint4/baseline/`; median lấy từ `reports/benchmark.csv`.
3. **VERIFIED — chẩn đoán và đăng ký giả thuyết.** Jobs 22–23 / stages 35–37 trùng cửa sổ thời gian của full-row distinct; shuffle và disk spill là tín hiệu ưu tiên. Giả thuyết một config và expected signal được viết tại `reports/experiment-log.md` **trước** phép đo after. Không chọn cache Gold vì baseline không chỉ ra Gold là bottleneck ưu tiên.
4. **VERIFIED — một thay đổi mỗi thí nghiệm.** Giảm `spark.sql.files.maxPartitionBytes` xuống 2 MiB: REVERT vì median tăng và spill không giảm. Cache raw: REVERT vì median và spill tăng. Dùng fingerprint + exact fallback: KEEP vì giảm shuffle/spill và runtime, không đổi KPI/output; nguồn `reports/experiment-log.md`.
5. **VERIFIED — after.** Mỗi batch có warm-up không tính, 3 hoặc 5 run đo; `reports/benchmark.csv` có từng runtime, input/code/config, hash Gold và output riêng. `reports/explain-before.txt`, `reports/explain-after.txt`, `evidence/checkpoint4/` có plan, jobs và stages. Báo median theo từng batch, không chọn run nhanh nhất.
6. **VERIFIED — correctness và quyết định.** Raw accounting, join/KPI, schema/grain, zone status và Gold checksum khớp ở 8 run optimized và 3 baseline repeat; 9 test OK. Quyết định KEEP trong `reports/checkpoint4.md`, với giới hạn một máy/một tháng và biến thiên giữa batch. Dữ liệu tăng 10× vẫn **PENDING** vì chưa chạy.

## Cách chạy khi bắt đầu đo

Các lệnh đã dùng từ root repo; mỗi variant chạy trên commit/code tương ứng cùng manifest, lookup và máy. Script tự gọi `scripts/run.ps1` cho từng Spark process và thu `jobs.json`/`stages.json` trước khi Spark tắt.

```powershell
./scripts/benchmark_checkpoint4.ps1 -Variant baseline -Runs 3
./scripts/benchmark_checkpoint4.ps1 -Variant after_hash -Runs 3
./scripts/benchmark_checkpoint4.ps1 -Variant after_hash -Runs 5
```

Baseline repeat dùng đúng bytes từ tag `checkpoint4-baseline` (kiểm SHA-256 trùng code baseline), 1 warm-up + 3 run; xem lệnh/source trong `reports/experiment-log.md`. Bản thử partition 2 MiB và cache raw đã REVERT, không nằm trong optimized code.

Mỗi `--output` là một root riêng; script tự tạo thư mục con `raw_<hash>_zone_<hash>_code_<hash>`. Bản sao Git chỉ chứa kết quả gọn và đường dẫn/evidence đã đối chiếu; không commit raw hoặc Gold lớn. Nếu raw/manifest của máy cũ không tồn tại, ingest lại một snapshot rồi cố định **một** manifest success cho toàn bộ thí nghiệm. Trước khi đo after, kiểm tra input/lookup hash và cấu hình khớp baseline; nếu không khớp, hủy cặp so sánh và đo lại.

## File và trình tự bàn giao

1. **VERIFIED:** `reports/benchmark.csv`, `reports/experiment-log.md`, explain before/after, evidence stage/job và `reports/checkpoint4.md` theo mục A–F.
2. **VERIFIED:** `reports/Checkpoint4_Report.pptx` giữ mẫu 6 slide giảng viên, đã điền số đo thật, có bảng before/after và metric stage/shuffle; đã render và kiểm bố cục. **PENDING:** diễn tập pitch tối đa 4 phút và nộp theo hướng dẫn giảng viên. Gắn tag optimized vào commit bàn giao.

Không đưa con số minh họa trong slide bài giảng vào báo cáo như kết quả của nhóm. Sau khi giảng viên giao thêm điều kiện hoặc feedback, cập nhật bảng yêu cầu và thí nghiệm theo nguồn thực tế.
