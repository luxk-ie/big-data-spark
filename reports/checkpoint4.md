# Báo cáo Đồ án 4 — Benchmark và tối ưu pipeline

**Nhóm 13 — Nguyễn Phú Vinh.** Trạng thái kỹ thuật: **VERIFIED** trên NYC TLC Yellow Taxi 01/2025 ngày 16/11/2026. Baseline tag `checkpoint4-baseline` (`e4dc1f0`); optimized code commit `afdbdce`; quyết định **KEEP**. Bản trình chiếu theo mẫu giảng viên: `reports/Checkpoint4_Report.pptx` (6 slide, số đo thật). Pitch trên lớp vẫn **PENDING**.

## A. Baseline

- **Workload:** đúng một manifest `success`, raw Parquet 59.158.238 bytes / 3.475.226 dòng, SHA-256 `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`; Taxi Zone Lookup 265 dòng, SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`. Pipeline lọc pickup tháng 01/2025, left join zone và tạo Gold grain `(pickup_date, pickup_hour, pickup_borough)` với `trip_count`.
- **Môi trường:** Python 3.12.14, Spark 3.5.8, Java Temurin 17.0.20.1, `local[*]`, 18 logical cores, RAM vật lý 16.886.128.640 bytes. Driver memory không đặt riêng; AQE `true`, `spark.sql.shuffle.partitions=200`, `spark.sql.files.maxPartitionBytes=134217728b` (mặc định). Mỗi run là một Spark process mới; không cache DataFrame. Cùng máy, input/lookup và cấu hình cho baseline/optimized.
- **Đo:** 1 warm-up 24,183 s (không tính); 3 run **23,884 / 23,791 / 24,084 s**, median **23,884 s**. 3.475.204 chuyến trong phạm vi, 4.652 dòng Gold, tổng `trip_count` 3.475.204. File gốc mỗi run dưới `data/processed/checkpoint4/baseline/20261116T063305911Z/`; bản sao evidence gọn tại `evidence/checkpoint4/baseline/`.

## B. Bottleneck evidence

`raw.distinct().count()` dùng để đếm duplicate toàn dòng mất median **7,381 s**, khoảng 31% runtime baseline. Ở run1, jobs 22–23 / stages 35–37 shuffle khoảng **195.077.820 bytes** và spill **339.151.566 bytes** xuống disk. Stage 35 gom trên 20 cột raw. Nguồn: `evidence/checkpoint4/baseline/run1/jobs.json`, `stages.json`, `reports/explain-before.txt`, thời gian action trong `evidence.json`.

## C. Hypothesis & change

Nếu snapshot không có fingerprint lặp, có thể chứng minh không có dòng trùng bằng cách shuffle một khóa 64-bit thay vì 20 cột. Optimized code dùng `xxhash64` trên toàn bộ cột, đếm fingerprint duy nhất; nếu có fingerprint lặp thì **fallback** về `raw.distinct().count()` exact. Hash collision không thể làm báo sai `duplicate_full_rows=0` vì mọi hash lặp đều kích hoạt fallback. Chỉ thay thuật toán đếm duplicate; input, join, KPI, output và Spark config giữ nguyên. Giả thuyết được ghi trước run tại `reports/experiment-log.md`.

Hai thử nghiệm trước bị **REVERT**: giảm `spark.sql.files.maxPartitionBytes` xuống 2 MiB cho median 24,850 s và spill không giảm; cache toàn bộ raw cho median 28,259 s. Cả hai vẫn đúng output nhưng chậm hơn baseline. Không có thay đổi nào trong hai thử nghiệm này được giữ trong optimized code.

## D. Before / After

| Metric | Baseline, batch đầu | Optimized, batch đầu | Tín hiệu |
| --- | ---: | ---: | --- |
| Runtime median, 3 run | 23,884 s | 23,217 s | Giảm **2,79%** |
| Action duplicate median | 7,381 s | 4,041 s | Giảm **45,25%** |
| Shuffle read/write, run1 | 195.077.820 bytes | 36.722.052 bytes | Giảm khoảng 81% |
| Disk spill, run1 | 339.151.566 bytes | 0 | Hết spill trong run đo |
| Gold output rows | 4.652 | 4.652 | Không đổi |
| `sum(trip_count)` | 3.475.204 | 3.475.204 | Không đổi |

Do runtime batch đầu dao động, đã đo bổ sung: 5 run optimized median **20,951 s** (20,493–22,387 s), rồi 3 run lại từ đúng bytes baseline tag median **25,250 s** (24,373–25,621 s), giảm **17,03%** ở cặp batch gần thời điểm. Đây là phép xác nhận bổ sung, không thay cho bảng ba run đầu; sự chênh lệch giữa các batch được ghi là giới hạn đo. Từng run và hash/code/config tại `reports/benchmark.csv`; explain optimized tại `reports/explain-after.txt`.

## E. Correctness

**VERIFIED trên 8 run optimized và 3 run baseline đo lại:**

1. `raw_input_rows = invalid_pickup_timestamp_rows + out_of_scope_rows + in_scope_rows = 0 + 22 + 3.475.204 = 3.475.226`.
2. `joined_rows = in_scope_rows = sum(trip_count) = 3.475.204`; `zone_status.matched = 3.475.204`; `duplicate_full_rows = 0`. Grain `(pickup_date, pickup_hour, pickup_borough)` duy nhất được pipeline assert.
3. Schema Gold giữ bốn cột `pickup_date: date`, `pickup_hour: integer`, `pickup_borough: string`, `trip_count: long`. SHA-256 file Gold ở mọi run là `72850e09350553b3431befb4c4d1ca686d0f4cc36709de8714fb0bfb5df01aee`.
4. Bộ kiểm thử 9 test **OK**; có fixture không trùng và có duplicate/null để kiểm nhánh fallback. Log: `evidence/checkpoint4/tests.txt`.

## F. Decision, hạn chế và bước tiếp

**KEEP** optimized code vì correctness giữ nguyên, shuffle/spill giảm đúng giả thuyết và median hai batch after đều thấp hơn baseline tương ứng. Kết luận hiệu năng áp dụng cho snapshot 01/2025 trên máy này; biến thiên warm-up/tải máy và cache hệ điều hành có thể ảnh hưởng runtime. Chưa đo dữ liệu tăng 10×; nếu có nhiều fingerprint lặp, fallback exact có thể làm runtime tăng. Khi nhận brief tiếp theo, kiểm tra khả năng mở rộng nhiều tháng và storage format trước khi đổi kiến trúc.

Chạy lại từ root repo bằng `./scripts/benchmark_checkpoint4.ps1 -Variant after_hash -Runs 3` trên optimized commit; raw không nằm trong Git nên máy khác phải ingest hoặc cung cấp đúng snapshot. `scripts/run.ps1` vẫn là entry point cho mọi Spark process. Đối chiếu đầy đủ hai thử nghiệm REVERT và từng batch ở `reports/experiment-log.md`.
