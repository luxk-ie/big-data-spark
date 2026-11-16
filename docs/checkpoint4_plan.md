# Kế hoạch Đồ án 4 — Benchmark và tối ưu pipeline

**Nhóm 13 — Nguyễn Phú Vinh.** Trạng thái: **PROPOSED**. Đã đối chiếu brief, bài giảng Buổi 10 và mẫu báo cáo ngày 16/11/2026; **chưa chạy benchmark Checkpoint 4, chưa có optimized commit, chưa có quyết định KEEP/REVERT**.

**PENDING:** Chưa hoàn tất vòng lập kế hoạch và review qua ChatGPT trong phiên này; bản kế hoạch được rút trực tiếp từ tài liệu giảng viên và repo, cần đối chiếu thêm khi kết nối trở lại.

## Phạm vi và nguồn yêu cầu

Giữ pipeline Yellow Taxi 01/2025 của Checkpoint 3: đúng một manifest `success`, Taxi Zone Lookup, Spark `local[*]`, Gold theo `(pickup_date, pickup_hour, pickup_borough)` với `trip_count`. Không đổi KPI, quy tắc lọc, raw, lookup hoặc cách ghi Gold trong thí nghiệm tối ưu đầu tiên.

| Yêu cầu | Nguồn giảng viên | Cách thực hiện dự kiến | Bằng chứng nghiệm thu |
| --- | --- | --- | --- |
| Freeze baseline, chạy ≥3 lần và báo median | `BigData_DoAn4_Brief_Checkpoint4_Template.docx` §1, §2, §4; slide Buổi 10 số 3, 6–8, 21 | Commit/tag code đo lường trước tối ưu; cùng manifest, lookup, máy, tài nguyên, config và trạng thái warm-up; chạy 1 lần warm-up rồi 3 lần đo | `reports/benchmark.csv`, commit/tag baseline, config và từng runtime |
| Chứng minh một bottleneck | Brief §1, §2, §7; slide 10–12, 25 | Lưu plan và stage/task metrics; xác định operator/job chiếm thời gian trước khi chọn thay đổi | `reports/explain-before.txt`, ảnh/metric Spark UI hoặc event log, `reports/experiment-log.md` |
| Một giả thuyết, một thay đổi chính | Brief §2, §4–5, §9; slide 13–19 | Ưu tiên thử cache Gold **chỉ nếu** baseline xác nhận các action đang tính lại cùng scan/join/aggregate | Giả thuyết viết trước, diff code/config chỉ một lever, expected signal |
| Before/after công bằng | Brief §2, §4, §7; slide 7, 20–22, 28 | 3 run đo cho mỗi variant với tài nguyên và input cố định; ghi median và độ dao động, không chọn run nhanh nhất | Bảng runtime, stage/task/shuffle/cache metrics, `reports/explain-after.txt` |
| Correctness không regression | Brief §2, §6–7; slide 9, 25, 31 | So schema, grain duy nhất, row counts và `sum(trip_count)`, zone status và hash logic Gold đã sắp xếp | ≥3 assertions có kết quả thực đo; fail thì REVERT |
| Quyết định và báo cáo | Brief §2, §7, §10; slide 24, 29–30; mẫu slide Checkpoint 4 số 1–6 | Ghi KEEP/REVERT, giới hạn đo trên máy hiện tại, báo cáo và pitch ≤4 phút | `reports/checkpoint4.md`, `reports/Checkpoint4_Report.pptx`, commit/tag optimized nếu KEEP |

## Hiện trạng trước Checkpoint 4

- **VERIFIED ở Checkpoint 3:** full run 24,322 s và rerun 23,733 s trên cùng snapshot; 3.475.226 dòng raw, 3.475.204 dòng trong phạm vi, 4.652 nhóm Gold. Nguồn: `evidence/checkpoint3/run1.json`, `run2.json`, `rerun_comparison.json`.
- **PENDING cho Checkpoint 4:** hai runtime trên **không đủ** yêu cầu ≥3 run và thiếu bản ghi đầy đủ cores/memory/config/warm-up; không dùng chúng làm median baseline Đồ án 4.
- `evidence/checkpoint3/plan.txt` có `BroadcastHashJoin` sẵn, nên không chọn broadcast làm thử nghiệm đầu. Plan có `Exchange` với 200 partition dự kiến nhưng AQE đang bật và Gold quan sát 1 partition; **chưa đủ** để gọi shuffle là bottleneck thực tế.
- `src/pipeline_tlc.py` gọi nhiều action trên Gold: tổng KPI, kiểm grain và `collect` để ghi file. Đây là **ứng viên** tính lại lineage, cần xác nhận bằng job/stage metrics; chưa phải bottleneck đã chứng minh.
- **PENDING:** working tree hiện có thay đổi của các công việc trước; chưa tạo tag `checkpoint4-baseline`. Phải tách/chốt các thay đổi đó và xác nhận commit chứa đúng pipeline, harness đo trước khi gắn tag. Không lấy hai lần chạy Checkpoint 3 làm baseline mới.
- **PENDING cho baseline chính thức:** đã thêm tùy chọn `--stage-evidence` để lưu `stages.json`, `jobs.json` và thời gian từng nhóm action từ Spark UI đang chạy; lượt probe full snapshot đã xác nhận API hoạt động, nhưng chưa thay được 3 run benchmark. Spark event log trực tiếp không khởi tạo được trên Windows hiện tại vì thiếu `winutils.exe`; dùng cùng cách lấy Spark UI cho cả baseline và after. Nếu thiếu các file execution evidence, dừng thí nghiệm.

## Giao thức benchmark

1. **PENDING — cố định phép đo trước tối ưu.** Chọn một manifest success và lookup; lưu SHA-256, kích thước, row count, code commit, Python/Spark/Java, `local[*]`, logical cores, RAM/driver memory, AQE, `spark.sql.shuffle.partitions`, cache state và đường dẫn output. Giữ nguyên cách đo trong hai variant. Chụp plan trước khi sửa và ghi stage/task metrics của các action lặp.
2. **PENDING — baseline.** Tạo tag `checkpoint4-baseline` sau khi harness đo đã ổn định và kiểm tra working tree/code hash. Mỗi process Spark độc lập; 1 run warm-up không tính, sau đó ≥3 run đo liên tiếp trong cùng điều kiện. Harness tạo `--output data/processed/checkpoint4/baseline/<batch>/<run-id>` riêng cho từng run để giữ nguyên `evidence.json`, `plan.txt`, `jobs.json`, `stages.json` và `gold.parquet`; thư mục này nằm ngoài Git. Ghi cả ba `elapsed_seconds` từ evidence và median vào `reports/benchmark.csv`; không dùng thời gian của warm-up trong median.
3. **PROPOSED — chẩn đoán và đăng ký giả thuyết.** Nếu Spark UI/event log cho thấy các action trên Gold lặp lại scan Parquet, broadcast join và aggregate với thời gian đáng kể, ưu tiên thí nghiệm cache Gold. Nếu không thấy tín hiệu này, **không áp dụng cache theo mẹo**; chọn lại một bottleneck có evidence và ghi giả thuyết mới trước khi sửa code.
4. **PROPOSED — đúng một thay đổi.** Với giả thuyết cache được xác nhận, thêm `gold.cache()` trước action đầu tiên dùng Gold và `unpersist()` khi xong. Không đổi partition, AQE, join, filter, KPI, resource hoặc quy tắc output trong cùng experiment. Tính cả lần materialize cache vào elapsed time.
5. **PENDING — after.** Trên cùng máy/input/resources/config, 1 warm-up và ≥3 run đo; ghi từng runtime, median, độ dao động, số job/scan lặp, cache hit, shuffle/spill/task metrics và plan. Mỗi variant có output/evidence riêng; tính speedup từ **median**, không từ run nhanh nhất.
6. **PENDING — correctness và quyết định.** Kiểm `raw_input_rows = invalid_pickup + out_of_scope + in_scope`, `joined_rows = in_scope_rows = sum(trip_count)`, schema/4 cột Gold, key `(date,hour,borough)` duy nhất, `zone_status`, và hash nội dung Gold sau sắp xếp. Nếu sai, REVERT. Nếu đúng nhưng chênh lệch runtime nằm trong độ dao động của các run hoặc cache làm tăng spill/GC, ghi **INCONCLUSIVE/REVERT**, không tuyên bố cải thiện. Chỉ KEEP khi tín hiệu execution đúng giả thuyết và giảm median lặp lại được; nêu giới hạn trên một máy, một tháng dữ liệu. Với câu hỏi dữ liệu tăng 10× của brief §8, chỉ nêu dự đoán và rủi ro bộ nhớ/cache; không gọi đó là kết quả benchmark nếu chưa chạy 10×.

## Cách chạy khi bắt đầu đo

Lệnh sau là **quy trình dự kiến**, chưa phải bằng chứng đã chạy Checkpoint 4. Chạy từ root repo, sau khi chốt commit/tag baseline. Giữ cùng manifest, lookup, Spark config và máy cho cả hai variant; thay `baseline` bằng `after` khi đo sau tối ưu. Script tự gọi `scripts/run.ps1` cho từng Spark process và thu `jobs.json`/`stages.json` trước khi Spark tắt.

```powershell
./scripts/benchmark_checkpoint4.ps1 -Variant baseline -Runs 3
```

Mỗi `--output` là một root riêng; script tự tạo thư mục con `raw_<hash>_zone_<hash>_code_<hash>`. Bản sao Git chỉ chứa kết quả gọn và đường dẫn/evidence đã đối chiếu; không commit raw hoặc Gold lớn. Nếu raw/manifest của máy cũ không tồn tại, ingest lại một snapshot rồi cố định **một** manifest success cho toàn bộ thí nghiệm. Trước khi đo after, kiểm tra input/lookup hash và cấu hình khớp baseline; nếu không khớp, hủy cặp so sánh và đo lại.

## File và trình tự bàn giao

1. Tạo `reports/benchmark.csv` với một hàng cho mỗi run, bao gồm variant, commit, input/lookup hash, config/resources, warm-up, elapsed, correctness metrics và Gold checksum.
2. Tạo `reports/experiment-log.md` ghi observation → hypothesis → một change → expected/actual signal → KEEP/REVERT; lưu `reports/explain-before.txt`, `reports/explain-after.txt` và bằng chứng stage/task tương ứng.
3. Viết `reports/checkpoint4.md` theo mục A–F của brief; điền **mẫu 6 slide giảng viên** ở `reports/Checkpoint4_Report.pptx` bằng số đo thật. Gắn commit/tag optimized chỉ sau correctness và quyết định.

Không đưa con số minh họa trong slide bài giảng vào báo cáo như kết quả của nhóm. Sau khi giảng viên giao thêm điều kiện hoặc feedback, cập nhật bảng yêu cầu và thí nghiệm theo nguồn thực tế.
