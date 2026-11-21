# Đồ án Big Data — NYC Yellow Taxi

Pipeline PySpark `local[*]` cho dữ liệu **NYC TLC Yellow Taxi tháng 01/2025**. Chương trình tải một snapshot Parquet nguyên bản, ghi manifest chứa SHA-256, kiểm tra input, join với Taxi Zone Lookup và tạo Gold Parquet có grain `(pickup_date, pickup_hour, pickup_borough)` với KPI `trip_count`.

## Checkpoint 4: benchmark và tối ưu

**VERIFIED trên snapshot 01/2025, một máy.** Baseline median **23,884 s** (3 run); batch optimized đầu median **23,217 s** (giảm 2,79%). Cặp batch đo bổ sung gần thời điểm cho baseline **25,250 s** và optimized **20,951 s** (giảm 17,03%). Đợt optimized ngày 20/11/2026 có median **20,191 s** (3 run), không có baseline cùng ngày. Mỗi batch bỏ một warm-up; các mức giảm chỉ áp dụng cho đúng cặp đo và điều kiện tương ứng.

Bottleneck được xác nhận là phép kiểm duplicate toàn dòng: median **7,381 s**, shuffle khoảng 195 MB và spill khoảng 339 MB ở baseline run1. Bản **KEEP** dùng `xxhash64` trên mọi cột, fallback về distinct exact khi fingerprint lặp. Action duplicate batch đầu còn median **4,041 s**; Gold rows, `trip_count`, schema và SHA-256 giữ nguyên. Hai thử nghiệm giảm kích thước partition đọc và cache raw đã **REVERT** vì chậm hơn.

```powershell
./scripts/benchmark_checkpoint4.ps1 -Variant after_hash -Runs 3
```

Lệnh trên gọi `scripts/run.ps1` cho từng Spark process và ghi mỗi run vào `reports/benchmark.csv`. Xem [báo cáo](reports/checkpoint4.md), [slide](reports/Checkpoint4_Report.pptx), [nhật ký thí nghiệm](reports/experiment-log.md) và [kịch bản trình bày](reports/checkpoint4_presentation_script.md). Tag `checkpoint4-baseline` và `checkpoint4-optimized` đánh dấu hai phiên bản công bố; SHA-256 code trong CSV xác nhận đúng bytes đã benchmark. Raw/processed **mới** sau khi clone không tự được commit; repo vẫn giữ các snapshot lịch sử đã công bố ở Checkpoint 2/3.

## Chạy trên Windows (PowerShell)

```powershell
./scripts/setup.ps1
./scripts/run.ps1 src/env_check.py
```

Repo đã có raw snapshot 01/2025 và Gold của các lần chạy đã kiểm chứng. Chạy pipeline với **một manifest success** có sẵn:

```powershell
$manifest = (Resolve-Path 'data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json').Path
./scripts/run.ps1 -Script src/pipeline_tlc.py -ScriptArgs @('--manifest', $manifest)
```

Manifest lịch sử có đường dẫn tuyệt đối của máy tạo; `pipeline_tlc.py` tự dùng file Parquet nằm cạnh manifest khi clone sang máy khác. Có thể nạp **snapshot mới** bằng `./scripts/run.ps1 src/ingest_tlc.py`; lệnh này tạo run mới, không ghi đè raw cũ. Không đọc gộp nhiều run vì các lần ingest hiện có cùng SHA-256, tức là snapshot trùng nội dung.

Output nằm dưới `data/processed/nyc_tlc_yellow/raw_<hash>_zone_<hash>_code_<hash>/`: `gold.parquet`, `preview.csv`, `evidence.json` và `plan.txt`. Chạy lại với cùng manifest/code sẽ ghi cùng output version; kiểm tra `evidence.json` và `preview.csv` để đối chiếu. `data/reference/taxi_zone_lookup.csv` là snapshot lookup nhỏ, nguồn và hash ở `metadata/taxi_zone_lookup.json`.

## Kiểm thử

```powershell
./scripts/run.ps1 -Script '-m' -ScriptArgs @('unittest', 'discover', '-s', 'tests', '-v')
```

Pipeline dùng Python 3.12, Java 17, PySpark 3.5.8 và PyArrow 19.0.1; dependencies được pin trong `requirements.txt`. `setup.ps1` tạo `.venv` và tải runtime cần thiết vào `.runtime` nếu máy chưa có. Checkpoint 3 có **8/8 test PASS**; Checkpoint 4 bổ sung test cho nhánh fingerprint và fallback, tổng cộng **9/9 test OK** trên máy phát triển.

## Tài liệu, báo cáo và bằng chứng

- `reports/checkpoint4.md`, `reports/Checkpoint4_Report.pptx`, `reports/benchmark.csv`: kết quả Checkpoint 4 và từng lần chạy; `evidence/checkpoint4/` có plan, jobs, stages và test log.
- `reports/checkpoint3.md` và `reports/Checkpoint3_Report.pptx`: báo cáo Đồ án 3.
- `evidence/checkpoint3/run1.json`, `run2.json`, `rerun_comparison.json`, `plan.txt`, `preview.csv`: số đo và kết quả từ full snapshot đã chạy trên máy phát triển.
- `evidence/checkpoint3/tests_tlc_20261113.txt`: log 8 test; `logs/` lưu lịch sử setup, môi trường, ingest, kiểm tra raw và test của Checkpoint 2.
- `data/sample/` chỉ là 1.000 dòng dẫn xuất để kiểm tra nhỏ, **không đại diện** cho toàn tháng. `metadata/` ghi nguồn và checksum.
- `docs/` lưu kiến trúc, kế hoạch, roadmap và yêu cầu giảng viên đã đối chiếu; `reports/` lưu báo cáo Checkpoint 2, Checkpoint 3 và quy trình thực hiện.

Các đường dẫn tuyệt đối trong evidence/log là dấu vết của máy đã chạy. Repo có cả raw và processed lịch sử để xem, kiểm hash và chạy lại pipeline; các output version cũ không tự cập nhật khi code thay đổi. Có một run ingest lỗi giữ file `.parquet.part` và manifest failed để bảo toàn lịch sử, không dùng run này làm input. Các script kiểm tra raw phụ trợ cũ dùng đường dẫn tuyệt đối trong manifest; trên máy clone hãy chạy pipeline theo lệnh trên hoặc ingest một run mới trước khi dùng các script đó.

`evidence/checkpoint3/slide_validation.json` là kết quả kiểm tra phiên bản slide **6 trang trước đó**; báo cáo PowerPoint hiện tại đã được chỉnh thành 7 trang. Không dùng file validation cũ để xác nhận bố cục slide hiện tại.

Nguồn dữ liệu: [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Lookup là dataset join; không dùng dữ liệu khách hàng giả của StarterKit.
