# Đồ án Big Data — NYC Yellow Taxi

Pipeline PySpark `local[*]` cho dữ liệu **NYC TLC Yellow Taxi tháng 01/2025**. Chương trình tải một snapshot Parquet nguyên bản, ghi manifest chứa SHA-256, kiểm tra input, join với Taxi Zone Lookup và tạo Gold Parquet có grain `(pickup_date, pickup_hour, pickup_borough)` với KPI `trip_count`.

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

Pipeline dùng Python 3.12, Java 17, PySpark 3.5.8 và PyArrow 19.0.1; dependencies được pin trong `requirements.txt`. `setup.ps1` tạo `.venv` và tải runtime cần thiết vào `.runtime` nếu máy chưa có. Lần kiểm tra trước khi xuất bản có **8/8 test PASS** trên máy phát triển.

## Tài liệu, báo cáo và bằng chứng

- `reports/checkpoint3.md` và `reports/Checkpoint3_Report.pptx`: báo cáo Đồ án 3.
- `evidence/checkpoint3/run1.json`, `run2.json`, `rerun_comparison.json`, `plan.txt`, `preview.csv`: số đo và kết quả từ full snapshot đã chạy trên máy phát triển.
- `evidence/checkpoint3/tests_tlc_20261113.txt`: log 8 test; `logs/` lưu lịch sử setup, môi trường, ingest, kiểm tra raw và test của Checkpoint 2.
- `data/sample/` chỉ là 1.000 dòng dẫn xuất để kiểm tra nhỏ, **không đại diện** cho toàn tháng. `metadata/` ghi nguồn và checksum.
- `docs/` lưu kiến trúc, kế hoạch, roadmap và yêu cầu giảng viên đã đối chiếu; `reports/` lưu báo cáo Checkpoint 2, Checkpoint 3 và quy trình thực hiện.

Các đường dẫn tuyệt đối trong evidence/log là dấu vết của máy đã chạy. Repo có cả raw và processed lịch sử để xem, kiểm hash và chạy lại pipeline; các output version cũ không tự cập nhật khi code thay đổi. Có một run ingest lỗi giữ file `.parquet.part` và manifest failed để bảo toàn lịch sử, không dùng run này làm input. Các script kiểm tra raw phụ trợ cũ dùng đường dẫn tuyệt đối trong manifest; trên máy clone hãy chạy pipeline theo lệnh trên hoặc ingest một run mới trước khi dùng các script đó.

`evidence/checkpoint3/slide_validation.json` là kết quả kiểm tra phiên bản slide **6 trang trước đó**; báo cáo PowerPoint hiện tại đã được chỉnh thành 7 trang. Không dùng file validation cũ để xác nhận bố cục slide hiện tại.

Nguồn dữ liệu: [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Lookup là dataset join; không dùng dữ liệu khách hàng giả của StarterKit.
