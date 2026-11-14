# Big Data — Yellow Taxi Checkpoint 2 và 3

## Checkpoint 3: nhu cầu đón khách theo giờ và borough

**Trạng thái: VERIFIED trên full snapshot và rerun; xem `reports/checkpoint3.md`.** Pipeline chọn đúng một manifest success của Checkpoint 2, xác minh size/SHA-256, left join với Taxi Zone Lookup chính thức, đếm chuyến theo ngày × giờ × borough, ghi Parquet và evidence theo version. Gold đã aggregate trong Spark được ghi thành một file Parquet nhỏ qua PyArrow vì Spark Hadoop writer trên Windows cần `winutils.exe` ngoài môi trường hiện tại. Spark đọc lại file để kiểm tra. Dữ liệu raw và manifest lịch sử không bị sửa.

```powershell
./scripts/run.ps1 src/pipeline_tlc.py
./scripts/run.ps1 src/pipeline_tlc.py  # chạy lại cùng input để đối chiếu
```

Mặc định dùng `data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json` và `data/reference/taxi_zone_lookup.csv`. Có thể truyền `--manifest`, `--zones`, `--output` qua `-ScriptArgs`. Nếu máy khác không có raw cùng hash, cần ingest hoặc chuyển snapshot đúng hash trước khi chạy. Output nằm ở `data/processed/nyc_tlc_yellow/raw_<hash>_zone_<hash>_code_<hash>/` và được Git ignore. `gold.parquet`, `evidence.json`, `plan.txt`, `preview.csv` nằm cùng output; kết quả gọn có thể sao lưu vào `evidence/checkpoint3/` sau khi kiểm chứng. Bảng lookup nhỏ có provenance tại `metadata/taxi_zone_lookup.json`.

`tpep_pickup_datetime` là `timestamp_ntz`; pipeline giữ nguyên giá trị đồng hồ của nguồn và lọc `[2025-01-01, 2025-02-01)`, không suy diễn UTC. Chuyến có mã đón null hoặc không khớp lookup được giữ trong nhóm Unknown riêng. Lỗi ở distance, amount hay dropoff được báo cáo nhưng không loại chuyến khỏi KPI nhu cầu. Xem quy tắc và giới hạn tại `docs/checkpoint3_plan.md`.

Full run đã đo 3.475.226 dòng raw, 3.475.204 chuyến trong phạm vi, 4.652 nhóm Gold; rerun cùng input/code cho cùng SHA-256 Gold. Bằng chứng gọn nằm ở `evidence/checkpoint3/`. Slide theo mẫu giảng viên: `reports/Checkpoint3_Report.pptx` (6 trang; tên nhóm/thành viên cần điền trước khi nộp).

Mốc nghiệm thu kỹ thuật trong Git: tag cục bộ `checkpoint-3`. Repo chưa có remote; nhóm cần điền thông tin và nộp theo hướng dẫn giảng viên.

Kiểm thử:

```powershell
./scripts/run.ps1 -Script '-m' -ScriptArgs @('unittest', 'discover', '-s', 'tests', '-v')
```

## Checkpoint 2: ingest raw

Pipeline đã triển khai: **NYC TLC Yellow Taxi tháng 01/2025 → Python HTTP ingestion → local raw Parquet + manifest → Spark read/inspect**.

NYC 311 bị HTTP 403 nên sử dụng nguồn TLC dự phòng. Chưa triển khai ETL, dashboard hoặc ML. Local storage đơn giản, phù hợp checkpoint; chưa thể hiện lưu trữ phân tán.

## Môi trường và setup Windows

Đã chạy trên Python 3.12.14, Temurin Java 17.0.20.1, PySpark 3.5.8, PyArrow 19.0.1, `local[*]`. Dependencies được pin tại requirements.txt và requirements.lock.txt.

Trong PowerShell tại thư mục project:

```powershell
./scripts/setup.ps1
./scripts/run.ps1 src/env_check.py
```

Setup tự dùng Python bundled nếu có; có thể chỉ định `./scripts/setup.ps1 -Python 'C:/path/to/python.exe'` với Python 3.12 có pip. Nếu không có runtime phù hợp, script tải Python 3.12.10 từ python.org, cài riêng ở .runtime/python. Java 17 tải từ Eclipse Adoptium và đối chiếu SHA-256. Không đổi PATH hệ thống. Tải Spark/Java khoảng vài trăm MB; thời gian setup tùy mạng. Python 3.12.10 là fallback bootstrap, chưa được chạy kiểm chứng trong lần triển khai hiện tại.

Thư mục .venv và .runtime là tài nguyên cục bộ bị Git ignore. Venv phụ thuộc Python gốc, không sao chép .venv sang máy khác; chạy setup để tạo mới. Nếu PowerShell chặn script theo chính sách máy, dùng shell/thiết lập được quản trị viên cho phép; project không tự thay đổi ExecutionPolicy.

## Ingest và kiểm tra raw

```powershell
./scripts/run.ps1 src/ingest_tlc.py
./scripts/run.ps1 src/ingest_tlc.py
./scripts/run.ps1 src/ingest_tlc.py
./scripts/run.ps1 src/verify_checkpoint.py
$manifest = Get-ChildItem data/raw/nyc_tlc_yellow -Recurse -Filter manifest.json |
    Where-Object { (Get-Content $_.FullName -Raw | ConvertFrom-Json).status -eq 'success' } |
    Select-Object -First 1
./scripts/run.ps1 src/inspect_raw.py $manifest.FullName
```

Mỗi lần gọi ingest tải file nguồn thật, giữ bytes nguyên bản và tạo thư mục mới:

`data/raw/nyc_tlc_yellow/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>_<suffix>/yellow_tripdata_2025-01.parquet`

Manifest cùng run chứa dataset, source, ingested_at_utc, bytes, sha256, destination, status; bổ sung rows và HTTP metadata. Tải lỗi ghi status failed, giữ file .part nếu có và thoát khác 0. Gate gồm dung lượng, Content-Length nếu có, loại phản hồi, magic bytes Parquet và metadata đọc được. Spark inspect là bước xác minh đọc dữ liệu sau landing. Không tự động retry; người dùng chạy lại tạo một run khác. Không âm thầm ghi đè hoặc append.

**Chạy 3 lần:** 3 snapshot độc lập. Nếu nguồn không đổi, SHA-256 bằng nhau; đây là duplicate được nhận diện, không phải ba tập dữ liệu khác nhau để cộng dồn. Mọi phân tích phải chỉ định một manifest, không đọc wildcard cả ba run. Lần tải TLC lỗi do file handle trên Windows đã được sửa và có regression test; manifest failed cũ được giữ nguyên.

`destination` trong manifest là đường dẫn tuyệt đối của lần chạy trên máy hiện tại. Sau khi clone, ingest lại để tạo manifest đúng máy; không dùng manifest máy cũ như path đang tồn tại. Raw lớn được ignore. Sample dẫn xuất nhỏ được commit và có snapshot cha trong metadata/rerun_verification.json.

## Test và evidence

```powershell
./scripts/run.ps1 -Script '-m' -ScriptArgs @('unittest', 'discover', '-s', 'tests', '-v')
```

- logs/env_check.txt: versions, master, count 1.000.000 và PASS.
- logs/env_check_console.txt: stdout/stderr Spark của smoke test.
- logs/tlc_run2.txt, tlc_run3.txt, tlc_run4.txt: ba ingestion thành công.
- logs/ingest.jsonl: lịch sử cả success và failed.
- logs/raw_inspection.txt: schema, 5 mẫu, 3.475.226 dòng, 20 cột.
- logs/tests.txt: kết quả kiểm thử tự động.
- metadata/rerun_verification.json: checksums kiểm tra lại và run IDs.
- metadata/source.md: provenance, phiên bản nguồn và điều khoản.
- docs/checkpoint2_report.md: nội dung báo cáo và exit ticket.
- reports/Checkpoint2_Report.pptx: báo cáo 6 slide theo mẫu, có speaker notes cho pitch khoảng 4 phút.
- reports/Checkpoint2_Report.docx: mẫu checkpoint report đã điền, exit ticket và nguồn/evidence, 2 trang.

Repo lưu code, cấu hình mẫu, sample nhỏ, metadata và bằng chứng gọn; không lưu runtime, raw lớn hoặc secret. INSTRUCTIONS.md là quy tắc triển khai. Tag nghiệm thu cục bộ: `checkpoint-2`; dùng `git rev-parse checkpoint-2` để lấy commit hash. Chưa có remote/push.

## Tiếp theo

Nhóm cần bổ sung tên thành viên, owner và deadline thật; diễn tập pitch tối đa 4 phút và bảo đảm từng thành viên giải thích được flow/trade-off. Buổi 7 có thể dùng TLC để phân tích nhu cầu theo giờ, khoảng cách/thời lượng và giá cước. Kế hoạch NYC 311 cũ được giữ làm lịch sử; phạm vi đang chạy được mô tả trong báo cáo Checkpoint 2.
