# Big Data Checkpoint 2

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

Repo lưu code, cấu hình mẫu, sample nhỏ, metadata và bằng chứng gọn; không lưu runtime, raw lớn hoặc secret. INSTRUCTIONS.md là quy tắc triển khai. Tag nghiệm thu cục bộ: `checkpoint-2`; dùng `git rev-parse checkpoint-2` để lấy commit hash. Chưa có remote/push.

## Tiếp theo

Nhóm cần bổ sung tên thành viên, owner và deadline thật; diễn tập pitch tối đa 4 phút và bảo đảm từng thành viên giải thích được flow/trade-off. Buổi 7 có thể dùng TLC để phân tích nhu cầu theo giờ, khoảng cách/thời lượng và giá cước. Kế hoạch NYC 311 cũ được giữ làm lịch sử; phạm vi đang chạy được mô tả trong báo cáo Checkpoint 2.
