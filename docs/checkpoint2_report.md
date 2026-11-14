# Báo cáo Checkpoint 2

Ngày chạy: 23/10/2026. Nhóm/thành viên: chưa được cung cấp. Các thông tin nhân sự và lịch nộp cần nhóm bổ sung trước khi nộp; không tự gán owner hoặc deadline.

## 1. Đề tài và checkpoint

Đề tài hiện tại: Ingestion và lưu trữ dữ liệu NYC TLC Yellow Taxi tháng 01/2025 có truy vết bằng PySpark local.

Checkpoint tag: `checkpoint-2` (lấy hash bằng `git rev-parse checkpoint-2`). Repo cục bộ, chưa cấu hình remote/push.

Nguồn ban đầu NYC 311 bị HTTP 403. Đã chuyển sang TLC, ứng viên dự phòng trong kế hoạch. File TLC tải trực tiếp từ liên kết chính thức.

## 2. Architecture update

`TLC monthly Parquet → Python urllib HTTP download → quality gate → raw snapshot + manifest/log → Spark read/inspect`

Trade-off: local storage và local[*] giảm độ phức tạp vận hành, đủ kiểm chứng ingestion/raw trong checkpoint; chưa thể hiện cluster phân tán. Không làm sạch raw, chưa thực hiện ETL/SQL/ML.

## 3. Environment evidence

- Python **3.12.14**; Java **Temurin 17.0.20.1**; PySpark **3.5.8**; PyArrow **19.0.1**.
- Mode: **local[*]**.
- Setup: `./scripts/setup.ps1`.
- Smoke test: `./scripts/run.ps1 src/env_check.py`.
- Kết quả: expected = actual = **1.000.000**; **PASSED**, khoảng 18,971 giây.
- Evidence: logs/env_check.txt và logs/env_check_console.txt.

## 4. Source và ingestion

- Nguồn: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
- Trang công bố và provenance: metadata/source.md.
- Ingestion command: `./scripts/run.ps1 src/ingest_tlc.py`.
- File thực tế: **59.158.238 bytes**, **3.475.226 dòng**, **20 cột**.
- SHA-256: `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`.
- Ba lần thành công: logs/tlc_run2.txt, logs/tlc_run3.txt, logs/tlc_run4.txt.
- Quality gate gồm HTTP status, content type, non-zero, Content-Length nếu có, magic/header/footer Parquet và đọc metadata. Spark đọc sample và count là bước xác minh tiếp theo.

## 5. Raw storage và manifest

Raw thực tế dưới `data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/`:

- `run_20261023T085112919770Z_3632e145/`
- `run_20261023T085118561129Z_7519021a/`
- `run_20261023T085124212690Z_7be43de3/`

Mỗi thư mục chứa `yellow_tripdata_2025-01.parquet` và `manifest.json`. Cả ba manifest có đầy đủ bảy trường bắt buộc và status success. Đã đọc lại file và tính lại SHA-256/bytes, kết quả PASS tại metadata/rerun_verification.json.

Spark đã đọc run đầu tiên: count **3.475.226**, schema **20 cột**, hiển thị 5 dòng. Evidence: logs/raw_inspection.txt và metadata/raw_inspection.json.

Sample dẫn xuất **1.000 dòng**: data/sample/tlc_yellow_2025-01_1000.parquet. Snapshot cha được ghi trong metadata/rerun_verification.json; sample không phải raw nguyên bản hoặc mẫu ngẫu nhiên đại diện.

## 6. Rerun, lỗi và next action

Ba lần chạy tạo ba run khác nhau, cùng checksum vì nguồn không đổi. Không overwrite và không cộng ba snapshot thành dữ liệu gấp ba. Đọc một manifest cụ thể cho phân tích.

Hai lỗi thực tế đã ghi nhận:

- NYC 311 HTTP 403: chuyển nguồn dự phòng, giữ failed manifest và log.
- TLC lần đầu bị Windows giữ file handle khi đổi tên .part: đã sửa đóng ParquetFile trước rename; giữ failed manifest cũ và có regression test.

Test tự động: **5 test PASS**, gồm giữ bytes và ba run, phản hồi rỗng/sai/header-only, Parquet đóng handle trước rename, lỗi mạng tạo failed manifest, Parquet hỏng/rỗng bị từ chối. Evidence: logs/tests.txt.

Không còn blocker kỹ thuật cho chuỗi đã chạy trên máy này. Chưa kiểm chứng setup sạch trên máy thứ hai hoặc nhánh tải Python fallback 3.12.10. Raw lớn bị Git ignore nên máy clone phải tải lại; sample nhỏ được lưu trong repo. Nhóm cần điền thành viên/owner/deadline thật và diễn tập thuyết trình tối đa 4 phút.

Buổi 7: dùng một snapshot TLC để học DataFrame/Spark SQL; kiểm tra schema, timestamp, giá cước và ngoại lệ trước khi làm phân tích nhu cầu theo giờ/khu vực. Không coi toàn bộ timestamp trong file tháng là chắc chắn thuộc tháng đó.

## Exit ticket

1. Lệnh kiểm tra môi trường: `./scripts/run.ps1 src/env_check.py` (wrapper thiết lập JAVA_HOME và Python; script gốc vẫn là src/env_check.py).
2. Snapshot: URL tháng + thời điểm ingest UTC + run ID + SHA-256, ghi cùng bytes và destination trong manifest.
3. Chạy 3 lần: 3 thư mục riêng; source không đổi có hash giống nhau; không ghi đè raw cũ.
4. Rủi ro trước Buổi 7: khác môi trường máy thành viên và schema/ngoại lệ dữ liệu. Owner/deadline chờ nhóm xác định.
