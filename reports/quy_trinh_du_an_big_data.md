# Toàn bộ quy trình thực hiện project Big Data đến Checkpoint 3

**Cập nhật:** 10/11/2026. **Phạm vi báo cáo:** từ tài liệu Đồ án 1, triển khai Checkpoint 2 đến pipeline Checkpoint 3 và lần chạy kiểm tra mới nhất. Tài liệu này dùng để trình bày tiến độ và đối chiếu bằng chứng; các mốc **PENDING** chưa được nhận là đã hoàn thành.

## 1. Tóm tắt đề tài

Project hiện xử lý **NYC TLC Yellow Taxi tháng 01/2025**. Dòng dữ liệu đã chạy:

```text
TLC Parquet → tải và giữ raw nguyên bản + manifest → kiểm SHA-256/schema
            → Spark chọn/lọc/đánh dấu chất lượng → left join Taxi Zone Lookup
            → đếm chuyến theo ngày × giờ × borough → Gold Parquet + evidence
```

Câu hỏi phân tích hiện tại: **số chuyến đón khách thay đổi theo ngày, giờ và borough như thế nào trong tháng 01/2025?** Chỉ số chính `trip_count` đếm **bản ghi chuyến**, không phải số hành khách hay doanh thu. Đây là pipeline xử lý dữ liệu bằng Spark, **không có bước train mô hình ML** trong yêu cầu Checkpoint 3. Spark chạy `local[*]` trên một máy, chưa phải cluster phân tán.

## 2. Căn cứ học phần và trạng thái từng giai đoạn

Đã kiểm danh mục **22 file** người dùng cung cấp; tên và vai trò ở [`docs/course_materials.md`](../docs/course_materials.md). Nguồn chính cho từng checkpoint là brief giảng viên, không lấy ví dụ `orders.csv`/`customers.csv` trong StarterKit làm dữ liệu taxi.

| Giai đoạn | Nguồn yêu cầu | Việc đã có và trạng thái |
| --- | --- | --- |
| Đồ án 1 / Proposal | `BigData_Buoi03_DoAn1_Brief_Proposal_Template.docx` §1–2; Pitch Template và Workshop Buổi 3 | **PENDING:** bộ file có mẫu/brief nhưng không có proposal đã điền hoặc feedback duyệt. Vì vậy chưa khẳng định đề tài TLC/KPI hiện tại đã được giảng viên chấp thuận từ Đồ án 1. |
| Đồ án 2 / Checkpoint 2 | `BigData_Buoi06_DoAn2_Brief_Checkpoint_Template.docx`; slide Buổi 6 | **VERIFIED:** dựng môi trường, ingest raw, manifest, ba run TLC thành công, Spark đọc/kiểm raw, báo cáo; tag Git `checkpoint-2`. |
| Đồ án 3 / Checkpoint 3 | `BigData_DoAn3_Brief_Checkpoint3_Template.docx` §1–6; slide Buổi 8; mẫu báo cáo | **VERIFIED** về pipeline kỹ thuật/full run/rerun/test. **PENDING** thông tin nhóm, xác nhận đề tài, thuyết trình và nộp bài. Đối chiếu D1–D7/R1/T1 tại [`docs/checkpoint3_requirements.md`](../docs/checkpoint3_requirements.md). |
| P4 / Buổi 9–10 | Workshop Buổi 3 slide 4; Buổi 8 slide 8, 31 | **PROPOSED:** slide nhắc performance/benchmark; chưa có brief/rubric riêng nên chưa triển khai tối ưu. |

## 3. Diễn biến từ đầu đến hiện tại

### 3.1. Chọn nguồn dữ liệu và xử lý rủi ro

Kế hoạch cũ dùng **NYC 311**. Khi thử hai endpoint chính thức, nguồn trả **HTTP 403**; failed manifest và log được giữ lại, không thay bằng dữ liệu giả. Nhóm chuyển sang ứng viên dự phòng **NYC TLC Yellow Taxi 01/2025**. Nguồn Parquet tháng được ghi tại [`metadata/source.md`](../metadata/source.md); TLC công bố các file trip record theo tháng. Việc đổi nguồn làm `docs/project_plan.md` và phần NYC 311 trong `docs/data_dictionary.md` trở thành phương án lịch sử, không phải pipeline đang chạy.

### 3.2. Checkpoint 2: môi trường, ingestion, raw

Môi trường đã chạy: **Python 3.12.14**, **Temurin Java 17.0.20.1**, **PySpark 3.5.8**, **PyArrow 19.0.1**, Spark `local[*]`. Dependencies được pin tại `requirements.txt`; PowerShell wrapper [`scripts/run.ps1`](../scripts/run.ps1) thiết lập Java/Python cho mỗi lệnh. Smoke test Spark đếm **1.000.000/1.000.000** trong khoảng **18,971 giây**; xem `logs/env_check.txt` và `logs/env_check_console.txt`. Thời gian này là số đo trên máy hiện tại.

Lệnh chính của bước ingest:

```powershell
./scripts/setup.ps1
./scripts/run.ps1 src/env_check.py
./scripts/run.ps1 src/ingest_tlc.py
./scripts/run.ps1 src/verify_checkpoint.py
```

[`src/ingest_tlc.py`](../src/ingest_tlc.py) tải Parquet, kiểm HTTP/bytes/Parquet rồi lưu **nguyên byte** vào `data/raw/nyc_tlc_yellow/ingest_date=.../run_.../`. Mỗi run có `manifest.json` chứa dataset, source, UTC ingestion time, bytes, SHA-256, destination và status. Raw và manifest lịch sử không bị chỉnh để phục vụ Đồ án 3.

Ngày 23/10/2026 có **ba run TLC thành công**, mỗi run một thư mục riêng. Cả ba cùng SHA-256 `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`, nghĩa là **ba snapshot cùng nội dung**; không cộng dồn thành ba lần số chuyến. Đối chiếu tại [`metadata/rerun_verification.json`](../metadata/rerun_verification.json). Một lỗi đầu tiên do Windows giữ file handle lúc đổi tên `.part` đã được sửa bằng cách đóng ParquetFile trước rename; failed manifest vẫn được giữ. Bài kiểm thử ingestion ban đầu có **5 test qua**, xem `logs/tests.txt`.

Spark đọc một raw snapshot và đo **59.158.238 bytes, 3.475.226 dòng, 20 cột**; [`metadata/raw_inspection.json`](../metadata/raw_inspection.json) và `logs/raw_inspection.txt` ghi bằng chứng. Sample 1.000 dòng trong `data/sample/` chỉ phục vụ kiểm nhỏ, không đại diện thống kê toàn tháng. Báo cáo Đồ án 2 tại [`docs/checkpoint2_report.md`](../docs/checkpoint2_report.md) và `reports/Checkpoint2_Report.docx/.pptx`; tag cục bộ `checkpoint-2`.

### 3.3. Chuẩn bị Checkpoint 3: yêu cầu và thiết kế

Brief Đồ án 3 yêu cầu một lệnh raw → output, ít nhất ba transformations có lý do, một join có kiểm key/cardinality, một aggregation có grain rõ, quality, baseline, khả năng chạy lại, README/report/evidence và phần nhóm giải thích được. Bảng đối chiếu nguồn/mục/slide và trạng thái D1–D7/R1/T1 nằm ở [`docs/checkpoint3_requirements.md`](../docs/checkpoint3_requirements.md). Kiến trúc ở [`docs/architecture.md`](../docs/architecture.md), kế hoạch ở [`docs/checkpoint3_plan.md`](../docs/checkpoint3_plan.md), roadmap ở [`docs/roadmap.md`](../docs/roadmap.md).

Dimension được chọn là **Taxi Zone Lookup CSV** từ trang TLC, lưu tại [`data/reference/taxi_zone_lookup.csv`](../data/reference/taxi_zone_lookup.csv). File có **12.331 bytes, 265 `LocationID` duy nhất**, SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`; URL và provenance tại [`metadata/taxi_zone_lookup.json`](../metadata/taxi_zone_lookup.json). Trước join, pipeline kiểm cột bắt buộc, key null và key trùng sau khi chuẩn hóa về số nguyên.

### 3.4. Checkpoint 3: pipeline xử lý thật

Lệnh end-to-end:

```powershell
./scripts/run.ps1 src/pipeline_tlc.py
```

[`src/pipeline_tlc.py`](../src/pipeline_tlc.py) dùng **đúng một manifest success**: `data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json`. Trước khi Spark đọc, code kiểm status/dataset/tên file/dung lượng/SHA-256. Nếu manifest chứa đường dẫn tuyệt đối cũ, code chỉ dùng file Parquet cùng thư mục manifest khi đúng size/hash; không sửa manifest.

Các phép xử lý Spark theo thứ tự:

1. **Select/cast** pickup, dropoff, `PULocationID`, `trip_distance`, `total_amount` để có hợp đồng cột rõ.
2. **Filter** pickup theo khoảng nửa mở `[2025-01-01 00:00:00, 2025-02-01 00:00:00)`. `timestamp_ntz` được giữ theo giá trị đồng hồ của nguồn; chưa xác nhận timezone, không tự chuyển UTC.
3. **Derive** ngày/giờ pickup và cờ lỗi dropoff, distance, amount. KPI nhu cầu chỉ cần pickup hợp lệ trong tháng; lỗi measure tùy chọn được báo riêng, không tự xóa chuyến.
4. **Left join** `PULocationID = LocationID` với lookup đã kiểm unique. Null/unmatched key sẽ giữ ở nhóm Unknown riêng. Borough đặc biệt có sẵn trong lookup như `Unknown`, `N/A`, `EWR` là **matched** và giữ nguyên nhãn, không nhầm với key join thất bại.
5. **Aggregate** theo `(pickup_date, pickup_hour, pickup_borough)`, tính `trip_count`, kiểm grain duy nhất và tổng số chuyến. Toàn bộ transform/join/aggregate chạy trong Spark. Gold nhỏ được PyArrow ghi ra Parquet vì Spark Hadoop writer trên Windows hiện thiếu `winutils.exe`; Spark đọc lại Gold để đối chiếu.

Output ở `data/processed/nyc_tlc_yellow/raw_<hash>_zone_<hash>_code_<hash>/gold.parquet`. Tên version gắn raw hash, lookup hash và code hash. Rerun ghi thay thế cùng file, **không append**. `data/raw/`, `data/processed/`, runtime và secret bị Git ignore; chỉ metadata/evidence gọn và preview được commit.

## 4. Kết quả đo trên full snapshot

Nguồn số chính: [`evidence/checkpoint3/run1.json`](../evidence/checkpoint3/run1.json), [`run2.json`](../evidence/checkpoint3/run2.json), [`rerun_comparison.json`](../evidence/checkpoint3/rerun_comparison.json). Không dùng fixture 1.000 dòng để suy ra KPI toàn tháng.

| Kiểm tra | Kết quả VERIFIED |
| --- | ---: |
| Raw input | 3.475.226 dòng |
| Pickup timestamp null | 0 |
| Pickup ngoài tháng 01/2025 | 22 |
| Pickup trong phạm vi | 3.475.204 |
| Lookup | 265 LocationID unique |
| Dòng sau left join | 3.475.204 |
| Key join matched | 3.475.204 |
| Key null/unmatched | 0 |
| Gold grain ngày × giờ × borough | 4.652 dòng |
| Tổng `trip_count` Gold | 3.475.204 |
| Dropoff null/đi trước pickup trong phạm vi | 124 |
| Distance âm/thiếu trong phạm vi | 0 |
| Amount âm/thiếu trong phạm vi | 63.037 |
| Duplicate toàn dòng | 0 |

Phương trình đối soát: `3.475.226 raw = 0 pickup null + 22 ngoài tháng + 3.475.204 trong tháng`. Join không tăng/giảm fact: `joined_rows = in_scope_rows`. Tổng KPI: `sum(trip_count) = 3.475.204`. Các cờ chất lượng có thể chồng lấp; không cộng chúng thành số dòng bị loại. KPI này không loại chuyến trong tháng vì amount/distance/dropoff bất thường, nên `valid_rows = in_scope_rows`, `rejected_rows = 0` theo quy tắc đã công bố.

Ví dụ từ [`preview.csv`](../evidence/checkpoint3/preview.csv): ngày **01/01/2025, giờ 00**, Manhattan có **6.560** bản ghi chuyến; Queens **486**. Đây là ví dụ một grain, không phải tổng tháng. File Gold trên máy được kiểm lại bằng PyArrow: **4.652 dòng, tổng 3.475.204**, SHA-256 `72850e09350553b3431befb4c4d1ca686d0f4cc36709de8714fb0bfb5df01aee`.

## 5. Baseline, chạy lại và kiểm thử

- **Baseline full run trên code cuối:** 24,322 giây; Spark 3.5.8, Python 3.12.14, `local[*]`, 15 raw partitions, 1 Gold partition. File [`plan.txt`](../evidence/checkpoint3/plan.txt) là `explain("formatted")`; chưa làm benchmark tối ưu.
- **Rerun cùng raw/lookup/code/config:** 23,733 giây, 4.652 dòng Gold, cùng schema/metrics và cùng SHA-256 Parquet. [`rerun_comparison.json`](../evidence/checkpoint3/rerun_comparison.json) ghi đối chiếu; không có duplicate append.
- **Kiểm thử tự động:** 8 test qua (5 ingest cũ + 3 nhóm test Checkpoint 3) trong [`tests.txt`](../evidence/checkpoint3/tests.txt). Test mới kiểm manifest status/size/hash, lookup duplicate kể cả `1` và `01`, biên tháng, null/unmatched key, join và grain trên fixture nhỏ. Test fixture không thay thế full run.
- **Lần chạy trực tiếp gần nhất:** lệnh `./scripts/run.ps1 src/pipeline_tlc.py` hoàn tất với exit code 0 sau **33,573 giây** trên cùng máy; đọc lại Gold được 4.652 dòng, `sum(trip_count)=3.475.204` và hash trên trùng baseline/rerun. Log cục bộ `.report-build/live-run.txt` bị Git ignore; bằng chứng full run/rerun đã commit vẫn nằm trong `evidence/checkpoint3/`. Runtime thay đổi giữa các lần chạy là bình thường, không dùng để tuyên bố tối ưu.

## 6. Sản phẩm bàn giao và Git

| Sản phẩm | Vị trí |
| --- | --- |
| Mã ingestion/raw inspection | `src/ingest_tlc.py`, `src/ingest_http.py`, `src/inspect_raw.py` |
| Mã pipeline Checkpoint 3 | `src/pipeline_tlc.py` |
| Lookup nhỏ có provenance | `data/reference/taxi_zone_lookup.csv`, `metadata/taxi_zone_lookup.json` |
| Báo cáo chi tiết Checkpoint 2/3 | `docs/checkpoint2_report.md`, `reports/checkpoint3.md` |
| Slide báo cáo Checkpoint 3 theo mẫu | `reports/Checkpoint3_Report.pptx` (6 trang); [`slide_validation.json`](../evidence/checkpoint3/slide_validation.json) ghi kiểm file thật |
| Sơ đồ và roadmap | `docs/architecture.md`, `docs/roadmap.md`, `.archify/.../tlc-checkpoint3.html` |
| Evidence gọn | `logs/`, `metadata/`, `evidence/checkpoint3/` |

Tag cục bộ `checkpoint-2` đánh dấu ingestion/raw; tag `checkpoint-3` trỏ tới commit `b426a25` chứa pipeline và evidence kỹ thuật. Sau đó commit `08843d3` bổ sung danh mục 22 tài liệu học phần. Repo chưa có remote/push theo README, nên không nhận là đã nộp lên hệ thống giảng viên.

## 7. Cách tái hiện khi giảng viên hỏi

Tại PowerShell trong thư mục repo:

```powershell
./scripts/setup.ps1
./scripts/run.ps1 src/env_check.py
./scripts/run.ps1 -Script '-m' -ScriptArgs @('unittest', 'discover', '-s', 'tests', '-v')
./scripts/run.ps1 src/pipeline_tlc.py
```

Máy chạy phải có raw Parquet của **một** manifest success đúng hash; raw 59 MB không nằm trong Git. Nếu clone trên máy khác, chạy ingestion hoặc chuyển snapshot đúng hash và manifest thích hợp. Không dùng wildcard nhiều run. Pipeline in JSON metrics và đường dẫn output; file `gold.parquet`, `evidence.json`, `plan.txt`, `preview.csv` nằm cùng version output. Sau khi chạy, đối chiếu `raw = pickup null + ngoài tháng + trong tháng`, `joined = trong tháng`, `sum(trip_count) = trong tháng`.

## 8. Dàn ý trình bày khoảng 4 phút

1. **Bài toán (30 giây):** nhóm muốn biết nhu cầu đón Yellow Taxi thay đổi theo ngày/giờ/borough; KPI là số bản ghi chuyến, không phải ML prediction.
2. **Nguồn và Checkpoint 2 (45 giây):** NYC 311 bị 403 nên dùng TLC Yellow Taxi 01/2025; raw 3.475.226 dòng, ba run độc lập cùng SHA-256; chọn một manifest để tránh đếm ba lần.
3. **Pipeline Checkpoint 3 (90 giây):** giải thích select/cast, lọc biên tháng, quality flags, left join với 265 LocationID unique, aggregate theo ngày × giờ × borough. Nêu chính sách giữ chuyến có amount/dropoff bất thường và phân biệt borough `Unknown` trong lookup với unmatched key.
4. **Bằng chứng (60 giây):** 22 chuyến ngoài tháng, 3.475.204 trong tháng, join không đổi row count, 4.652 nhóm Gold, tổng KPI khớp fact. Hai full runs cho cùng hash; 8 test qua, baseline và execution plan đã lưu.
5. **Giới hạn/tiếp theo (15 giây):** còn xác nhận proposal/KPI, timezone, thông tin nhóm và hướng dẫn nộp; chỉ benchmark performance khi có brief Buổi 9–10.

## 9. Phần còn chờ xác nhận — PENDING

- Proposal Đồ án 1 đã điền và feedback duyệt không có trong bộ 22 file; cần đối chiếu đề tài Yellow Taxi/câu hỏi nhu cầu và việc dùng Taxi Zone Lookup với giảng viên nếu phạm vi proposal khác.
- Tên nhóm, danh sách thành viên hiện tại, owner, deadline, nơi nộp và remote Git chưa được xác nhận trong repo. Slide Checkpoint 3 vẫn có chỗ chờ điền; không tự lấy tên từ báo cáo Checkpoint 2 làm danh sách nhóm hiện nay.
- Nguồn TLC đang đọc thành `timestamp_ntz`, chưa xác nhận quy ước múi giờ chính thức. Không gọi ngày/giờ Gold là giờ địa phương New York cho tới khi có căn cứ.
- Buổi 9–10/P4 chỉ được slide giới thiệu trước; chưa có brief/rubric riêng. Chưa train ML, triển khai HDFS/Kafka/cloud hay tối ưu Spark vì chưa thuộc yêu cầu hiện tại.
