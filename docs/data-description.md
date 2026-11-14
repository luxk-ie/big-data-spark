# Mô tả dữ liệu đang dùng — NYC TLC Yellow Taxi 01/2025

**Trạng thái:** **VERIFIED** đối với snapshot, schema, số dòng, số null, lookup và kết quả Checkpoint 3 nêu dưới đây. **PENDING** đối với quy ước múi giờ của timestamp nguồn và việc giảng viên chấp thuận câu hỏi/KPI cuối cùng. Tài liệu này mô tả dữ liệu **thực tế của project**; [`data_dictionary.md`](data_dictionary.md) vẫn chứa phần thiết kế NYC 311 cũ, không áp dụng cho pipeline taxi.

## 1. Dữ liệu dùng để trả lời câu hỏi gì?

Project dùng bản ghi **chuyến Yellow Taxi tại New York City** để trả lời câu hỏi: *Số chuyến đón khách thay đổi thế nào theo ngày, giờ và borough đón trong tháng 01/2025?* Một bản ghi raw là một chuyến được nguồn công bố; **không có trip ID duy nhất** trong 20 cột, vì vậy project không giả định có khóa nghiệp vụ cho từng chuyến. KPI `trip_count` là **số bản ghi chuyến**, không phải số hành khách, số xe, doanh thu hay số chuyến duy nhất sau khử trùng.

Ba tập dữ liệu liên quan:

| Tập | Vai trò và grain | Định dạng | Trạng thái |
| --- | --- | --- | --- |
| `yellow_tripdata_2025-01.parquet` | Fact raw; một dòng cho một bản ghi chuyến nguồn | Parquet, 20 cột | **VERIFIED**: 3.475.226 dòng, 59.158.238 byte |
| `taxi_zone_lookup.csv` | Dimension khu vực; một dòng cho một `LocationID` | CSV, 4 cột | **VERIFIED**: 265 mã duy nhất |
| `gold.parquet` | Kết quả nhu cầu đón; một dòng trên `(pickup_date, pickup_hour, pickup_borough)` | Parquet, 4 cột | **VERIFIED**: 4.652 nhóm, tổng `trip_count` = 3.475.204 |

TLC giải thích Yellow Taxi trip data gồm thời điểm đón/trả, khu vực, khoảng cách, biểu giá, thanh toán, các khoản phí và số hành khách do tài xế báo. Dữ liệu do các nhà cung cấp công nghệ gửi cho TLC; TLC **không bảo đảm độ chính xác tuyệt đối của từng bản ghi**. File được phát hành theo tháng, nhưng project vẫn lọc theo `tpep_pickup_datetime` vì trong file tháng 01 có 22 dòng nằm ngoài tháng phân tích. Nguồn: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), [Yellow Taxi Data Dictionary, 18/03/2025](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

## 2. Nguồn gốc và snapshot được chọn

- Nguồn file: [Yellow Taxi 01/2025 Parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet), được liên kết trên trang TLC ở trên.
- Raw snapshot dùng cho Checkpoint 3: `data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/yellow_tripdata_2025-01.parquet`.
- Manifest cùng run: `data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json`; `status=success`, `ingested_at_utc=2026-10-23T08:51:12.919770+00:00`.
- Raw SHA-256: `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`; dung lượng **59.158.238 byte**. Pipeline kiểm `status`, tên file, byte count và hash trước khi Spark đọc.
- Repo có nhiều run ingest thành công cùng checksum. **Mỗi lần phân tích chỉ chọn một manifest**; đọc wildcard nhiều run sẽ đếm lặp cùng snapshot. Raw, manifest lịch sử và file tải gốc không bị sửa để phục vụ Checkpoint 3.

Nguồn kiểm chứng nội bộ: `metadata/raw_inspection.json`, `logs/raw_inspection.txt`, `evidence/checkpoint3/run1.json` và `src/pipeline_tlc.py`. File raw lớn nằm ngoài Git; bản `data/sample/` 1.000 dòng chỉ dùng kiểm thử/smoke test, **không đại diện cho các thống kê toàn tháng**.

## 3. Data dictionary của 20 cột raw

Kiểu dưới đây là kiểu Spark đọc từ snapshot này (`logs/raw_inspection.txt`). Tất cả cột được Spark đánh dấu `nullable=true`; cột có `0` ở bảng vẫn có thể có null ở snapshot khác. Số null **VERIFIED** được cộng từ `null_count` của cả 4 Parquet row group trong đúng file raw đã chọn; đây là null ở raw, không phải số dòng bị pipeline loại. Ý nghĩa cột được diễn giải từ [data dictionary chính thức của TLC](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

| Cột raw | Kiểu Spark | Ý nghĩa và cách dùng trong project | Null raw |
| --- | --- | --- | ---: |
| `VendorID` | `integer` | Mã nhà cung cấp TPEP gửi bản ghi; không dùng trong KPI hiện tại. | 0 |
| `tpep_pickup_datetime` | `timestamp_ntz` | Thời điểm đồng hồ đo bắt đầu; dùng lọc tháng, tạo ngày và giờ đón. | 0 |
| `tpep_dropoff_datetime` | `timestamp_ntz` | Thời điểm đồng hồ đo kết thúc; dùng cờ `invalid_dropoff`. | 0 |
| `passenger_count` | `long` | Số hành khách do tài xế báo; **không** dùng thay `trip_count`. | 540.149 |
| `trip_distance` | `double` | Khoảng cách theo đồng hồ đo, đơn vị **mile**; dùng cờ `invalid_distance`, chưa làm KPI. | 0 |
| `RatecodeID` | `long` | Mã biểu giá có hiệu lực lúc kết thúc chuyến; chưa dùng trong KPI. | 540.149 |
| `store_and_fwd_flag` | `string` | `Y` nếu bản ghi được lưu trên xe rồi gửi sau vì mất kết nối; `N` nếu không. | 540.149 |
| `PULocationID` | `integer` | Mã Taxi Zone nơi bắt đầu chuyến; khóa trái để join `LocationID`. | 0 |
| `DOLocationID` | `integer` | Mã Taxi Zone nơi kết thúc chuyến; chưa dùng trong KPI pickup. | 0 |
| `payment_type` | `long` | Mã hình thức thanh toán; chưa dùng trong KPI. | 0 |
| `fare_amount` | `double` | Cước theo thời gian và quãng đường do đồng hồ tính. | 0 |
| `extra` | `double` | Các khoản phụ thu/extra khác. | 0 |
| `mta_tax` | `double` | Thuế MTA phát sinh theo biểu giá áp dụng. | 0 |
| `tip_amount` | `double` | Tip ghi qua thẻ; **không bao gồm tip tiền mặt**. | 0 |
| `tolls_amount` | `double` | Tổng phí cầu đường của chuyến. | 0 |
| `improvement_surcharge` | `double` | Phụ phí cải thiện dịch vụ áp trên chuyến theo quy định TLC. | 0 |
| `total_amount` | `double` | Tổng tiền tính cho khách, **không gồm tip tiền mặt**; dùng cờ `invalid_amount`, chưa gọi là doanh thu. | 0 |
| `congestion_surcharge` | `double` | Khoản NYS congestion surcharge thu trong chuyến. | 540.149 |
| `Airport_fee` | `double` | Phí đón tại LaGuardia/JFK theo định nghĩa TLC. **Giữ nguyên chữ hoa `A` của schema raw**. | 540.149 |
| `cbd_congestion_fee` | `double` | Phí mỗi chuyến cho MTA Congestion Relief Zone; TLC cho biết cột này có từ dữ liệu 2025 và khoản phí bắt đầu 05/01/2025. | 0 |

Mã tham chiếu theo TLC: `RatecodeID` gồm 1 standard, 2 JFK, 3 Newark, 4 Nassau/Westchester, 5 negotiated, 6 group ride, 99 null/unknown. `payment_type` gồm 0 flex fare, 1 thẻ tín dụng, 2 tiền mặt, 3 không thu, 4 tranh chấp, 5 không rõ, 6 voided trip. Đây là **ý nghĩa mã của nguồn**, không phải phân bố đã đo trong project; không suy diễn rằng mọi mã đều xuất hiện trong snapshot này.

**Lưu ý thời gian:** `timestamp_ntz` không mang offset/múi giờ. Pipeline giữ nguyên giá trị giờ nguồn và đặt Spark session timezone `UTC` để xử lý nhất quán; thao tác này **không chứng minh** timestamp nguồn là UTC hay giờ địa phương New York. **PENDING:** xác nhận quy ước timezone của TLC trước khi diễn giải kết quả theo giờ địa phương. Không tự chuyển UTC sang New York.

## 4. Taxi Zone Lookup và phép join

TLC liên kết [Taxi Zone Lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) trên [trang Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Snapshot project là `data/reference/taxi_zone_lookup.csv`, **12.331 byte**, SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`; provenance ở `metadata/taxi_zone_lookup.json`.

| Cột lookup | Ý nghĩa | Dùng trong Checkpoint 3 |
| --- | --- | --- |
| `LocationID` | Mã Taxi Zone; trong snapshot có 265 giá trị khác nhau, không null/trùng. | Khóa phải của join, cast sang `int`. |
| `Borough` | Borough hoặc nhãn đặc biệt do TLC cung cấp (`Unknown`, `N/A`, `EWR`). | Tạo `pickup_borough` sau join. |
| `Zone` | Tên khu vực/zone chi tiết. | Giữ trong nguồn tham chiếu, chưa đưa vào Gold. |
| `service_zone` | Phân nhóm service zone có sẵn trong CSV. | Chưa dùng trong KPI. |

Pipeline dùng **left join** `PULocationID = LocationID`, kiểm `LocationID` không null và unique trước join. Nếu khóa trái null hoặc không khớp, fact vẫn được giữ và gắn nhãn riêng `Unknown (null key)`/`Unknown (unmatched key)`; không đồng nhất các trường hợp đó với nhãn `Unknown` **đã có trong lookup**. Full run đo được **3.475.204 matched**, **0 null/unmatched key** và **3.475.204 dòng sau join**: join không làm tăng hoặc mất fact trong snapshot này.

## 5. Phạm vi phân tích, quy tắc chất lượng và Gold

Pipeline chọn 5 cột cần xử lý từ raw (`tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `trip_distance`, `total_amount`), cast kiểu, rồi giữ pickup trong khoảng nửa mở **`[2025-01-01 00:00:00, 2025-02-01 00:00:00)`** theo timestamp nguồn. Sau đó tạo `pickup_date`, `pickup_hour`, cờ chất lượng, join zone và `groupBy` ba khóa để `count(*) AS trip_count`.

| Kiểm tra trên full snapshot | Số dòng **VERIFIED** | Quy tắc |
| --- | ---: | --- |
| Raw input | 3.475.226 | Một manifest success. |
| Pickup null | 0 | Không vào KPI vì thiếu ngày/giờ đón. |
| Pickup ngoài tháng phân tích | 22 | Không vào KPI; không xóa khỏi raw. |
| Pickup trong phạm vi / `valid_rows` | 3.475.204 | Input của join và KPI. |
| Dropoff thiếu hoặc sớm hơn pickup | 124 | Cờ độc lập; chuyến vẫn được đếm. |
| Distance null hoặc âm | 0 | Cờ độc lập; chưa dùng distance làm KPI. |
| `total_amount` null hoặc âm | 63.037 | Cờ độc lập; không tự sửa số tiền. |
| Duplicate toàn bộ 20 cột raw | 0 | Được đo; pipeline không tự khử trùng theo khóa giả định. |
| Joined rows / matched zone | 3.475.204 / 3.475.204 | Bảo toàn fact. |
| Gold rows / tổng `trip_count` | 4.652 / 3.475.204 | Mỗi tổ hợp ngày × giờ × borough một dòng. |

Đối soát: **3.475.226 = 0 + 22 + 3.475.204**; `joined_rows = in_scope_rows`; `sum(trip_count) = in_scope_rows`. Các cờ dropoff/distance/amount **có thể chồng lấp**, nên không cộng chúng thành số dòng loại. Vì câu hỏi chỉ đếm nhu cầu theo pickup, `rejected_rows = 0` trong tập đã vào phạm vi. Nguồn: `evidence/checkpoint3/run1.json`; số liệu này là full run, không lấy từ fixture.

Gold có schema: `pickup_date: date`, `pickup_hour: integer` (0–23 theo giá trị nguồn), `pickup_borough: string`, `trip_count: long`. `trip_count` là **số chuyến theo nhóm**, không phải số khách. Output ở `data/processed/nyc_tlc_yellow/raw_9af277e4c0d3_zone_1a99e1050922_code_c27cfdf95d17/gold.parquet`, cùng `evidence.json`, `plan.txt`, `preview.csv`. File Gold hiện tại có SHA-256 `72850e09350553b3431befb4c4d1ca686d0f4cc36709de8714fb0bfb5df01aee`; full run và rerun lưu trong `evidence/checkpoint3/rerun_comparison.json` cho cùng hash. Gold được Spark aggregate; PyArrow ghi Parquet nhỏ trên Windows, rồi Spark đọc lại kiểm số dòng.

## 6. Giới hạn diễn giải và cách kiểm tra

- **PENDING:** timezone nguồn; chưa gọi `pickup_hour` là giờ địa phương New York được xác nhận.
- **PENDING:** nhóm/giảng viên chốt đề tài, KPI cuối và việc dùng Taxi Zone Lookup làm dataset join cho rubric. Tài liệu đang mô tả pipeline đã chạy, không tự xem proposal NYC 311 cũ là đề tài đã duyệt.
- Không dùng `total_amount` làm doanh thu thuần hay `tip_amount` làm tổng mọi tip; không suy ra lợi nhuận, số xe, số khách hoặc nhu cầu thực tế ngoài phạm vi báo cáo nguồn.
- TLC cảnh báo dữ liệu do nhà cung cấp gửi và có thể sai/thiếu. Kiểm tra của project chỉ chứng minh những điều kiện đã đo trên snapshot này; nó không xác nhận tính đúng đắn của mọi chuyến ngoài đời.
- Output là **một tháng** và **một snapshot**. Thêm tháng hoặc đổi bản TLC cần đọc lại schema, hash, quality và cập nhật tài liệu; không ghép các ingest run trùng nội dung.

Chạy lại trên máy đã setup và có raw snapshot đúng hash:

```powershell
./scripts/run.ps1 src/pipeline_tlc.py
```

Lệnh in đường dẫn output, metrics và thời gian. Mở `preview.csv` để xem vài nhóm, `evidence.json` để đối chiếu số dòng, `plan.txt` để xem `explain("formatted")`. Bằng chứng lịch sử gọn nằm trong `evidence/checkpoint3/`; không sửa các file đó để khớp một lần chạy mới.

**Nguồn chính:** [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) · [Yellow Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) · manifest/raw/schema/lookup/`run1.json` trong repo nêu ở từng mục.
