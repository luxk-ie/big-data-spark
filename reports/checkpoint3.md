# Checkpoint 3 - Spark Processing Pipeline

**Trạng thái:** **VERIFIED** cho full run và rerun trên Yellow Taxi 01/2025. Các số trong báo cáo lấy từ `evidence/checkpoint3/run1.json`, `run2.json` và `rerun_comparison.json`, không lấy từ fixture 1.000 dòng. Nguồn yêu cầu giảng viên theo brief/mục/slide: [`docs/checkpoint3_requirements.md`](../docs/checkpoint3_requirements.md). Câu hỏi phân tích đang dùng: **số chuyến đón khách thay đổi thế nào theo ngày, giờ và borough đón trong tháng 01/2025?**

## 1. Pipeline architecture

`Raw Parquet + manifest` → `Read/Schema` → `Select/Cast + Filter` → `Left Join Taxi Zone` → `Quality + đối soát` → `Aggregate` → `Gold Parquet + preview`.

- **Input fact:** một snapshot raw Yellow Taxi 01/2025, xác minh manifest `success`, dung lượng và SHA-256 trước khi Spark đọc. Không đọc wildcard nhiều run ingest vì chúng trùng nội dung.
- **Input dimension:** Taxi Zone Lookup CSV do [TLC liên kết](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), bản sao tại `data/reference/taxi_zone_lookup.csv` và provenance tại `metadata/taxi_zone_lookup.json`.
- **Processing:** PySpark 3.5.8, `local[*]`; các phép biến đổi, join và aggregate chạy trong `src/pipeline_tlc.py`.
- **Output:** một version theo raw hash, lookup hash và code hash; gồm `gold.parquet`, `evidence.json`, `plan.txt`, `preview.csv`. Quality được đo trước/sau join và kiểm lại với tổng Gold.

Chạy từ thư mục gốc repo:

```powershell
./scripts/run.ps1 src/pipeline_tlc.py
```

## 2. Input contract

| Thuộc tính | Yellow Taxi fact | Taxi Zone Lookup dimension |
| --- | --- | --- |
| Dataset | `nyc_tlc_yellow`, file `yellow_tripdata_2025-01.parquet` | `taxi_zone_lookup.csv` |
| Grain | Một bản ghi chuyến theo nguồn | Một dòng cho một `LocationID` |
| Key(s) | Không có trip ID duy nhất; `PULocationID` là khóa join, không phải khóa chuyến | `LocationID` không null và unique |
| Row count **VERIFIED** | 3.475.226 dòng raw / 1 file / 20 cột | 265 dòng / 4 cột / 265 mã unique |
| Size | 59.158.238 byte | 12.331 byte |
| SHA-256 | `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a` | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` |

Manifest được chọn: `data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json`. Pickup/dropoff được Spark đọc là `timestamp_ntz`; `PULocationID` là integer. **PENDING:** xác nhận quy ước timezone của nguồn, vì timestamp không mang offset. Pipeline giữ nguyên giá trị đồng hồ nguồn, không tự chuyển UTC. Data dictionary đầy đủ: [`docs/data-description.md`](../docs/data-description.md).

## 3. Transformations

1. **Select/cast:** chọn `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `trip_distance`, `total_amount`; cast thành `timestamp_ntz`, `int` và `double` để hợp đồng cột rõ ràng.
2. **Filter phạm vi:** chỉ giữ pickup trong `[2025-01-01 00:00:00, 2025-02-01 00:00:00)`. Full run: 3.475.226 raw = **0 pickup null + 22 ngoài tháng + 3.475.204 trong tháng**.
3. **Derive và đánh dấu chất lượng:** tạo `pickup_date`, `pickup_hour`; đánh dấu `invalid_dropoff` khi dropoff null hoặc trước pickup, `invalid_distance` khi distance null/âm, `invalid_amount` khi amount null/âm. Full run trong phạm vi: **124** dropoff lỗi, **0** distance lỗi, **63.037** amount lỗi. Các cờ có thể chồng lấp và **không loại chuyến** khỏi KPI đếm pickup.

Không có business key đáng tin để khử trùng chuyến. Pipeline đo **0 duplicate toàn bộ dòng raw** và không tự `dropDuplicates` theo pickup time/zone. `valid_rows = 3.475.204`, `rejected_rows = 0` trong phạm vi phân tích. Nguồn đo: `evidence/checkpoint3/run1.json`; quy tắc: `src/pipeline_tlc.py`.

## 4. Join validation

- **Join type/key:** left join `PULocationID = LocationID`; nhiều chuyến có thể thuộc một zone.
- **Rows before/after:** fact trong phạm vi **3.475.204**; lookup **265**; sau join **3.475.204**. Join không làm nhân bản hoặc mất fact.
- **Duplicate keys:** `LocationID` bên lookup được kiểm unique và không null trước join; **0 khóa trùng** trong snapshot này. Duplicate toàn dòng fact: **0**, nhưng đây không phải kiểm khóa chuyến.
- **Unmatched:** **0** key null/không khớp; **3.475.204 matched**. Nếu một run khác có key null/unmatched, left join vẫn giữ fact và gắn nhãn `Unknown (null key)` hoặc `Unknown (unmatched key)`. Các nhãn `Unknown`, `N/A`, `EWR` vốn có trong lookup vẫn được xem là **matched**, không gộp với unmatched.

Đối soát `joined_rows = in_scope_rows = 3.475.204`. Source: `evidence/checkpoint3/run1.json`; ảnh trích từ file evidence nằm ở slide 4 của `reports/Checkpoint3_Report.pptx`.

## 5. Aggregation & output

- **Output grain:** một dòng cho mỗi tổ hợp `(pickup_date, pickup_hour, pickup_borough)`.
- **KPI/metrics:** `trip_count = count(*)`, là **số bản ghi chuyến**, không phải số hành khách hoặc doanh thu. Full run có **4.652 nhóm Gold**, `sum(trip_count) = 3.475.204 = in_scope_rows`; key nhóm được kiểm unique.
- **Schema:** `pickup_date: date`, `pickup_hour: integer`, `pickup_borough: string`, `trip_count: long`.
- **Output path/format:** `data/processed/nyc_tlc_yellow/raw_9af277e4c0d3_zone_1a99e1050922_code_c27cfdf95d17/gold.parquet` (Parquet). Cùng thư mục có `preview.csv`, `evidence.json`, `plan.txt`. Preview gọn được lưu thêm ở `evidence/checkpoint3/preview.csv`.

Spark thực hiện aggregate. Trên Windows hiện tại, PyArrow ghi file Gold nhỏ vì Spark Hadoop writer cần `winutils.exe`; Spark đọc lại để kiểm số dòng. Rerun cùng input/code ghi thay thế đúng output version, không append. Hai lần chạy có cùng SHA-256 Gold: `72850e09350553b3431befb4c4d1ca686d0f4cc36709de8714fb0bfb5df01aee` (`evidence/checkpoint3/rerun_comparison.json`). Raw và manifest lịch sử không bị ghi đè.

## 6. Baseline

| Chỉ số | Full run **VERIFIED** | Rerun cùng input/code |
| --- | ---: | ---: |
| Runtime trên máy này | 24,322 giây | 23,733 giây |
| Input raw | 59.158.238 byte / 1 Parquet | Cùng snapshot/hash |
| Partitions observed | raw **15**, Gold **1** | Cùng output logic |
| Environment | Python 3.12.14, Spark 3.5.8, `local[*]` | Cùng code/config |

`explain("formatted")` đã lưu ở `evidence/checkpoint3/plan.txt` và cạnh Gold tại `data/processed/.../plan.txt`. Plan có `BroadcastHashJoin LeftOuter`, `HashAggregate` và `Exchange`; đây là **baseline trước tối ưu**, chưa có thí nghiệm tối ưu partition/broadcast/cache/skew. Runtime có thể khác trên máy khác hoặc lần chạy khác; chỉ so sánh khi cùng input, code, cấu hình và môi trường. Evidence: `run1.json`, `run2.json`, `rerun_comparison.json`; full run và rerun cùng metrics, schema, hash Gold.

## 7. Blocker & next action

- **Blocker:** không có **BLOCKED** về kỹ thuật cho pipeline Checkpoint 3. **PENDING** xác nhận timezone nguồn; xác nhận câu hỏi/KPI và việc dùng lookup làm dataset join với proposal/giảng viên; điền thông tin nhóm và nơi nộp. D7 còn **PENDING** khâu bàn giao; T1 còn **PENDING** phần trình bày.
- **Next action — PROPOSED, chờ brief Buổi 9–10:** (1) benchmark trên cùng snapshot và thử tối ưu partition/shuffle, đối chiếu runtime, execution plan và Gold với baseline; (2) mở rộng dữ liệu TLC sang nhiều tháng, kiểm tra schema và chất lượng từng tháng trước khi gộp; (3) nếu phạm vi cho phép, làm dashboard nhỏ để xem nhu cầu theo giờ và borough từ Gold. Đây là hướng phát triển project, chưa phải hạng mục Đồ án 3 đã được giao hoặc nghiệm thu.

Nếu cần chạy lại để trình diễn, dùng lệnh ở mục 1 rồi mở `preview.csv` và `evidence.json` trong đường dẫn `output` lệnh in ra. Khi chuyển repo sang máy khác, cần setup môi trường và ingest/chuyển đúng raw snapshot vì manifest cũ ghi `destination` tuyệt đối của máy tạo.
