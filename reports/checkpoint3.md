# Báo cáo Checkpoint 3 — Spark Processing Pipeline

Trạng thái: **VERIFIED** cho full run ngày 10/11/2026; rerun và các hạng mục trình bày xem bảng cuối. Nguồn yêu cầu giảng viên và vị trí slide/brief: `docs/checkpoint3_requirements.md`.

## Câu hỏi, input và grain

**Câu hỏi:** Nhu cầu đón khách Yellow Taxi tháng 01/2025 thay đổi theo ngày, giờ và borough đón như thế nào? KPI chính `trip_count` là số bản ghi chuyến, grain output duy nhất `(pickup_date, pickup_hour, pickup_borough)`. Không diễn giải đây là số hành khách hay doanh thu.

Input raw: một manifest `success` tại `data/raw/nyc_tlc_yellow/ingest_date=2026-10-23/run_20261023T085112919770Z_3632e145/manifest.json`. SHA-256 `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`, 59.158.238 bytes. Pipeline xác minh status, dataset, filename, bytes và SHA-256 trước khi Spark đọc. Không đọc wildcard ba run cùng snapshot.

Dimension: [Taxi Zone Lookup CSV do TLC liên kết](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), bản sao `data/reference/taxi_zone_lookup.csv`, SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`, 12.331 bytes, 265 mã `LocationID` duy nhất. `metadata/taxi_zone_lookup.json` ghi provenance. Lookup được kiểm header, mã null và duplicate trước join.

## Pipeline và quy tắc dữ liệu

Lệnh end-to-end:

```powershell
./scripts/run.ps1 src/pipeline_tlc.py
```

Các transformation Spark: (1) chọn/cast pickup, dropoff, location ID, distance, amount để giữ hợp đồng cột; (2) lọc pickup trong khoảng nửa mở `[2025-01-01, 2025-02-01)`; (3) derive ngày/giờ pickup và cờ chất lượng; (4) left join `PULocationID = LocationID`; (5) groupBy ngày × giờ × borough để đếm chuyến. Pickup là `timestamp_ntz`, giữ nguyên giờ đồng hồ nguồn; chưa có bằng chứng xác nhận múi giờ. Không tự chuyển UTC.

Không có trip ID đáng tin để khử trùng chuyến, nên đo duplicate toàn dòng mà không xóa. Dropoff sai hoặc thiếu, distance âm/thiếu và amount âm/thiếu là cờ chất lượng độc lập; chúng không loại chuyến khỏi KPI nhu cầu nếu pickup hợp lệ. Pickup ngoài tháng và pickup null không vào KPI. Key null/unmatched giữ trong nhóm Unknown riêng; borough có trong lookup, kể cả giá trị đặc biệt, giữ nguyên.

Gold được Spark aggregate; PyArrow ghi một file Parquet nhỏ do Spark Hadoop writer trên Windows hiện thiếu `winutils.exe`. Spark đọc lại để xác minh số dòng. Output có version gồm raw hash, lookup hash và code hash; rerun ghi đè đúng file `gold.parquet`, không append. File dữ liệu xử lý lớn được Git ignore.

## Kết quả full run — VERIFIED

Nguồn số: `evidence/checkpoint3/run1.json`; đây là số đo trên full snapshot, không suy ra từ fixture 1.000 dòng.

| Chỉ số | Giá trị |
| --- | ---: |
| Raw input | 3.475.226 |
| Pickup timestamp null | 0 |
| Pickup ngoài tháng | 22 |
| Pickup trong tháng | 3.475.204 |
| Joined rows | 3.475.204 |
| Matched zone | 3.475.204 |
| Null/unmatched zone | 0 |
| Gold rows | 4.652 |
| Tổng `trip_count` | 3.475.204 |
| Dropoff thiếu/đi trước pickup trong phạm vi | 124 |
| Distance âm/thiếu trong phạm vi | 0 |
| Amount âm/thiếu trong phạm vi | 63.037 |
| Duplicate toàn dòng | 0 |

Đối soát: `3.475.226 = 0 + 22 + 3.475.204`; `joined_rows = in_scope_rows`; `sum(trip_count) = in_scope_rows`. Không có dòng in-scope bị loại vì KPI chỉ dùng pickup, nên `valid_rows = 3.475.204`, `rejected_rows = 0`. Các cờ chất lượng có thể chồng lấp.

Baseline full run trên code cuối: 24,322 giây; Spark 3.5.8, Python 3.12.14, `local[*]`, 15 raw partitions, 1 Gold partition. Execution plan định dạng formatted: `evidence/checkpoint3/plan.txt`. Đây là baseline máy hiện tại, không hứa hẹn runtime trên máy khác. Gold preview: `evidence/checkpoint3/preview.csv`.

## Đối chiếu yêu cầu

| Mã | Trạng thái | Bằng chứng |
| --- | --- | --- |
| D1–D5 | VERIFIED | `src/pipeline_tlc.py`, full-run metrics và tests; join/aggregate/quality đối soát ở trên |
| D6 | VERIFIED | runtime, bytes, environment, partitions và formatted plan trong evidence |
| D7 | PENDING | README, code, báo cáo, preview, 6 slide và commit/tag cục bộ `checkpoint-3` đã có; `evidence/checkpoint3/slide_validation.json` kiểm file PPTX thật. Thông tin nhóm và nộp bài còn chờ |
| R1 | VERIFIED | `evidence/checkpoint3/rerun_comparison.json`: cùng raw/lookup/code/schema/metrics, SHA-256 Gold bằng nhau, 4.652 dòng sau rerun |
| T1 | PENDING | Nhóm cần tự giải thích grain, join, KPI và baseline khi trình bày |

## Giới hạn và việc tiếp theo

- Chưa xác nhận timezone chính thức của pickup timestamp; biểu diễn ngày/giờ theo giá trị nguồn.
- Câu hỏi kinh doanh và việc dùng lookup làm dataset join cần đối chiếu proposal/giảng viên nếu có thay đổi.
- Chưa tối ưu partition, broadcast, cache hoặc skew; đó là phần Buổi 9–10 theo tài liệu.
- Slide theo mẫu đã có 6 trang; thành viên, owner, deadline và remote nộp bài còn chờ thông tin nhóm.

Rerun full snapshot thứ hai trên code cuối mất 23,733 giây; cùng output version. Hash file Gold hai lần đều là `72850e09350553b3431befb4c4d1ca686d0f4cc36709de8714fb0bfb5df01aee`. Số dòng sau lần hai vẫn 4.652; file được ghi thay thế, không append. `run1.json`, `run2.json` và `rerun_comparison.json` lưu riêng để đối chiếu.
