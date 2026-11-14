# Kế hoạch Đồ án 3 — Spark Processing Pipeline

Trạng thái: pipeline **VERIFIED** trên full run và rerun; xem `reports/checkpoint3.md`. Yêu cầu giảng viên đã được đọc từ tài liệu gốc; xem bảng nguồn/mục/slide tại `docs/checkpoint3_requirements.md`.

## Mục tiêu và phạm vi

Từ đúng một snapshot raw Yellow Taxi của Checkpoint 2, chạy một lệnh PySpark để trả lời câu hỏi đề xuất: **số chuyến đón khách thay đổi thế nào theo ngày, giờ và borough trong tháng 01/2025?** Output chính có grain ngày × giờ × borough đón và KPI `trip_count`. Zone Lookup, quality evidence và baseline phục vụ kiểm chứng. Các chỉ số khoảng cách/tiền là tùy chọn sau khi quy tắc dữ liệu được chốt. Đồ án 3 yêu cầu tối thiểu ba transformations, một join, một aggregation, output và bằng chứng rerun. Chưa cần tối ưu partition/broadcast/cache/skew.

## Lộ trình triển khai có thể kiểm chứng

| Bước | Việc cần làm | Điều kiện hoàn thành |
| --- | --- | --- |
| 1. Chốt input | Xác minh manifest success và hash; tải Taxi Zone Lookup từ TLC, lưu URL/hash/snapshot; kiểm schema, grain, `LocationID` unique | Có input contract và lệnh kiểm tra trên dữ liệu thật |
| 2. Skeleton | Thêm `src/pipeline_tlc.py`, module quality nhỏ nếu cần, entry point PowerShell và README lệnh chạy | Chạy một lệnh nhận manifest + lookup path, báo lỗi rõ nếu thiếu input |
| 3. Transform | Chọn/cast cột; kiểm pickup/dropoff, distance, total; derive pickup_date/hour và cờ chất lượng; không loại chuyến do measure tùy chọn | Ít nhất 3 phép biến đổi có lý do; đối soát raw và in-scope |
| 4. Join | Left join `PULocationID` với zone `LocationID`; kiểm null/trùng dimension, matched/unmatched, số dòng trước/sau | Không nhân bản fact, unmatched có policy rõ |
| 5. Aggregate/output | `groupBy(pickup_date, pickup_hour, borough)` với `trip_count` là KPI chính; Parquet + preview nhỏ. Chỉ số tiền/khoảng cách là tùy chọn | Grain output duy nhất, số chuyến cộng lại khớp số fact đủ điều kiện, output đọc lại được |
| 6. Evidence/baseline | Log schema/counts, quality, sample KPI, `explain("formatted")`, elapsed time, bytes/files/partitions và phiên bản môi trường; chạy lần hai cùng input | Output logic tương đương, không append; báo cáo `reports/checkpoint3.md` theo tên trong StarterKit, chỉ có số đo thật |
| 7. Nộp | Cập nhật README, slide theo mẫu, commit/tag sau khi chạy thật; chuẩn bị pitch 4 phút | Người khác có thể làm theo hướng dẫn và giải thích join/grain/KPI |

## Các quyết định cần kiểm chứng khi viết code

- Dữ liệu chuyến không có business key duy nhất trong schema 20 cột: đo duplicate toàn dòng và báo cáo; không tự `dropDuplicates` theo pickup time + zone vì có thể xóa chuyến hợp lệ.
- Mốc thời gian pickup có thể nằm ngoài tháng tên file; xác định quy tắc filter theo câu hỏi phân tích, giữ count bị loại. Không tự sửa timestamp nguồn.
- Spark báo `timestamp_ntz` cho hai cột pickup/dropoff. **PENDING**: xác nhận timezone nguồn. Ranh giới phân tích đề xuất `[2025-01-01, 2025-02-01)` theo pickup; thống kê số dòng ngoài khoảng và không âm thầm bỏ chúng.
- `total_amount` âm có thể là điều chỉnh/hoàn tiền; báo count và quyết định KPI tiền có bao gồm chúng hay không. Không thay âm bằng 0 im lặng.
- Nếu zone lookup có `Unknown`/mã đặc biệt, phân biệt mã có trong lookup với key không khớp. Không đổi unmatched thành một borough thật.
- Full run 3.475.226 dòng mới là evidence nghiệm thu; sample 1.000 dòng chỉ dùng unit/smoke test, không dùng để báo kết quả toàn tháng.
- Các quality flag (null, timestamp, khoảng cách, tiền, key) có thể chồng lấp. Đối soát: `raw_input_rows = invalid_pickup_timestamp_rows + out_of_scope_rows + in_scope_rows`; `in_scope_rows = valid_rows + rejected_rows`. KPI nhu cầu chỉ loại dòng không có pickup timestamp hoặc ngoài tháng, vì vậy `valid_rows = in_scope_rows`, `rejected_rows = 0`. Lỗi ở các trường tùy chọn được báo riêng nhưng không loại chuyến. Duplicate toàn dòng được đo, không tự xóa. Dòng ngoài tháng chỉ thuộc `out_of_scope_rows`.

## Kiểm thử và bằng chứng tối thiểu

- Fixture nhỏ kiểm null/invalid, dimension duplicate, unmatched và row multiplication; kiểm output grain/KPI bằng số tự tính được.
- Full run chọn một manifest; ghi rows trước/sau mỗi filter, join rows, unmatched, output rows, explain, runtime và hashes.
- Rerun cùng input: so schema, row counts và KPI sau khi sắp xếp ổn định; runtime không cần giống nhau.
- Không tạo báo cáo/slide PASS từ output mẫu hoặc số liệu giả.

## Gate nghiệm thu trước khi đánh dấu VERIFIED

- Một lệnh chạy trên một manifest success và lookup có hash; cả hai checksum khớp nguồn đã ghi.
- `raw_input = out_of_scope + in_scope` và `in_scope = valid + rejected`; dimension `LocationID` unique; join không tăng số fact; matched/unmatched có số đo và policy.
- Gold có grain ngày × giờ × borough, `sum(trip_count)` bằng số fact đủ điều kiện và đọc lại được.
- Hai lần chạy cùng raw/lookup/code/config cho schema, counts và KPI tương đương; không append.
- Có runtime, input bytes, partitions, environment, formatted plan và commit gắn với full run.
- `README`, báo cáo, slide và commit chỉ ghi số liệu đã chạy; từng thành viên giải thích được phần việc.

## Chỗ cần nhóm/giảng viên xác nhận

1. Câu hỏi kinh doanh cuối: ưu tiên nhu cầu theo giờ và borough, hay KPI khác từ proposal đã được duyệt? Proposal NYC 311 cũ không còn khớp nguồn TLC.
2. Tên nhóm, thành viên, người phụ trách, deadline và nơi nộp/remote Git.
3. Có được dùng Taxi Zone Lookup làm dataset thứ hai cho join không? Đây là lựa chọn tự nhiên theo mã `PULocationID`, nhưng cần đối chiếu cách giảng viên chấm yêu cầu join.
