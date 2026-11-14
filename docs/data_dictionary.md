# Data dictionary dự kiến — NYC 311

> Đây là thiết kế NYC 311 ban đầu. Dataset thực tế của Checkpoint 2 đã đổi sang TLC vì HTTP 403; schema TLC thực tế nằm tại logs/raw_inspection.txt và metadata/raw_inspection.json. Không áp các tên cột/rule NYC 311 dưới đây lên TLC.

## Schema TLC đang sử dụng

Spark đã đọc 20 cột trong snapshot tháng 01/2025. `VendorID` là nhà cung cấp, `tpep_pickup_datetime`/`tpep_dropoff_datetime` là thời điểm đón/trả, `passenger_count` là số khách, `trip_distance` là khoảng cách chuyến, `RatecodeID` là mã biểu giá, `store_and_fwd_flag` là cờ lưu/chuyển tiếp, `PULocationID`/`DOLocationID` là mã khu vực đón/trả, `payment_type` là loại thanh toán. Các cột tiền: `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee`.

Schema và mẫu được lưu tại logs/raw_inspection.txt. Không thay đổi tên, kiểu hoặc giá trị raw trong Checkpoint 2. Định nghĩa mã, đơn vị và giới hạn diễn giải cho bước xử lý sau phải đối chiếu [data dictionary chính thức](https://home4.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf). Chưa triển khai cleaning hoặc suy luận chất lượng chỉ dựa vào tên cột.

Tài liệu thiết kế ngày 23/10/2026; chưa thay thế việc kiểm tra schema file tải thực tế. Nguồn: [NYC 311](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data), [thay đổi nhãn cột](https://www.nyc.gov/opendata/news/all-news/311-Service-Requests-Updates).

## Các trường phân tích

- `unique_key`: khóa yêu cầu dự kiến từ API; Silver giữ dưới dạng string. Không ép số để tránh mất định dạng. Null/blank cách ly khỏi tập phân tích chính.
- `created_date`: ngày tạo; parse theo định dạng thực tế đã kiểm tra. Null hoặc không parse được cách ly khỏi Silver chính.
- `closed_date`: ngày đóng; null được phép và không đồng nghĩa thời lượng 0. Parse lỗi phải được thống kê riêng.
- `complaint_type`: tên backend dự kiến cho nhãn hiển thị Problem; chuẩn hóa thành `problem` tại Silver. Null giữ nhóm Unknown để tránh mất số đếm.
- `descriptor`: tên backend dự kiến cho Problem Detail; chuẩn hóa thành `problem_detail`. Null được phép.
- `agency`: cơ quan tiếp nhận/xử lý; giữ nhóm Unknown nếu thiếu. Không suy ra chất lượng cơ quan chỉ từ thời gian đóng.
- `status`: trạng thái nguồn; giữ nguyên giá trị và thống kê phân bố trước khi ánh xạ.
- `borough`, `incident_zip`: trường phân tích khu vực tùy chọn; ZIP giữ string, không thay thế dữ liệu thiếu bằng một khu vực thật.

Tên backend và định dạng timestamp phải được xác nhận từ metadata/schema thực tế trước khi viết transform. CSV xuất có thể dùng nhãn hiển thị thay cho tên API; cần ánh xạ tường minh, phát hiện thiếu cột và fail có thông báo khi schema không tương thích.

## Thời gian và trùng lặp

- `ingested_at_utc` là thời gian tải, luôn ISO-8601 UTC; không dùng nó thay ngày tạo yêu cầu.
- Dùng múi giờ `America/New_York` cho giờ/thứ của yêu cầu. Kiểm tra định nghĩa nguồn trước khi chuyển múi giờ; timestamp thiếu offset quanh DST có thể mơ hồ, phải ghi hạn chế thay vì khẳng định độ chính xác tuyệt đối.
- `resolution_hours = (closed_at - created_at) / 3600` chỉ cho cặp thời gian hợp lệ, không âm. Trường hợp khác giữ cờ lý do, bỏ khỏi RQ3 nhưng không tự động bỏ khỏi RQ1/RQ2.
- Dòng trùng hoàn toàn: giữ một bản, ghi số loại bỏ. Cùng unique_key nhưng nội dung khác: chưa tự chọn tùy tiện; cách ly nhóm xung đột và báo số lượng, chỉ chốt quy tắc sau khi xem nguồn.
- Với median/p90 xấp xỉ bằng Spark, ghi rõ phương pháp và tham số accuracy; không trình bày như phân vị chính xác.

## Báo cáo chất lượng bắt buộc khi chạy

Ghi run ID, schema thực tế, số dòng đọc, số dòng parse lỗi, thiếu khóa, trùng hoàn toàn, khóa xung đột, thiếu ngày đóng, thời lượng âm, số dòng Silver và số mẫu RQ3. Các cờ lỗi có thể chồng lấp; tổng đối soát dùng nhóm loại bỏ loại trừ nhau.

Không áp ngưỡng null cho mọi cột. Điều kiện loại khỏi tập chính chỉ áp dụng cho khóa/ngày tạo theo quy tắc trên; từng câu hỏi có điều kiện đủ dữ liệu riêng.
