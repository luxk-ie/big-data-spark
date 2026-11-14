# Phương án đồ án Big Data — NYC 311

> Cập nhật triển khai 23/10/2026: NYC 311 trả HTTP 403; Checkpoint 2 đã chuyển sang phương án dự phòng NYC TLC Yellow Taxi tháng 01/2025. Xem README.md, metadata/source.md và docs/checkpoint2_report.md cho nguồn, kết quả và phạm vi đang chạy. Phần NYC 311 bên dưới là phương án ban đầu, không phải pipeline đã triển khai.

Ngày lập: 23/10/2026. Phương án được xây dựng cùng ChatGPT qua skill codex-with-chatgpt, sau khi ChatGPT xác nhận workspace `big-data-spark` và đọc `INSTRUCTIONS.md`, `src/env_check.py`.

## Đề tài và lựa chọn dữ liệu

**Phân tích xu hướng yêu cầu dịch vụ công và thời gian đóng yêu cầu tại New York bằng Apache Spark.**

Dataset đề xuất: **311 Service Requests from 2020 to Present**, mã `erm2-nwe9`, do NYC Open Data công bố. Đây là lựa chọn để triển khai tiếp; chưa tải dữ liệu hoặc chạy pipeline trong bước lập kế hoạch.

Ba ứng viên đã xem xét:

- NYC 311: dữ liệu yêu cầu dịch vụ công; có bài toán thời gian, loại vấn đề, cơ quan và khu vực. Chọn vì thể hiện được ingestion, chất lượng dữ liệu và Spark SQL trong cùng một đề tài. Điểm khó là dữ liệu nguồn có thể cập nhật và schema cần kiểm tra.
- NYC TLC Trip Records: chuyến taxi, file Parquet theo tháng; phù hợp nếu ưu tiên tải file theo tháng và phân tích nhu cầu đi lại. Là phương án dự phòng khi việc xuất NYC 311 gặp giới hạn truy cập.
- Citi Bike Trip Histories: lịch sử chuyến xe đạp tải xuống được; dễ diễn giải nhưng cần quản lý ZIP/CSV và khác biệt schema giữa các thời kỳ.

Nguồn để kiểm tra và tải:

- [NYC 311 — trang dataset](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data)
- [NYC — thông báo thay đổi schema và tách dữ liệu](https://www.nyc.gov/opendata/news/all-news/311-Service-Requests-Updates)
- [TLC — Trip Record Data](https://home4.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Citi Bike — System Data](https://citibikenyc.com/system-data)

Không đóng đinh số dòng hiện tại vì nguồn thay đổi. Theo thông báo NYC, dữ liệu 2010–2019 đã tách khỏi bộ 2020 đến nay; nhãn Complaint Type đổi thành Problem, Descriptor đổi thành Problem Detail, còn tên backend API được giữ nguyên.

## Phạm vi dữ liệu

- Pilot: xuất nguyên bản một tháng hoàn chỉnh, đề xuất tháng 01/2025; không làm sạch trước khi lưu raw.
- Bản đánh giá: mở rộng các tháng liên tiếp trong năm 2025, hướng đến 1–3 triệu dòng thực tế sau khi đo dung lượng và thời gian. Đây là mục tiêu, chưa phải số liệu đã kiểm chứng.
- Giữ đầy đủ trường nguồn trong raw; chỉ chọn các cột cần thiết khi tạo Silver.
- Ghi khoảng thời gian truy vấn theo nửa mở `[start, end)`, URL xuất thực tế, thời điểm tải và phiên bản schema. Không dùng LIMIT như một mẫu đại diện cho cả năm.
- Ưu tiên mỗi lần xuất thành một phản hồi/file hoàn chỉnh. Nếu cần nhiều phần, lưu từng phần nguyên bản với checksum riêng và manifest tổng chứa danh sách file; không thay thế bảy trường bắt buộc bằng một schema khác.
- Nguồn đang thay đổi: nhiều request tải liên tiếp không bảo đảm snapshot giao dịch đồng nhất. Ghi thời gian từng phần và hạn chế này; mọi phân tích tái lập phải đọc lại cùng snapshot raw đã lưu.

## Câu hỏi nghiên cứu

1. Loại vấn đề nào có nhiều yêu cầu nhất? Đếm và tính tỷ trọng theo Problem.
2. Lượng yêu cầu thay đổi thế nào theo tháng, thứ trong tuần và giờ địa phương New York?
3. Trong nhóm có ngày đóng hợp lệ, thời gian từ tạo đến đóng khác nhau thế nào theo loại vấn đề và cơ quan? Báo cáo số mẫu, median và p90, kèm tỷ lệ yêu cầu chưa đóng.
4. Tùy chọn: phân bố theo borough/ZIP khi trường vị trí đủ chất lượng. Số lượng yêu cầu không đồng nghĩa tỷ lệ theo dân số hoặc chất lượng dịch vụ.

Ngày đóng là chỉ báo hành chính; không mặc nhiên chứng minh vấn đề thực tế đã được giải quyết. RQ3 chịu thiên lệch do chỉ quan sát các yêu cầu đã đóng.

## Kiến trúc và quy tắc

Nguồn chính thức → raw bất biến + manifest → Silver Parquet bằng PySpark → Gold bằng Spark SQL → biểu đồ và báo cáo.

Mọi SparkSession dùng `local[*]`. Không cần Kafka, streaming hay cloud cluster cho phạm vi ban đầu.

Raw tuân thủ nguyên văn quy ước trong INSTRUCTIONS.md:

`data/raw/nyc_311/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/<filename>`

Mỗi run có `manifest.json` cùng thư mục với đủ `dataset`, `source`, `ingested_at_utc`, `bytes`, `sha256`, `destination`, `status`. Dung lượng và checksum lấy từ bytes thực tế. Chỉ success khi tải hoàn tất và xác minh thành công. File dở dang dùng đuôi `.part`, không được đưa vào transform. Khi thất bại vẫn ghi failed, mô tả lỗi trong log; không bịa checksum của file chưa tải được.

Tên run dùng UTC đến microsecond, tạo thư mục độc quyền; nếu trùng tên phải tạo tên mới. Rerun luôn tạo snapshot riêng, không sửa run cũ. Silver/Gold cũng mang run ID đầu vào; chạy lại transform trên cùng snapshot phải cho kết quả tổng hợp tương đương.

## Kế hoạch triển khai theo file

1. **Môi trường:** kiểm tra Python, Java, PySpark; chạy `src/env_check.py`, lưu bằng chứng 1.000.000 dòng tại `logs/env_check.txt`. Hiện script đã tồn tại nhưng lần chạy trước bị chặn bởi python.exe trong PATH, chưa xác nhận Spark hoạt động.
2. **Ingestion:** tạo `src/ingest_311.py`; nhận phạm vi ngày, tải theo luồng, giới hạn retry/timeout, lưu raw và manifest. Tạo kiểm thử thành công, lỗi tải, checksum và hai lần chạy độc lập.
3. **Transform:** tạo `src/transform_311.py`; đọc một run được chỉ định bằng schema tường minh, chuẩn hóa ở Silver, kiểm tra khóa, null, trùng lặp và timestamp. Lưu `data/processed/nyc_311/<run_id>/silver/` và bản ghi bị loại kèm lý do. Xem quy tắc tại data_dictionary.md.
4. **Phân tích:** tạo `src/analyze_311.py`, lưu SQL tại `sql/`; tạo `top_problem_types`, `requests_by_time`, `resolution_summary` trong Gold. Xuất CSV cho bảng nhỏ; không collect toàn bộ raw về driver.
5. **Nghiệm thu:** bổ sung README với thứ tự chạy; tạo `docs/validation_report.md` khi có kết quả thật. Báo cáo dung lượng, số dòng, thời gian, phiên bản môi trường và run ID. Vẽ tối thiểu ba biểu đồ từ Gold.

Checkpoint 2 theo brief và slide Buổi 6 đã cung cấp: môi trường có thể tái tạo và chạy thành công, nguồn truy cập được, ingest raw, manifest/log, Spark đọc được sample raw, minh chứng rerun và commit/tag cùng báo cáo ngắn. Silver/Gold dành cho Buổi 7 và giai đoạn sau; không phải yêu cầu Checkpoint 2. Xem `checkpoint2_requirements.md` để đối chiếu rubric và mẫu nộp.

## Tiêu chí nghiệm thu

- Smoke test local[*] đếm đúng 1.000.000 dòng, exit code 0 và có log thực tế.
- Manifest đủ bảy trường, đường dẫn tồn tại khi success, bytes và SHA-256 khớp file.
- Hai lần ingest tạo hai run khác nhau; checksum/file của run cũ không thay đổi.
- Lỗi mạng không được báo thành công hoặc cho transform đọc file chưa hoàn chỉnh.
- Đối soát số dòng raw = Silver giữ lại + bản ghi cách ly + dòng trùng bị loại theo quy tắc có ghi nhận; không đếm lặp một dòng có nhiều lỗi.
- Yêu cầu chưa đóng vẫn tham gia RQ1/RQ2; không gán thời lượng bằng 0. RQ3 chỉ dùng cặp timestamp hợp lệ, thời lượng không âm và có số mẫu đi kèm.
- Ba bảng Gold có schema, truy vấn và snapshot nguồn xác định; tổng RQ1/RQ2 khớp số dòng Silver đủ điều kiện.
- Chạy lại trên cùng raw cho kết quả tổng hợp tương đương; ghi rõ sai số cho phân vị xấp xỉ.
- Chỉ báo cáo thành công cho các bước đã thực sự chạy. Không tạo log hoặc kết quả giả.

## Điều kiện cần xác nhận trước khi triển khai

Đã có rubric Checkpoint 2 từ ba tài liệu Buổi 6. Tài liệu không ấn định số dòng tối thiểu; yêu cầu giải thích kịch bản tăng quy mô và ý nghĩa Big Data. Chưa biết RAM/CPU khả dụng, hạn nộp thực tế, thành viên nhóm, điều kiện dùng lại dữ liệu và giới hạn tải/API hiện hành. Bắt đầu pilot rồi đo; không tải toàn bộ kho ngay. Không cần các thông tin này để hoàn tất phương án, nhưng cần trước khi chốt quy mô và lịch thực hiện.

Mở rộng sau khi phần bắt buộc đạt: nhiều năm, phân tích địa lý bằng bảng tra cứu chính thức, đo hiệu năng partition/cache, dashboard hoặc ML nếu rubric yêu cầu.
