# Nguồn dữ liệu Checkpoint 2

Nguồn đang sử dụng: NYC Taxi and Limousine Commission, Yellow Taxi tháng 01/2025.

- Trang công bố: https://home4.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- File tải: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
- Dữ liệu do các nhà cung cấp công nghệ được TLC cho phép cung cấp; TLC lưu ý không bảo đảm độ chính xác. File được công bố theo tháng ở định dạng Parquet.
- Dữ liệu 2025 có thêm `cbd_congestion_fee`; không giả định schema các năm giống nhau.
- Quyền sử dụng: tham khảo điều khoản NYC tại https://www.nyc.gov/home/terms-of-use.page và tài liệu trên trang TLC trước khi phân phối lại. Không gán một giấy phép Creative Commons chưa được nguồn công bố. Raw lớn không đưa vào Git.
- Snapshot cụ thể được định danh bằng URL tháng, thời điểm ingest UTC và SHA-256 trong manifest. Cùng tên file nguồn không bảo đảm bytes không bao giờ đổi; mỗi lần chạy phải tính lại hash.
- Checkpoint đã tải 59.158.238 bytes; Parquet có 3.475.226 dòng, 20 cột. SHA-256: `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`.

Ngày kiểm tra: 23/10/2026. NYC 311 là lựa chọn ban đầu nhưng hai endpoint chính thức trả HTTP 403 tại thời điểm triển khai. Evidence: logs/ingest_run1.txt và manifest failed trong data/raw/nyc_311/. Không sử dụng dữ liệu giả thay thế. TLC là ứng viên dự phòng đã ghi trong docs/project_plan.md.

Sample trong data/sample/ là bản dẫn xuất 1.000 dòng đầu từ snapshot thành công, phục vụ kiểm tra nhỏ; không đại diện thống kê và không thay thế raw. metadata/rerun_verification.json ghi snapshot cha.
