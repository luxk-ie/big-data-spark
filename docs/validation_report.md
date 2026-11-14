# Kết quả kiểm chứng Checkpoint 2

Ngày 23/10/2026, bằng chứng tại repo cục bộ.

- Environment: PASS, logs/env_check.txt; Python 3.12.14, Java 17.0.20.1, Spark 3.5.8, local[*], count 1.000.000.
- Source: TLC download thành công, metadata/source.md. NYC 311 thất bại HTTP 403, không báo success giả.
- Ingest: 3 successful runs, mỗi run 59.158.238 bytes; log tlc_run2/3/4.txt.
- Rerun: PASS, metadata/rerun_verification.json; 3 path riêng và checksum khớp manifest khi tính lại.
- Spark raw read: PASS, logs/raw_inspection.txt; 3.475.226 dòng, 20 cột, schema và 5 mẫu.
- Automated tests: 5 PASS tại logs/tests.txt; dữ liệu fixture chỉ dùng test, không thay thế evidence nguồn thật. Bao gồm Parquet hỏng/rỗng và regression đóng handle trên Windows.
- Sample: 1.000 dòng dẫn xuất có snapshot cha; không chỉnh sửa raw.
- Reproducibility: requirements pin, setup/run scripts và README có lệnh. Chưa chạy trên máy sạch thứ hai; nhánh cài Python fallback chưa kiểm chứng.
- Report: reports/Checkpoint2_Report.pptx đã điền đủ 6 slide, có speaker notes; đã mở/render và kiểm tra từng slide trong Microsoft PowerPoint. reports/Checkpoint2_Report.docx là phần mẫu nộp đã điền kèm exit ticket, 2 trang; đã render bằng Microsoft Word và kiểm tra ảnh từng trang. Thông tin nhóm/owner/deadline chưa được cung cấp nên được đánh dấu cần bổ sung. Ghi chú pitch phân bổ khoảng 230 giây; chưa đo thuyết trình thật.

Git tag checkpoint-2 là mốc nghiệm thu cục bộ sau khi lưu evidence. Chưa push lên dịch vụ bên ngoài. Team understanding cần từng thành viên tự diễn giải, không thể chứng minh bằng việc script chạy tự động.
