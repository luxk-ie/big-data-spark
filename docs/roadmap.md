# Roadmap học phần Big Data

Roadmap này phân biệt yêu cầu đã giao với dự kiến kỹ thuật. Cập nhật theo brief mới của giảng viên, không tự xem các giai đoạn sau là rubric chính thức.

| Giai đoạn | Trạng thái | Kết quả và gate |
| --- | --- | --- |
| Đồ án 1 / proposal | Tài liệu gốc có trong thư mục bài giảng; repo hiện không chứa proposal hoàn chỉnh | Cần đối chiếu đề tài đã duyệt với việc đổi từ NYC 311 sang TLC |
| Đồ án 2 / Checkpoint 2 | Đã chạy và có tag `checkpoint-2` | Ingest TLC nguyên bản, manifest, ba rerun, Spark đọc raw, báo cáo và evidence |
| Đồ án 3 / Checkpoint 3, Buổi 8 | **VERIFIED** pipeline/full run/rerun và slide 6 trang; **PENDING** thông tin nhóm, trình bày và chốt đề tài | Raw → ≥3 transform → join → aggregate → output; quality, baseline, rerun, báo cáo tại `reports/checkpoint3.md` |
| Buổi 9–10 / performance | Được slide Buổi 8 báo trước, **chưa có brief chi tiết** | Dùng baseline Đồ án 3; đo partition/shuffle, thử tối ưu có kiểm soát và so sánh cùng input/máy |
| Các đồ án sau | Chưa có yêu cầu | Chỉ mở rộng khi nhận đề; cân nhắc nhiều tháng, dashboard hoặc ML nếu rubric và thời gian cho phép |

## Trình tự công việc tiếp theo

1. Nhóm xác nhận KPI và việc dùng lookup zone với proposal/giảng viên.
2. Điền thông tin thành viên vào slide đã tạo và diễn tập pitch 4 phút; commit/tag `checkpoint-3` đã lưu cục bộ, cần nộp lên nơi giảng viên chỉ định theo lịch thực tế.
3. Sau khi giảng viên giao Buổi 9–10, thiết kế benchmark so sánh trên snapshot cố định.

Điều kiện chuyển giai đoạn: có input/output định danh bằng hash, lệnh chạy được, số dòng đối soát, kết quả đọc lại được và báo cáo không lẫn kế hoạch với kết quả.
