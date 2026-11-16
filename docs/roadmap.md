# Roadmap học phần Big Data

Roadmap này phân biệt yêu cầu đã giao với dự kiến kỹ thuật. Danh mục 25 file hiện có và vai trò từng tài liệu: `docs/course_materials.md`. Cập nhật theo brief mới của giảng viên, không tự xem các giai đoạn sau là rubric chính thức.

| Giai đoạn | Trạng thái | Kết quả và gate |
| --- | --- | --- |
| Đồ án 1 / proposal | Brief, mẫu pitch và workshop đã có; **PENDING** proposal đã điền và feedback duyệt | Brief §1–2 yêu cầu problem, hai dataset ứng viên, FR/NFR đo được, kiến trúc v1 và kế hoạch nhóm; cần đối chiếu đề tài đã duyệt với việc đổi từ NYC 311 sang TLC |
| Đồ án 2 / Checkpoint 2 | Đã chạy và có tag `checkpoint-2` | Ingest TLC nguyên bản, manifest, ba rerun, Spark đọc raw, báo cáo và evidence |
| Đồ án 3 / Checkpoint 3, Buổi 8 | **VERIFIED** pipeline/full run/rerun và slide 6 trang; **PENDING** thông tin nhóm, trình bày và chốt đề tài | Raw → ≥3 transform → join → aggregate → output; quality, baseline, rerun, báo cáo tại `reports/checkpoint3.md` |
| Đồ án 4 / Buổi 10 / performance | **Brief/rubric và mẫu 6 slide đã có**; benchmark và tối ưu còn **PENDING** | Freeze baseline; ≥3 run + median; bottleneck evidence → một hypothesis/change → before/after + correctness → KEEP/REVERT. Kế hoạch: `docs/checkpoint4_plan.md` |
| Các đồ án sau | Chưa có yêu cầu | Chỉ mở rộng khi nhận đề; cân nhắc nhiều tháng, dashboard hoặc ML nếu rubric và thời gian cho phép |

## Trình tự công việc tiếp theo

1. Nhóm xác nhận KPI và việc dùng lookup zone với proposal/giảng viên.
2. Điền thông tin thành viên vào slide đã tạo và diễn tập pitch 4 phút; commit/tag `checkpoint-3` đã lưu cục bộ, cần nộp lên nơi giảng viên chỉ định theo lịch thực tế.
3. Thực hiện Checkpoint 4 theo brief Buổi 10: cố định workload và benchmark baseline ≥3 lần trước khi thử một tối ưu có bằng chứng.

Điều kiện chuyển giai đoạn: có input/output định danh bằng hash, lệnh chạy được, số dòng đối soát, kết quả đọc lại được và báo cáo không lẫn kế hoạch với kết quả.
