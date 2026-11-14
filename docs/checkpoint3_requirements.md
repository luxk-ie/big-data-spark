# Đối chiếu yêu cầu Đồ án 3

Trạng thái tài liệu: **VERIFIED** về nội dung văn bản, bảng và slide gốc tại `C:/Users/ACER/Documents/Docs/BigData/`, đọc ngày 10/11/2026. File gốc nằm ngoài repo; bảng này là bản đối chiếu để nhóm làm việc, không thay thế brief giảng viên. D1–D6 và R1 đã có bằng chứng full run; D7 và T1 còn bước bàn giao/trình bày thực tế.

Nguồn gốc đối chiếu: `BigData_DoAn3_Brief_Checkpoint3_Template.docx` (gọi là **Brief** trong bảng), `BigData_Buoi08_DoAn3_Spark_Processing_Pipeline.pptx` (**slide Buổi 8**) và `BigData_DoAn3_Checkpoint3_Report_Template.pptx` (**mẫu slide báo cáo**). Các file này được đọc tại máy hiện tại, chưa đưa vào Git nên người clone repo cần có bản giảng viên để đối chiếu độc lập.

| Mã | Yêu cầu giảng viên | Nguồn | Triển khai dự kiến | Bằng chứng cần có |
| --- | --- | --- | --- | --- |
| D1 | Một lệnh chạy end-to-end từ raw đến output | Brief Đồ án 3, §2 D1; slide Buổi 8 số 6–7 | `src/pipeline_tlc.py` nhận manifest và lookup snapshot | Lệnh, exit code, output đọc lại |
| D2 | ≥3 transformations có lý do nghiệp vụ | Brief §1 và §2 D2; slide 14 | Chọn/cast cột, lọc theo rule, derive ngày/giờ và cờ chất lượng | Code, counts trước/sau |
| D3 | ≥1 join; key, type, uniqueness, matched/unmatched, row counts | Brief §1 và §2 D3; slide 15 | Left join `PULocationID` với lookup `LocationID` | Duplicate key check, counts, unmatched sample |
| D4 | ≥1 aggregation phục vụ câu hỏi đề tài; output rõ grain | Brief §1 và §2 D4; slide 10, 16 | Số chuyến theo ngày × giờ × borough đón | Schema, bảng KPI mẫu, tổng đối soát |
| D5 | Quality: null, duplicate, invalid, unmatched hoặc tương đương | Brief §2 D5; slide 11, 17 | Quality flags và nhóm loại trừ không chồng | Quality report từ full snapshot |
| D6 | Baseline trước tối ưu: runtime, input size, execution plan, environment | Brief §2 D6; slide 8, 21 | Đo full run, lưu `explain("formatted")` | Log thời gian, bytes/files/partitions, plan |
| D7 | README, checkpoint3.md, code, evidence/output mẫu và commit | Brief §2 D7; slide 29–30; mẫu slide báo cáo 1–6 | `reports/checkpoint3.md`, slide báo cáo, hướng dẫn chạy và commit/tag sau nghiệm thu | Repo, commit/tag, report/slide |
| R1 | Chạy lại không tạo duplicate hoặc output hỗn loạn | Brief §4–5; slide 6, 22 | Chọn input theo hash, không append, so kết quả logic hai lần | Rerun comparison |
| T1 | Nhóm giải thích được grain, join, KPI, baseline | Brief §5–6; slide 25, 32 | Pitch tối đa 4 phút và exit ticket | Thành viên tự trình bày; không suy ra từ code |

Brief §6 chấm 10 điểm: pipeline 2,0; transformations 1,5; join 2,0; aggregation/output 1,5; quality 1,5; baseline/reproducibility 1,0; team understanding 0,5. Slide 2 và 8 dành tối ưu partition, broadcast, cache và skew cho Buổi 9–10; đây không phải điều kiện hoàn thành Checkpoint 3.

StarterKit Buổi 8 dùng `orders.csv`/`customers.csv` để minh họa pipeline; nó không phải input của đề tài taxi. Tài liệu Checkpoint 2 ở repo chứng minh raw TLC 01/2025 đã được nạp, nhưng chưa chứng minh bất kỳ D1–D7 nào của Checkpoint 3.

## Trạng thái nghiệm thu hiện tại

| Mã | Trạng thái | Bằng chứng trên full snapshot |
| --- | --- | --- |
| D1–D5 | **VERIFIED** | `src/pipeline_tlc.py`, `evidence/checkpoint3/run1.json`, `preview.csv`, `tests.txt` và đối soát trong `reports/checkpoint3.md` |
| D6 | **VERIFIED** | Runtime/input/partitions/environment trong `run1.json`; `evidence/checkpoint3/plan.txt` |
| R1 | **VERIFIED** | `run1.json`, `run2.json`, `rerun_comparison.json`: cùng code/input/schema/metrics/Gold hash |
| D7 | **PENDING** | README, báo cáo, 6 slide và commit/tag cục bộ `checkpoint-3` đã có; thông tin nhóm, nơi nộp và nộp bài còn chờ |
| T1 | **PENDING** | Cần thành viên tự thuyết trình/exit ticket; không suy ra từ code hoặc slide |
