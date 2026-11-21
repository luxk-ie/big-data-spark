# Kịch bản trình bày Checkpoint 4

**Nhóm 13 — Nguyễn Phú Vinh.** Trạng thái: **PROPOSED** lời thoại, chưa đo thời lượng khi diễn tập. Dùng cùng [báo cáo PowerPoint](Checkpoint4_Report.pptx); mục tiêu tối đa **4 phút** theo `BigData_DoAn4_Brief_Checkpoint4_Template.docx` §1 và bài giảng Buổi 10 slide 24, 29. Số liệu lấy từ `reports/benchmark.csv`, `reports/checkpoint4.md` và `evidence/checkpoint4/`.

## Bài nói chính

### Slide 1 — Mở đầu, khoảng 20 giây

“Em là Nguyễn Phú Vinh, nhóm 13. Checkpoint 4 đo và tối ưu pipeline taxi từ Checkpoint 3. Nhóm cố định baseline, tìm bottleneck bằng Spark metrics, thay một phần xử lý, đo lại và kiểm output trước khi quyết định KEEP.”

### Slide 2 — Baseline, khoảng 35 giây

“Workload là một snapshot Yellow Taxi tháng 1/2025: 59 MB, 3.475.226 dòng, join bảng tra 265 khu vực. Gold đếm chuyến theo ngày, giờ và borough. Hai phiên bản chạy Spark 3.5.8 `local[*]` trên cùng máy 18 cores, cùng input và config. Bỏ một warm-up, ba run baseline là 23,884; 23,791; 24,084 giây. Median 23,884 giây; tag `checkpoint4-baseline`.”

### Slide 3 — Bằng chứng bottleneck, khoảng 45 giây

“Action `raw.distinct().count()` để kiểm dòng trùng mất median 7,381 giây, khoảng 31% runtime. Plan cho thấy Exchange và phép gom trên cả 20 cột raw. Ở baseline run1, stage 35 ghi 194 MB shuffle và spill 159 MB xuống disk; stage 37 đọc shuffle và spill thêm 180 MB. Tổng spill 339 MB. Jobs 22–23 trùng thời gian action. Nhóm chọn phép kiểm duplicate làm bottleneck; Gold join đã dùng BroadcastHashJoin.”

### Slide 4 — Giả thuyết và một thay đổi, khoảng 40 giây

“Giả thuyết là khóa 20 cột làm shuffle rộng. Nhóm chỉ đổi phép kiểm duplicate: tính `xxhash64` trên toàn bộ cột rồi đếm fingerprint duy nhất. Nếu số này bằng tổng dòng thì không có dòng trùng. Nếu fingerprint lặp, kể cả hash collision, pipeline quay về `raw.distinct().count()` để đếm exact. Kỳ vọng Exchange chỉ mang khóa 64-bit, giảm shuffle, spill và thời gian action; input, config, KPI và Gold giữ nguyên.”

### Slide 5 — Before/after, khoảng 55 giây

“Batch đầu có ba run mỗi phiên bản. Median pipeline giảm từ 23,884 xuống 23,217 giây, tức 2,79%; action duplicate giảm từ 7,381 xuống 4,041 giây. Ở run1, shuffle giảm từ 195 xuống 36,7 MB, disk spill từ 339 MB về 0. Gold vẫn có 4.652 dòng và 3.475.204 chuyến. Vì khoảng runtime batch đầu còn giao nhau, nhóm đo thêm: năm run after median 20,951 giây, ba run baseline gần thời điểm median 25,250 giây. Kết quả ủng hộ cải thiện, nhưng không suy ra một speedup chung cho mọi lần chạy.”

### Slide 6 — Correctness và quyết định, khoảng 45 giây

“Nhóm đối soát 3.475.226 dòng raw bằng 22 dòng ngoài tháng cộng 3.475.204 dòng trong tháng. Sau join, tổng chuyến Gold vẫn bằng 3.475.204; schema, grain và checksum giữ nguyên. Chín test đạt, kể cả fallback khi có duplicate. Thử giảm partition và cache raw đều chậm hơn nên REVERT. Nhóm KEEP fingerprint với fallback exact vì output đúng, shuffle và spill giảm. Kết luận chỉ áp dụng cho một tháng trên máy này. Hướng tối ưu tiếp là benchmark nhiều tháng, tìm bottleneck mới qua Spark metrics, rồi thử giảm scan lặp hoặc chỉnh shuffle; chỉ KEEP khi median giảm và Gold không đổi.”

**Tổng thời lượng dự kiến: 240 giây.** Khi tập, cắt bớt số lẻ trên slide 2 hoặc 3 nếu vượt 4 phút; không bỏ phần correctness và giới hạn kết luận.

## Câu hỏi có thể gặp sau bài nói

| Câu hỏi | Trả lời ngắn |
| --- | --- |
| Baseline và after có cùng workload không? | Có. Cùng một manifest raw và lookup được đối chiếu SHA-256, cùng máy, Spark `local[*]`, 18 cores, AQE và `shuffle.partitions=200`. Mỗi batch có một warm-up rồi ít nhất ba run đo. |
| Vì sao gọi đây là bottleneck? | Action duplicate mất median 7,381 giây; plan có Exchange theo 20 cột. Baseline run1 ghi khoảng 195 MB shuffle và 339 MB disk spill ở các stage liên quan. |
| Vì sao hash không làm sai kết quả? | Chỉ trả `duplicate_full_rows=0` khi mọi fingerprint đều khác nhau. Bất kỳ fingerprint lặp nào cũng kích hoạt phép đếm distinct toàn dòng như baseline. |
| Tại sao không chọn cache hay đổi partition? | Đã đo từng thay đổi riêng: partition 2 MiB cho median 24,850 giây, cache raw 28,259 giây; cả hai chậm hơn baseline nên REVERT. |
| Output sau tối ưu đúng như trước không? | Raw accounting, số dòng join, tổng `trip_count`, schema/grain và SHA-256 Gold khớp ở các run đã đối chiếu; 9 test đạt. |
| Nếu speedup nhỏ hơn độ dao động thì kết luận thế nào? | Không kết luận chỉ từ batch đầu 2,79%. Nhóm đo thêm batch after và baseline gần thời điểm, đồng thời báo riêng biến thiên giữa các batch. |
| Vì sao lần chạy ngày 20/11 chỉ khoảng 20 giây? | Batch mới có ba run 20,191 / 19,460 / 20,347 giây, median 20,191 giây; input, code và Gold checksum khớp. Khác ngày đo và không có baseline cùng ngày, nên chỉ báo đây là kết quả đo lại của optimized, không dùng làm tỷ lệ speedup chính. |
| Dữ liệu tăng 10× thì sao? | **PENDING:** chưa đo nên chưa khẳng định. Nếu nhiều fingerprint lặp, nhánh fallback có thể tốn thời gian; cần benchmark workload lớn rồi mới chọn cách lưu trữ hoặc tối ưu khác. |

Khi giảng viên yêu cầu mở bằng chứng, dùng `reports/benchmark.csv` cho từng run, `reports/explain-before.txt` và `reports/explain-after.txt` cho plan, `evidence/checkpoint4/baseline/run1/stages.json` và `evidence/checkpoint4/after_hash/` cho stage metrics. Không đọc đường dẫn trong bài nói chính.
