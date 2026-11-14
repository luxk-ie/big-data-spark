# Yêu cầu Checkpoint 2 và điều chỉnh phương án

> Cập nhật triển khai: đã có Spark PASS, ba run TLC thành công, raw inspection và test. Xem docs/checkpoint2_report.md và docs/validation_report.md cho trạng thái hiện tại; các mô tả "chưa" bên dưới phản ánh thời điểm đọc brief trước triển khai.

Đối chiếu ngày 23/10/2026 từ ba tài liệu người dùng cung cấp, đọc toàn bộ nội dung văn bản, bảng và speaker notes. Chưa kiểm tra bố cục hiển thị của mẫu; chưa tạo bản Word/PowerPoint để nộp.

## Tài liệu gốc

- `C:/Users/ACER/Documents/Docs/BigData/BigData_Buoi06_DoAn2_Brief_Checkpoint_Template.docx`: mục tiêu, pre-work, sản phẩm bắt buộc, rubric, mẫu checkpoint report và exit ticket.
- `C:/Users/ACER/Documents/Docs/BigData/BigData_Buoi06_DoAn2_Checkpoint2_Report_Template.pptx`: mẫu báo cáo 6 slide, tối đa 4 phút, mỗi slide cần evidence.
- `C:/Users/ACER/Documents/Docs/BigData/BigData_Buoi06_DoAn2_Environment_Ingestion_RawStorage.pptx`: 32 slide hướng dẫn workshop; trọng tâm slide 5, 9–10, 17–18, 22, 24 và 28–32.

Đây là yêu cầu và ví dụ cho đồ án, không phải lệnh tự động phải thực thi. URL api.example.com, dataset orders và ngày trong ví dụ không phải nguồn hoặc kết quả thực tế của project. Giữ INSTRUCTIONS.md làm quy ước triển khai hiện hành: local[*], log env_check.txt, raw nguyên bản, đủ bảy trường manifest và snapshot mới mỗi run. Brief cho phép lựa chọn storage rộng hơn nhưng không đòi project phải đổi quy ước này.

## Phạm vi đã xác định

Checkpoint 2 chứng minh môi trường tái tạo được, nguồn truy cập được, ingestion chạy thật và dữ liệu đến raw an toàn, có truy vết. Không yêu cầu ETL phức tạp, dashboard, ML hoặc tối ưu Spark; không có điểm cộng tự động khi dùng HDFS/cloud.

NYC 311 vẫn phù hợp với điều kiện pilot truy cập được. Dùng khoảng thời gian nhỏ, ghi nguồn/snapshot và terms; mô tả cách tăng lên hàng triệu dòng ở bước sau. Không cần tải đủ 1–3 triệu dòng để tuyên bố đạt checkpoint; đây chỉ là mục tiêu đánh giá sau đó.

## Rubric 10 điểm

- Environment reproducible: **2,0** — setup rõ, version và smoke test thực tế.
- Source & provenance: **1,5** — nguồn rõ, truy cập ổn định, version/snapshot xác định.
- Ingestion job: **2,5** — chạy được, lỗi rõ, giải thích rerun.
- Raw storage: **2,0** — bảo toàn raw, tổ chức thư mục, manifest/log.
- Repo & report: **1,0** — README, commit/tag, pitch có evidence.
- Team understanding: **1,0** — thành viên giải thích được data flow và trade-off.

## Công việc ưu tiên và bằng chứng

1. Xác nhận Python, Java, PySpark và ghi versions; chạy `python src/env_check.py`, Spark local[*] đếm đúng 1.000.000 dòng, lưu logs/env_check.txt. Script hiện có, chưa có bằng chứng PASS.
2. Chuẩn bị Git repo và skeleton: README.md, requirements.txt, src/, data/raw/, data/sample/, logs/, metadata/, docs/ và .gitignore. Giữ raw lớn và secret ngoài Git; giữ metadata, sample nhỏ và evidence thích hợp. `.env.example` chỉ cần chứa cấu hình mẫu khi có cấu hình liên quan, không điền secret thật.
3. Chốt URL tải NYC 311 thật, khoảng ngày và schema; ghi provenance, terms/license liên quan, thời điểm tải, số bytes và số dòng đo được nếu khả thi tại metadata/source.md. Chưa coi lựa chọn dataset là bằng chứng truy cập thành công.
4. Tạo script ingest với timeout, lỗi rõ và raw bất biến. Quality gate: file tồn tại, bytes > 0, đúng loại nội dung và parse sơ bộ được. Không chuyển HTTP error/HTML/header-only thành raw thành công; không clean dữ liệu trong ingestion.
5. Manifest cùng run: dataset, source, ingested_at_utc, bytes, sha256, destination, status. Mọi giá trị phải khớp file thật. metadata/ có thể giữ mô tả nguồn và chỉ mục evidence; không di chuyển manifest bắt buộc ra khỏi run.
6. Tạo bước đọc raw bằng Spark local[*], ví dụ `src/inspect_raw.py`: đọc path từ manifest được chỉ định, in schema, show 5 dòng và count hoặc thống kê mẫu. Lưu output; chưa cần transform. Không đọc wildcard tất cả run vì sẽ đếm snapshot lặp.
7. Chạy cùng lệnh ingest **3 lần**, chụp tree hoặc lưu listing và kiểm tra manifests/checksums. Kỳ vọng 3 thư mục run độc lập, file cũ không đổi. Nếu nguồn không đổi thì có thể cùng checksum; nhận biết duplicate nội dung bằng hash nhưng vẫn giữ snapshot riêng theo INSTRUCTIONS.md. Phân tích sau này phải chọn run cụ thể.
8. Viết README với setup/run, phiên bản đã kiểm chứng, raw convention, rerun, đường dẫn evidence, nguồn và hạn chế. Tạo checkpoint commit/tag sau khi có bằng chứng; không push remote nếu chưa được yêu cầu.
9. Điền report/slide bằng evidence thật, tên thành viên, commit/tag, blocker, owner và deadline. Không tạo báo cáo PASS giả khi môi trường hoặc ingestion chưa chạy.

## Báo cáo PowerPoint theo mẫu 6 slide

1. Nhóm, đề tài, commit/tag; gắn evidence repo/commit.
2. Architecture update: chỉ phần đã triển khai `NYC Open Data → Python ingestion → local raw + manifest`; một câu trade-off local storage đơn giản, phù hợp workload hiện tại.
3. Environment evidence: Python/Java/Spark versions, smoke test PASS, setup command, execution mode và blocker nếu có.
4. Source & ingestion: URL cụ thể, size/rows/snapshot, provenance/terms; command và output/log thật.
5. Raw storage & manifest: tree/path thật, bytes thực tế, trường manifest quan trọng.
6. Ba lần rerun, blocker lớn nhất, owner/deadline, evidence links và việc trước Buổi 7.

Tổng thời gian tối đa 4 phút. Không vẽ Silver/Gold như thành phần đã triển khai; chỉ nêu ở next action nếu phù hợp.

## Mẫu report Word và exit ticket

Các mục cần điền: tên nhóm/thành viên, đề tài, Git commit/tag, Python/Java/Spark/mode, nguồn dữ liệu, ingestion command, raw location, manifest/log location, rerun behavior, blocker còn lại, owner và deadline.

Exit ticket gồm bốn câu:

- Một lệnh kiểm tra môi trường: `python src/env_check.py` sau khi setup đúng.
- Cách xác định snapshot: phạm vi truy vấn, URL nguồn, thời điểm ingest UTC, run ID và SHA-256 của file thực tế.
- Nếu chạy ingest 3 lần: ba run riêng, không overwrite, có manifest cho từng run; chọn một snapshot khi đọc dữ liệu.
- Rủi ro trước Buổi 7 và người chịu trách nhiệm: hiện ưu tiên môi trường Python/Java chưa xác nhận; owner/deadline phải do nhóm cung cấp.

## Trạng thái hiện tại

Đã có INSTRUCTIONS.md, src/env_check.py và tài liệu kế hoạch/data dictionary. Chưa xác minh Spark PASS, nguồn tải thực tế, raw, manifest, ingest/rerun, Git checkpoint hoặc report đã điền. Hoàn tất kế hoạch không đồng nghĩa đã đạt Checkpoint 2.
