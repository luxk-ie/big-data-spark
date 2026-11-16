# Nhật ký thí nghiệm Checkpoint 4

## Baseline và quan sát trước khi đổi cấu hình — 16/11/2026

**VERIFIED:** tag `checkpoint4-baseline` → commit `e4dc1f0`; một manifest Yellow Taxi 01/2025 có SHA-256 `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`; lookup SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`. Pipeline code SHA-256 của ba lần đo: `7fd32c7141945ecf7fdb235c770826566a872f48737cbb20ad6f1abb5dfde7cd`. Spark 3.5.8, `local[*]`, 18 logical cores, RAM vật lý 16.886.128.640 bytes, AQE bật, `spark.sql.shuffle.partitions=200`; driver memory không được đặt riêng. `spark.sql.files.maxPartitionBytes` là mặc định `134217728b` (đã kiểm bằng Spark session cùng máy). Warm-up 24,183 s không tính. Ba run đo 23,884 / 23,791 / 24,084 s; median **23,884 s**, khoảng min–max 0,293 s. Nguồn từng run: `reports/benchmark.csv` và output riêng dưới `data/processed/checkpoint4/baseline/20261116T063305911Z/`.

**VERIFIED:** cả bốn run có 3.475.226 raw rows, 3.475.204 in-scope/joined/trips, 4.652 Gold rows, cùng Gold SHA-256 `72850e09350553b3431befb4c4d1ca686d0f4cc36709de8714fb0bfb5df01aee`. Mỗi run đo lưu 31 completed stages và 31 jobs. `raw.distinct().count()` để tính `duplicate_full_rows` mất 7,399 / 7,301 / 7,381 s (median 7,381 s, khoảng 31% runtime toàn pipeline). Ở run1, stage 35 đọc raw, ghi 194.391.139 shuffle bytes và spill 158.828.820 disk bytes; stage 37 đọc shuffle đó và spill thêm 180.322.746 disk bytes. Jobs 22–23 và khoảng thời gian khớp action `duplicate_full_rows`; hai stage này là bottleneck ưu tiên. `plan.txt`, `jobs.json`, `stages.json`, `evidence.json` của từng run nằm cùng output.

## Giả thuyết đăng ký trước phép đo after

**PROPOSED — một biến:** giảm `spark.sql.files.maxPartitionBytes` từ mặc định `134217728b` xuống `2097152` bytes. Probe trên đúng raw file cho thấy số partition đọc Parquet tăng từ 15 lên 29. **Nếu** stage 35 spill vì mỗi task phải tổng hợp quá nhiều full rows, **thì** tăng partition đầu vào có thể giảm spill/task và thời gian `duplicate_full_rows`, kéo median toàn pipeline xuống; **trong khi** input hash, `duplicate_full_rows`, các quality count, schema/Gold hash giữ nguyên. Chỉ đổi cấu hình này qua `PYSPARK_SUBMIT_ARGS`; giữ code pipeline, master, cores, memory, AQE, shuffle partitions, manifest/lookup và warm-up giống baseline.

Expected signal: stage 35 có 29 task thay vì 15; disk spill của jobs 22–23 giảm, thời gian action duplicate giảm. Nếu thêm task làm tổng thời gian tăng hoặc spill không giảm, REVERT. Chỉ KEEP khi tất cả correctness assertions khớp, signal thực thi phù hợp và mức giảm median lớn hơn dao động 0,293 s quan sát ở baseline; nếu hiệu ứng nhỏ, ghi INCONCLUSIVE/REVERT. Tại lúc đăng ký giả thuyết, **chưa có số liệu after**.

## Kết quả thí nghiệm 1 — partition đọc Parquet

**VERIFIED:** chạy 1 warm-up + 3 run với `PYSPARK_SUBMIT_ARGS='--conf spark.sql.files.maxPartitionBytes=2097152 pyspark-shell'`, batch `20261116T063912865Z`. Runtime đo: 24,187 / 24,850 / 24,985 s; median **24,850 s**, lâu hơn baseline 0,966 s (~4,0%). Số raw partitions và task stage 35 tăng 15 → 29, nhưng disk spill của stage 35+37 ở run1 là 339.151.603 bytes so với baseline 339.151.566 bytes: gần như không đổi. Median action duplicate 7,409 s so với 7,381 s baseline. Dữ liệu `reports/benchmark.csv`, `evidence/checkpoint4/after_partition/run1`–`run3`.

**VERIFIED correctness:** cả ba run after có cùng raw/lookup/code SHA-256, toàn bộ quality metrics, output schema và Gold SHA-256 với baseline. **Decision: REVERT** cấu hình 2 MiB; expected signal giảm spill và runtime không xảy ra. Số partition tăng nhưng không giải quyết được phần hash aggregate/shuffle gây spill.

## Giả thuyết thí nghiệm 2 — đăng ký trước khi sửa code

**Observation VERIFIED:** ở baseline run1 có 6 stage đọc hơn 1 MB từ Parquet, tổng `inputBytes` 172.349.342 bytes; nhiều action trên cùng snapshot kéo lại raw lineage. Full-row distinct vẫn là action đơn lẻ tốn thời gian nhất, nhưng các scan lặp là chi phí cộng dồn có thể tránh.

**PROPOSED — một thay đổi code:** cache DataFrame `raw` trước action đầu tiên; Spark process độc lập cho từng run nên cache luôn bắt đầu rỗng và thời gian materialize được tính vào elapsed. **Nếu** đọc lại raw/transform nhiều lần đóng góp đáng kể vào runtime, **thì** các stage sau sẽ dùng `InMemoryTableScan` và tổng input Parquet bytes giảm, median toàn pipeline giảm; **trong khi** input hash, row counts, `duplicate_full_rows`, schema và Gold SHA-256 giữ nguyên. Giữ nguyên mọi Spark config, resources, manifest/lookup và harness. Chấp nhận rằng cache có thể làm tăng memory/spill và không giảm shuffle của distinct; nếu median không cải thiện rõ hoặc memory/spill xấu đi, REVERT. Tại lúc đăng ký, chưa có số đo after cache.

## Kết quả thí nghiệm 2 — cache raw

**VERIFIED:** commit thử nghiệm `77c1e4d`, 1 warm-up + 3 run đo batch `20261116T064247232Z`; 28,606 / 28,259 / 28,117 s, median **28,259 s**, lâu hơn baseline 4,375 s (**18,32%**). `plan.txt` có `InMemoryTableScan`, tức cache thực sự được sử dụng. Tuy nhiên việc materialize full 20 cột có chi phí lớn; disk spill toàn run1 tăng từ 339.151.566 lên 354.423.331 bytes. Median action duplicate tăng từ 7,381 lên 7,835 s. Các quality metrics, schema và Gold SHA-256 của cả ba run vẫn khớp baseline. Bằng chứng: `reports/benchmark.csv`, `evidence/checkpoint4/after_cache/run1`–`run3`.

**Decision: REVERT** cache raw. Kết quả đúng nhưng tổng runtime và spill xấu đi. Không giữ cache trong pipeline cuối.

## Giả thuyết thí nghiệm 3 — đăng ký trước khi sửa code

**Observation VERIFIED:** action exact full-row distinct là bottleneck lớn nhất, với median 7,381 s và 194.391.139 shuffle bytes ở stage 35 của baseline run1; stage 35+37 spill 339.151.566 disk bytes. Baseline có `duplicate_full_rows=0` trên snapshot đã xác minh SHA-256.

**PROPOSED — một thay đổi thuật toán cho cùng metric:** tính `xxhash64` trên toàn bộ cột của mỗi raw row, rồi đếm số fingerprint duy nhất. Nếu số fingerprint bằng `raw_count`, suy ra chắc chắn không có hai row giống nhau và trả `duplicate_full_rows=0`. Nếu có fingerprint lặp (bao gồm cả hash collision), **fallback** về `raw.distinct().count()` hiện tại để tính chính xác. Như vậy không chấp nhận sai số do hash collision. Kỳ vọng đường nhanh trên snapshot này shuffle khóa 64-bit thay vì 20 cột đầy đủ, giảm spill và median của action duplicate, giữ các count/schema/Gold SHA-256. Nếu fingerprint lặp hoặc chi phí hash cao làm median không giảm rõ, REVERT. Giữ input, tài nguyên, Spark config và quy tắc output như baseline.
