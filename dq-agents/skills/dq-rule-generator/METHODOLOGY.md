# Methodology - DQ Rule Generator

## 1. Build Context

Bốn nguồn tín hiệu, mỗi nguồn sinh ra một loại rule khác nhau:

| Nguồn | Lấy bằng | Cho biết |
|---|---|---|
| Column lineage DIRECT | `get_lineage()` | `transformation` — CÔNG THỨC thật sinh ra từng cột |
| Phụ thuộc INDIRECT | `__indirect_impact__` | cột nào là GROUP_BY / JOIN / FILTER |
| Business metadata | YAML | ý nghĩa nghiệp vụ, `domain_constants`, dải giá trị |
| Lịch sử volume | `check_volume_anomaly()` | row-count bất thường — CHỈ để chỉnh weight |

Từ OpenLineage 1.53.0, `transformation` là **biểu thức Catalyst thật**, không phải nhãn chung
chung. Ví dụ thật của pipeline này:

```
tac_device       substring(imeisv, 1, 8) AS tac_device
date_time        substring(date_hour, 1, 10) AS date_time
imsi             imsi                                        (IDENTITY - copy thẳng)
apn_ims_count    count(CASE WHEN (apn IN ('ims',...)) THEN 1 END) AS apn_ims_count
volte_call_count count(CASE WHEN ((event_id = '...') AND (apn IN ('ims',...))) THEN 1 END) AS ...
```

**Đọc công thức, đừng đọc nhãn.** Toàn bộ mục 2 dựa vào việc phân tích chuỗi này.

## 2. Ánh xạ tín hiệu → rule

### 2.1. IDENTITY (copy thẳng từ nguồn)

Cột không qua biến đổi nào. Pipeline không thể làm hỏng nó — mọi lỗi đều **đến từ nguồn**.

→ Sinh: NULL / rỗng, định dạng, độ dài, tập giá trị hợp lệ.
→ Trong `description` phải nêu rõ lỗi bắt nguồn từ hệ thống nguồn, không phải lỗi biến đổi.

### 2.2. `substring(x, 1, N)` — cắt chuỗi

Đây là mỏ vàng: công thức cho biết **chính xác** độ dài mong đợi.

→ Sinh 2 rule:
1. Kết quả phải đúng `N` ký tự: `LENGTH(tac_device) <> 8`
2. Nguồn phải đủ dài: nếu `imeisv` ngắn hơn `N` thì `substring` trả về chuỗi cụt **mà không
   báo lỗi** — sai âm thầm.

### 2.3. `cast(x AS T)` — ép kiểu

→ Sinh: `try_cast(x AS T) IS NULL AND x IS NOT NULL` — bắt trường hợp ép kiểu thất bại
(Spark trả NULL chứ không ném lỗi).

### 2.4. AGGREGATION — **ĐỌC KỸ, đây là chỗ hay sinh rule vô dụng**

**KHÔNG sinh** `count(...) < 0` hay `count(...) IS NULL`. `COUNT()` trong SQL **không bao giờ**
trả về số âm hay NULL — rule đó luôn đúng 0 dòng, tốn chỗ mà không bắt được gì. Đây là lỗi đã
xảy ra thật và bị Execute Rules bác thẳng.

**Thay vào đó, SO SÁNH CÔNG THỨC GIỮA CÁC CỘT để tìm bất biến.** Khi điều kiện của cột A là
tập con điều kiện của cột B thì suy ra ngay một ràng buộc:

```
volte_call_count = count(CASE WHEN (event_id = '...' AND apn IN (...)) THEN 1 END)
apn_ims_count    = count(CASE WHEN (apn IN (...))                      THEN 1 END)

Điều kiện thứ nhất = (A AND B), thứ hai = (B)  ->  {A∧B} ⊆ {B}
=> BẤT BIẾN: volte_call_count <= apn_ims_count
=> failed_sql_expr: volte_call_count > apn_ims_count
```

Đây là rule **chỉ suy ra được từ lineage**, không đọc được từ YAML, và là giá trị lớn nhất mà
lineage mang lại. Luôn rà mọi cặp cột aggregate để tìm quan hệ tập con kiểu này.

Ngoài ra: tổng các cột đếm con không được vượt tổng thể, cột đếm không vượt số dòng nguồn.

### 2.5. GROUP_BY (từ `__indirect_impact__`)

Các cột GROUP_BY tạo thành **grain** của bảng. Một cột grain bị NULL sẽ sinh ra một nhóm NULL
giả, làm sai toàn bộ số liệu tổng hợp — hậu quả nặng hơn NULL ở cột thường nhiều.

→ Sinh: NOT NULL cho TỪNG cột grain, `importance_weight` = 5.
→ Sinh THÊM một rule `kind: "dataset"` kiểm khoá grain còn duy nhất sau bước gộp:
  `count(DISTINCT k) <> count(k)`. Đây là rule bắt buộc, không phải tuỳ chọn — xem 2.8.

### 2.6. JOIN / FILTER (từ `__indirect_impact__`)

→ JOIN: hai rule, ở hai checkpoint khác nhau.
  1. TRƯỚC join: cột khoá NULL sẽ làm mất dòng âm thầm → NOT NULL cho cột khoá.
  2. SAU join: dòng mồ côi (khoá có giá trị nhưng không khớp bảng tra) →
     `join_key IS NOT NULL AND right_col IS NULL`. Mức dòng bình thường, không cần subquery:
     sau join cả hai vế đã nằm chung một dataframe.
  Rule 2 quan trọng vì nó gọi đúng tên nguyên nhân. Thiếu nó thì lỗi vẫn lộ ra, nhưng lộ dưới
  dạng "cột vế phải bị NULL" — đọc report không biết là do khoá hỏng hay do bảng tra thiếu.
→ FILTER: không sinh rule mức dòng được (dòng bị lọc thì không còn trong output để mà kiểm).
  Ghi nhận trong `overall_impact_warning`.

### 2.7. Business metadata

| Có gì trong YAML | Sinh rule |
|---|---|
| `domain_constants` (danh sách giá trị hợp lệ) | `col NOT IN (danh sách)` |
| `business_rule` nêu chuẩn cụ thể | đúng theo chuẩn đó — vd "15 chữ số theo ITU E.212" → `LENGTH(x) <> 15 OR x NOT RLIKE '^[0-9]+$'` |
| `description` nói đây là khoá/định danh | NOT NULL, weight cao |
| đơn vị / dải giá trị | kiểm tra biên |

**YAML thắng suy đoán.** Nếu YAML nói IMSI 15 chữ số thì viết 15, đừng đoán 9-15. Ràng buộc
sai vì đoán sẽ báo động giả hàng loạt — đã xảy ra: một rule đoán độ dài làm 96% số dòng bị
đánh dấu vi phạm.

### 2.8. Quét theo HỌ LỖI — bắt buộc làm cho TỪNG checkpoint

Mục 2.1-2.7 đi từ *tín hiệu* (lineage nói gì) ra rule. Mục này đi từ *kiểu hỏng* ra rule. Phải
làm cả hai, vì chúng bỏ sót những thứ khác nhau: đi từ tín hiệu thì chỉ thấy cột nào bị biến
đổi, không thấy được những kiểu hỏng không gắn với một phép biến đổi nào.

Sáu họ dưới đây là danh sách ĐÓNG. Với mỗi checkpoint, duyệt đủ sáu họ. Họ nào không sinh rule
thì phải nêu lý do trong `overall_impact_warning` — **không được im lặng bỏ qua**.

| Họ | Câu hỏi nó trả lời | kind |
|---|---|---|
| `completeness` | Giá trị có mặt không? | row |
| `validity` | Giá trị có đúng dạng / đúng miền không? | row |
| `uniqueness` | Khoá có bị trùng không? | dataset |
| `referential` | Khoá có khớp được bảng tra không? | row (sau join) |
| `consistency` | Các cột có mâu thuẫn nhau không? | row |
| `distribution` | Cả bảng có lệch bất thường không? | dataset |

**`completeness`** — NULL, chuỗi rỗng, giá trị thay-cho-thiếu (`'unknown'`, `-1`, `0` khi 0 vô
nghĩa). Đừng chỉ kiểm NULL: nếu pipeline dùng `coalesce(x,'unknown')` thì NULL không bao giờ
xuất hiện, phải đếm tỉ lệ `= 'unknown'` mới đo được chất lượng join.

**`validity`** — độ dài, regex, tập giá trị, dải số, `try_cast` thất bại. Lấy hằng số từ
`domain_constants` trong YAML, đừng đoán. Đoán sai một lần đã làm 96% số dòng bị báo vi phạm.

**`uniqueness`** — `count(DISTINCT k) <> count(k)`, `kind: "dataset"`,
`observed_expr = count(k) - count(DISTINCT k)` và **`observed_unit: "rows"`** — con số đó là
SỐ DÒNG thừa, khai đúng thì điểm phản ánh 1.88% thay vì 100%. **Tính duy nhất phụ
thuộc checkpoint**: một khoá có thể được phép trùng ở stage giữa (sau `explode`) rồi phải trở
lại duy nhất sau `groupBy`. Đọc kỹ description của cột và `structural_invariants` để biết đặt ở
checkpoint nào — đặt sai chỗ là rule báo động giả trên dữ liệu hoàn toàn hợp lệ.

**`referential`** — dòng mồ côi sau LEFT JOIN: `join_key IS NOT NULL AND right_col IS NULL`.
Đặt ở checkpoint ngay SAU join — đây là ngoại lệ 2 của quy tắc đặt sớm nhất (SKILL.md mục 2),
không phải phá lệ.

**`right_col IS NULL` TRẦN KHÔNG PHẢI rule referential, và cũng KHÔNG tính là đã phủ cột đó.**
Đây là lỗi đã xảy ra thật. Agent sinh ra:

```
JNMAP1_ORIG_MSN_NULL:  original_msisdn IS NULL          <- SAI
```

Rule này bắt 15 dòng và kết luận "thiếu dữ liệu ở original_msisdn". Nhưng dữ liệu không
thiếu — nó CHƯA TỪNG TỒN TẠI, vì `imsi_new = '452049999'` không có trong bảng mapping (đã
kiểm: bảng 600 dòng, 0 dòng khớp). Rule đổ lỗi cho cột đích thay vì cho việc tra cứu trượt.

Dạng đúng phân biệt được ba tình huống mà `IS NULL` trần gộp làm một:

| khoá | cột vế phải | nghĩa | họ |
|---|---|---|---|
| NULL | NULL | nguồn thiếu khoá | `completeness`, đặt TRƯỚC join |
| có giá trị | NULL | **bảng tra thiếu bản ghi** | `referential`, đặt SAU join |
| có giá trị | có giá trị | bình thường | — |

Khi một cột đến từ vế phải của LEFT JOIN, rule `completeness` cho nó phải đặt ở checkpoint
TRƯỚC join (trên cột nguồn), còn ở checkpoint sau join thì viết dạng mồ côi.

### Cột có điều kiện: NULL là giá trị đúng

Cùng một kiểu sai với `IS NULL` trần, nhưng nguyên nhân khác. Cột sinh bởi
`when(<điều kiện>, ...).otherwise(null)` chỉ có giá trị ở những dòng thoả điều kiện — và
điều kiện thường hiếm. NULL ở các dòng còn lại không phải thiếu dữ liệu, đó là **câu trả
lời đúng**.

Đã xảy ra trên `cell_capacity_daily`. `Congestion.java:90` viết:

```java
when(is_congested, ...).otherwise(null)
```

Agent sinh năm rule `incident_hours IS NULL OR ...`, `incident_seq IS NULL OR ...`. Trino
xác nhận 94,2% số dòng bị báo vi phạm — đúng bằng tỷ lệ dòng KHÔNG tắc nghẽn. Dữ liệu
không hỏng chỗ nào cả.

Nhận ra bằng `kinds` chứa `CONDITIONAL` trong lineage, hoặc bằng chính tên cột khi nó nói
về một sự kiện thỉnh thoảng mới xảy ra.

Hai dạng viết đúng:

| ý định | biểu thức |
|---|---|
| NULL hợp lệ, chỉ kiểm miền giá trị | `col IS NOT NULL AND col < 0` |
| NULL chỉ sai khi điều kiện bật | `is_congested AND col IS NULL` |

Cột **không có trong lineage và không có mô tả nghiệp vụ** thì bạn chỉ biết tên và kiểu —
chưa đủ để khẳng định nó bắt buộc NOT NULL. Dùng dạng `IS NOT NULL AND ...` và để phần NULL
lại cho lượt sau.

SKILL.md nói "đo chất lượng join thì đếm TỶ LỆ không khớp, đừng coi từng dòng không khớp là
lỗi". Rule này làm đúng như vậy: `violation_pct` của nó CHÍNH LÀ tỷ lệ mồ côi. Điều cần tránh
là đặt `NOT NULL` trần cho cột vế phải — cái đó coi mỗi dòng không khớp là một lỗi.

Mồ côi vài phần trăm thường là chuyện bình thường, nên `importance_weight` vừa phải; cái đáng
báo động là tỉ lệ NHẢY VỌT so với mọi khi, và đó là việc của `distribution`.

**`consistency`** — quan hệ giữa các cột trong cùng một dòng: thứ tự thời gian
(`start <= end`), quan hệ tập con giữa các cột đếm (xem 2.4), cột dẫn xuất khớp cột nguồn
(`LENGTH(tac_device) <> 8` khi `tac_device = substring(imeisv,1,8)`). Viết dạng NULL-safe:
`a IS NOT NULL AND b IS NOT NULL AND a > b` — nếu một vế NULL thì đó là việc của
`completeness`, không phải của rule này.

**`distribution`** — `kind: "dataset"`, ngưỡng trên toàn bảng: số dòng nở ra sau join,
`avg`/`sum` ra ngoài dải hợp lý, tỉ lệ mồ côi vượt ngưỡng. Chỉ sinh khi có căn cứ đọc được từ
`domain_constants` hoặc từ công thức trong lineage. Không tự bịa ngưỡng — ngưỡng bịa là rule
báo động giả mỗi ngày, và rule báo động giả thì sau vài lần không ai đọc nữa.

Hai họ cuối cùng hay bị bỏ sót nhất, và đó đúng là hai họ mà rule mức dòng không thay thế được.

### 2.9. Ví dụ đã kiểm chứng trên chính pipeline này

Mọi biểu thức dưới đây đã CHẠY THẬT trên Spark 4.1.2 và cho ra con số nêu kèm. Dùng chúng làm
khuôn, đừng chép nguyên xi sang cột khác mà không kiểm kiểu dữ liệu và `domain_constants`.

**`completeness`**

```
src_calls    CALL_ID_NULL        call_id IS NULL
src_calls    IMSI_NEW_NULL       imsi_new IS NULL OR TRIM(imsi_new) = ''
src_infra    CITY_CODE_NULL      city_code IS NULL
jn_infra     PROV_UNKNOWN_RATE   count(*) FILTER (WHERE end_province = 'unknown') * 100.0 / count(*)
                                 kind=dataset, observed_unit=percent
```

Dòng cuối là mẹo quan trọng: `end_province = coalesce(city_code,'unknown')` nên **không bao
giờ NULL**. Rule `IS NULL` ở đó là rule chết. Muốn đo thiếu dữ liệu thì đếm tỷ lệ giá trị
thay-cho-thiếu. Áp dụng cho mọi cột có `coalesce`, `nvl`, `ifnull` trong lineage.

**`validity`**

```
src_calls    IMSI_NEW_FORMAT     imsi_new IS NULL OR LENGTH(imsi_new) <> 15
                                   OR imsi_new NOT RLIKE '^[0-9]+$'        -> bat 15 dong
src_infra    INFRA_LAC_RANGE     infra_lac IS NULL OR infra_lac < 0 OR infra_lac > 65535
src_calls    START_TIME_SIGN     call_start_time IS NULL OR call_start_time < 0
tf_final     DATE_TIME_FORMAT    date_time IS NULL OR LENGTH(date_time) <> 10
```

`15` lấy từ `domain_constants.imsi_length`, `65535` từ `lac_range`, `10` từ công thức lineage
`substring(date_hour, 1, 10)`. Không con số nào là phỏng đoán — đó là điều kiện để được sinh
rule miền giá trị.

**`uniqueness`** — luôn `kind=dataset`, `observed_unit=rows`

```
src_calls    SRC_CALL_ID_UNIQUE      failed   : count(DISTINCT call_id) <> count(call_id)
                                     observed : count(call_id) - count(DISTINCT call_id)
                                     expected : = 0            -> do duoc 9 dong trung
tf_final     FINAL_CALL_ID_UNIQUE    (nhu tren)
```

Đặt ở đâu là câu hỏi riêng: description của `call_id` nói *"sau explode ở Step 5 một call_id
có thể ứng với nhiều dòng trung gian, nhưng sau groupBy ở Step 9 phải trở lại duy nhất"*. Nên
duy nhất ở `src_calls` (trước explode) và ở `tf_final` (sau groupBy) — KHÔNG đặt ở các
checkpoint giữa hai mốc đó.

**`referential`** — hai tầng, đặt ở checkpoint NGAY SAU join

```
jn_map1   MSISDN_ORPHAN       original_imsi IS NOT NULL AND original_msisdn IS NULL
                              kind=row     -> violation_pct CHINH LA ty le mo coi (15/480)
jn_map1   MSISDN_ORPHAN_RATE  count(*) FILTER (WHERE original_msisdn IS NULL)*100.0/count(*) > 5
                              kind=dataset, observed_unit=percent, expected "<= 5"
```

Tầng `row` ghi nhận, tầng `dataset` báo động khi tỷ lệ vượt ngưỡng. Mồ côi vài phần trăm là
bình thường; cái đáng báo là nhảy vọt.

**`consistency`** — so cột với cột, viết NULL-safe

```
src_calls  CALL_TIME_ORDER    call_start_time IS NOT NULL AND call_end_time IS NOT NULL
                                AND call_start_time > call_end_time
src_calls  CS_TEARDOWN_ORDER  cc_disconnect_event_time IS NOT NULL
                                AND cc_release_event_time IS NOT NULL
                                AND cc_disconnect_event_time > cc_release_event_time
tf_final   LAC_SAC_PAIRED     (start_lac IS NULL) <> (start_sac IS NULL)
```

Hai rule đầu lấy thẳng từ `structural_invariants`. Rule thứ ba là dạng "hai cột phải cùng có
hoặc cùng không" — hữu ích cho mọi cặp toạ độ/khoá ghép. Dùng `IS NOT NULL AND ...` chứ đừng
`IS NULL OR ...`: thiếu giá trị là việc của `completeness`, rule này chỉ nói về mâu thuẫn.

**`distribution`** — luôn `kind=dataset`

```
tf_decode  DECODE_ROW_LOSS   count(*) < 100          observed count(*)      unit=value
jn_map2    FANOUT_GUARD      count(*) > 100000       observed count(*)      unit=value
src_calls  DURATION_SANE     avg(call_duration) NOT BETWEEN 0 AND 7200000
                             observed avg(call_duration)                    unit=value
```

Ngưỡng phải có căn cứ: gọi `check_volume_anomaly()` để xem các lần chạy trước ở khoảng nào rồi
mới chọn. Ngưỡng bịa ra là rule báo động giả mỗi ngày, và rule báo động giả thì sau vài lần
không ai đọc nữa.

**KHÔNG viết — đều là lỗi đã trả giá thật**

| Rule sai | Vì sao | Viết đúng là |
|---|---|---|
| `TRIM(call_id) = ''` | `call_id` kiểu `int`, hàm chuỗi trên cột số không bao giờ kích hoạt | `call_id IS NULL` |
| `p_hour NOT RLIKE '^[0-9]{2}$'` | nguồn lưu `"8"` không phải `"08"`; chính pipeline mới `lpad`. Vi phạm 500/500 trên dữ liệu hợp lệ | `p_hour NOT RLIKE '^[0-9]{1,2}$'` |
| `end_province IS NULL` | sau `coalesce(...,'unknown')` thì không bao giờ NULL | đếm tỷ lệ `= 'unknown'` |
| `original_msisdn IS NULL` | vế phải LEFT JOIN, NULL là hợp lệ | dạng mồ côi ở trên |
| `out_lac IS NULL` ở `jn_infra` | Step 5 đã lọc hết dòng không giải mã được | đặt ở `tf_decode`, hoặc bỏ |
| `count(call_id) < 0` | `COUNT()` không bao giờ âm | bỏ hẳn |
| `count(DISTINCT date_time) <> count(date_time)` ở một bảng gộp theo ngày | `date_time` là cột NGÀY, hàng triệu thuê bao cùng chia một ngày. Vi phạm 100% trên dữ liệu hoàn toàn đúng | duy nhất là của KHOÁ NHÓM đầy đủ `(date_time, msisdn, imsi, imeisv, tac_device)`, và chỉ ở chặng SAU `groupBy` |

Bốn dòng đầu đều có bằng chứng sẵn trong `domain_constants`, `structural_invariants`, hoặc
biểu thức lineage. Đọc chúng trước khi viết rule sẽ tránh được cả sáu.

Dòng cuối là kiểu sai NGUY HIỂM NHẤT và nó sinh ra từ việc cố cho đủ họ: pipeline chỉ có ba
checkpoint, agent thấy `uniqueness` còn trống ở `tf_derive` nên bịa một rule duy nhất trên cột
đầu tiên nhìn thấy. **Một họ không áp dụng được thì bỏ trống và nêu lý do — ép cho đủ sẽ tạo
false positive, mà một rule luôn vi phạm còn tệ hơn một họ để trống: nó kéo điểm xuống mỗi lần
chạy, làm người đọc mất lòng tin vào toàn bộ bảng điểm, và che mất lỗi thật.**

### Đừng lặp cùng một phép kiểm ở nhiều checkpoint

`msisdn IS NULL` ở cả `src_ebs`, `tf_derive` và `tf_final` là ba rule cho một câu hỏi. Chỉ lặp
lại khi giữa hai checkpoint có một phép biến đổi **có thể làm hỏng chính cột đó** — join (sinh
NULL ở vế phải), explode, cast, `coalesce`, hay một `groupBy` đổi ý nghĩa của cột. Cột đi thẳng
qua không ai đụng tới thì kiểm một lần ở chặng SỚM NHẤT nó xuất hiện là đủ: bắt được sớm hơn,
và mẫu số của các chặng sau không bị pha loãng bởi những rule chắc chắn sạch.

## 3. Chọn importance_weight

| Weight | Khi nào |
|---|---|
| 5 | cột GROUP_BY (grain), khoá định danh, KPI chính |
| 4 | cột có cột khác dẫn xuất từ nó, bất biến giữa các cột aggregate |
| 3 | cột dẫn xuất (substring/cast) |
| 1-2 | cột mô tả, ít ảnh hưởng hạ nguồn |

Nếu `check_volume_anomaly` báo bất thường ở cột liên quan → cân nhắc tăng 1 bậc.

## 4. Fallback khi thiếu lineage

Không có lineage (job chưa chạy lần nào, Marquez trống) → **chỉ** sinh business rules từ YAML.
Nêu rõ trong `overall_impact_warning` rằng chưa có lineage nên chưa kiểm được các bất biến
giữa cột. Bỏ toàn bộ mục 2.1-2.6.

## 5. Ràng buộc kỹ thuật bắt buộc

`failed_sql_expr` viết theo `kind` — xem dq-sql-generator/OUTPUT_FORMAT.md, mục "`kind` quyết
định biểu thức được viết thế nào". Tóm tắt:

- `kind: "row"` (mặc định): biểu thức MỨC DÒNG, cấm aggregate, bắt buộc bẫy NULL.
- `kind: "dataset"`: biểu thức LÀ aggregate trả boolean, bắt buộc kèm `observed_expr` +
  `expected`. Đây là cách viết tính duy nhất và các ngưỡng trên toàn bảng.

Cả hai kind đều cấm window và subquery sang bảng khác.

Tài liệu này trước đây ghi "không aggregate, tính duy nhất không viết được". Đã lạc hậu: câu đó
đúng khi rule chạy bằng `observe()`, còn checkpoint hiện dùng `df.agg()`. Hệ quả của việc không
cập nhật: suốt thời gian đó không rule duy nhất nào được sinh ra, và dữ liệu trùng khoá lọt
xuống bảng đích.

## 6. Hand-off và đóng gói

- Phối hợp dq-sql-generator: mỗi rule có `failed_sql_expr` inverted logic; rule `kind: "row"`
  luôn bẫy NULL, rule `kind: "dataset"` thì không cần.
- Trả đúng JSON schema trong OUTPUT_FORMAT.md, không markdown wrapper.
