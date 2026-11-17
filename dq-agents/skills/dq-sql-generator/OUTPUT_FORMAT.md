# Output Format - DQ SQL Generator

Trả về JSON nguyên chất (không markdown) với đúng schema sau:

{
	"overall_impact_warning": "Cảnh báo 1-2 câu về hậu quả hạ nguồn nếu dữ liệu sai.",
	"rules": [
		{
			"rule_id": "CELL_ID_NULL",
			"column": "cell_id",
			"description": "Khóa chính định danh bị null hoặc rỗng.",
			"failed_sql_expr": "cell_id IS NULL OR TRIM(cell_id) = ''",
			"importance_weight": 5
		},
		{
			"rule_id": "CALL_ID_UNIQUE",
			"column": "call_id",
			"description": "call_id phải duy nhất sau bước gộp.",
			"kind": "dataset",
			"failed_sql_expr": "count(DISTINCT call_id) <> count(call_id)",
			"observed_expr": "count(call_id) - count(DISTINCT call_id)",
			"expected": "= 0",
			"importance_weight": 5
		}
	]
}

## Bắt buộc
- failed_sql_expr phải là inverted logic để bắt dòng lỗi.
- SQL phải tương thích Spark SQL 3.x trở lên.
- importance_weight là số nguyên trong [1, 5].
- Chọn đúng `kind` — xem mục dưới. Chọn sai thì rule bị loại lúc chạy và stage mất phép đo.

## `kind` quyết định biểu thức được viết thế nào

Đây là điều quan trọng nhất của tài liệu này. Framework thực thi HAI kiểu rule khác hẳn nhau,
và một biểu thức hợp lệ với kiểu này là lỗi với kiểu kia.

| | `kind: "row"` (mặc định) | `kind: "dataset"` |
|---|---|---|
| Executor bọc lại thành | `count(when(<expr>, 1))` | **không bọc** — dùng thẳng `<expr>` làm aggregate |
| `<expr>` phải trả về | boolean cho MỘT dòng | boolean cho CẢ BẢNG |
| Aggregate trong `<expr>` | **cấm** — thành aggregate lồng aggregate | **bắt buộc** — không có thì lỗi |
| Bẫy NULL | bắt buộc | không áp dụng |
| Kết quả đo | số dòng vi phạm | có/không, quy đổi 0% hoặc 100% |

Hai kiểu đều đo bằng `df.agg()` trong **cùng một action** với các rule khác của stage.

### `kind: "row"` — viết được

| Loại | Ví dụ |
|---|---|
| NULL / rỗng | `imsi IS NULL OR TRIM(imsi) = ''` |
| Độ dài | `imsi IS NULL OR LENGTH(imsi) <> 15` |
| Khoảng giá trị | `volte_call_count IS NULL OR volte_call_count < 0` |
| Định dạng / regex | `imsi IS NULL OR imsi NOT RLIKE '^[0-9]+$'` |
| Tập giá trị | `imsi IS NULL OR substr(imsi,1,5) NOT IN ('45204','45201')` |
| So sánh CỘT VỚI CỘT cùng dòng | `apn_ims_count < volte_call_count` |
| Mồ côi sau LEFT JOIN | `join_key IS NOT NULL AND right_col IS NULL` |

Cách tự kiểm: che hết bảng, chỉ để lộ một dòng. Trả lời được dòng này đúng hay sai → `row`.

### `kind: "dataset"` — viết được

`<expr>` là aggregate trả boolean. Bắt buộc kèm `observed_expr` (aggregate trả CON SỐ),
`expected` (ngưỡng đọc được) và `observed_unit` — boolean chỉ nói hỏng hay không, không nói
hỏng bao nhiêu.

`observed_unit` quyết định ĐIỂM, không chỉ để hiển thị:

| giá trị | khi nào | điểm tính thế nào |
|---|---|---|
| `rows` | `observed_expr` trả về SỐ DÒNG vi phạm | `observed / total_rows × 100` |
| `percent` | `observed_expr` đã trả về 0–100 | lấy thẳng |
| `value` | đơn vị khác (`avg`, `max`, mili giây…) | không quy đổi được → vi phạm = 100% |

Khai sai hoặc bỏ trống là mất thông tin thật: 9 dòng trùng khoá trên 480 dòng sẽ bị tính
thành hỏng 100% thay vì 1.88%, và điểm của checkpoint ít rule bị một rule lấn át hoàn toàn.

| Loại | failed_sql_expr | observed_expr | observed_unit |
|---|---|---|---|
| Khoá duy nhất | `count(DISTINCT call_id) <> count(call_id)` | `count(call_id) - count(DISTINCT call_id)` | `rows` |
| Nở dòng sau join | `count(*) > 1000000` | `count(*)` | `value` |
| Ngưỡng trên tổng | `sum(duration) > 86400000` | `sum(duration)` | `value` |
| Trung bình lệch dải | `avg(duration) NOT BETWEEN 1 AND 7200` | `avg(duration)` | `value` |
| Tỉ lệ mồ côi quá cao | `count(*) FILTER (WHERE original_msisdn IS NULL) * 100.0 / count(*) > 5` | `count(*) FILTER (WHERE original_msisdn IS NULL) * 100.0 / count(*)` | `percent` |

`count(DISTINCT ...)` và `FILTER (WHERE ...)` **chạy được** ở đây. Đã đo trực tiếp trên Spark
4.1.2, kết quả lưu ở `probe-observe-vs-agg.json`: qua `df.agg()` cả hai đều `ok: true`.

### KHÔNG viết được — cả hai kind

| Loại | Ví dụ SAI | Lỗi Spark (đã đo) |
|---|---|---|
| Aggregate trong rule `kind: "row"` | `sum(x) > 1000` mà quên khai `kind` | `NESTED_AGGREGATES_UNSUPPORTED` |
| So sánh giữa các DÒNG | `lag(x) OVER (ORDER BY d)` | `WINDOW_EXPRESSIONS_UNSUPPORTED` |
| Truy vấn sang BẢNG KHÁC | `msisdn NOT IN (SELECT ... FROM other)` | `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY` |

Toàn vẹn tham chiếu **không** cần subquery: đặt rule ở checkpoint SAU phép join, lúc đó cả hai
vế đã nằm chung một dataframe, và dòng mồ côi nhận ra bằng `join_key IS NOT NULL AND right_col
IS NULL` — một biểu thức mức dòng bình thường.

### Vì sao tài liệu này từng nói ngược lại

Bản trước ghi "mọi aggregate đều hỏng, tính duy nhất không viết được". Đúng ở thời điểm đó: rule
chạy bằng `Dataset.observe()`, và `observe()` từ chối `count(DISTINCT)` lẫn `FILTER`. Từ khi
checkpoint chuyển sang `DqMetricQuery` dùng `df.agg()`, giới hạn ấy không còn, nhưng tài liệu
không được cập nhật — hệ quả là bộ rule sinh ra suốt thời gian đó **không có rule duy nhất nào**,
và dữ liệu trùng khoá đã lọt xuống bảng đích mà không rule nào thấy.

`Dataset.observe()` vẫn còn giới hạn cũ. Nếu có đường nào quay lại dùng `observe()` thì bảng
"KHÔNG viết được" phải áp dụng lại cho đường đó.

## Không hợp lệ
- Markdown table.
- JSON đặt trong code fence.
- Rule không có failed_sql_expr.
- Rule `kind: "dataset"` thiếu `observed_expr`, `expected` hoặc `observed_unit`.
