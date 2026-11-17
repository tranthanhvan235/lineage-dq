# Output Format - DQ Rule Generator

Trả về JSON nguyên chất (không markdown) với đúng schema sau:

{
	"overall_impact_warning": "Cảnh báo 1-2 câu về hậu quả hạ nguồn nếu dữ liệu sai, kèm các HỌ LỖI đã bỏ trống và lý do.",
	"rules": [
		{
			"stage": "src_calls",
			"rule_id": "RSRP_OUT_OF_BOUNDS",
			"column": "p_cell_rsrp",
			"description": "Giá trị nằm ngoài dải hợp lệ nghiệp vụ.",
			"failed_sql_expr": "p_cell_rsrp IS NULL OR p_cell_rsrp < -140 OR p_cell_rsrp > -44",
			"importance_weight": 5
		},
		{
			"stage": "tf_final",
			"rule_id": "CALL_ID_UNIQUE_AFTER_GROUPBY",
			"column": "call_id",
			"description": "Sau bước gộp, mỗi cuộc gọi chỉ được còn một dòng.",
			"kind": "dataset",
			"failed_sql_expr": "count(DISTINCT call_id) <> count(call_id)",
			"observed_expr": "count(call_id) - count(DISTINCT call_id)",
			"expected": "= 0",
			"observed_unit": "rows",
			"importance_weight": 5
		}
	]
}

## Bắt buộc mọi rule
- `stage`: tên một checkpoint CÓ THẬT trong mục 0 của input, và cột phải tồn tại ở checkpoint đó.
- `rule_id`: string, không trùng trong cùng một stage.
- `column`: tên cột đầu vào bị kiểm tra.
- `description`: mô tả logic lỗi theo nghiệp vụ.
- `failed_sql_expr`: biểu thức SQL inverted logic, tương thích Spark SQL 3.x trở lên.
- `importance_weight`: số nguyên trong [1, 5].

## Bắt buộc riêng theo `kind`

| | `kind: "row"` (mặc định, có thể bỏ trống) | `kind: "dataset"` |
|---|---|---|
| `failed_sql_expr` | biểu thức MỨC DÒNG, **phải bắt được NULL** | aggregate trả boolean, KHÔNG cần bắt NULL |
| `observed_expr` | không dùng | **bắt buộc** — aggregate trả con số thật |
| `expected` | không dùng | **bắt buộc** — ngưỡng kỳ vọng đọc được, vd `"= 0"` |
| `observed_unit` | không dùng | **bắt buộc** — `rows` / `percent` / `value`. Quyết định điểm |

Chi tiết cách viết từng loại: dq-sql-generator/OUTPUT_FORMAT.md.

## Không hợp lệ
- Markdown table.
- JSON nằm trong code fence.
- Thiếu `stage`, `failed_sql_expr` hoặc `importance_weight`.
- Rule `kind: "dataset"` thiếu `observed_expr`, `expected` hoặc `observed_unit`.
