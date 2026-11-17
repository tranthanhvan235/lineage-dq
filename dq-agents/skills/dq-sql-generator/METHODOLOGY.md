# Methodology - DQ SQL Generator

## 1. Receive Rule Intents
- Nhận rule context (rule_id, column, description, importance_weight).
- Hiểu ý nghĩa nghiệp vụ để tránh viết SQL đúng kỹ thuật nhưng sai logic.

## 2. Translate To Inverted Logic
- Chuyển kỳ vọng "dữ liệu đúng" thành điều kiện "dữ liệu sai".
- Rule mức dòng (`kind: "row"`) phải có nhánh bắt NULL. Rule mức tập (`kind: "dataset"`) thì
  không — biểu thức của nó là aggregate trên cả bảng, bắt NULL ở đó vô nghĩa.
- Mau co ban mức dòng:
  - Range check: col IS NULL OR col < min OR col > max
  - Non-empty text: col IS NULL OR TRIM(col) = ''
  - Set membership: col IS NULL OR col NOT IN (...)
  - Orphan sau LEFT JOIN: join_key IS NOT NULL AND right_col IS NULL
- Mau co ban mức tập:
  - Khoá duy nhất: count(DISTINCT k) <> count(k)
  - Ngưỡng trên tổng: sum(col) > N

## 3. Spark SQL 3.x Compliance
- Ưu tiên syntax Spark SQL 3.x.
- Tránh cú pháp đặc thù của hệ khác nếu không cần thiết.
- Nếu dùng regex, ưu tiên mẫu an toàn, rõ ràng.

## 4. Quality Guardrails
- Không sinh SQL vô hiệu (ví dụ luôn FALSE/TRUE).
- failed_sql_expr cần ngắn gọn, dễ hiểu và tiếp cận trực tiếp trên cột đầu vào.
- Giữ nguyên importance_weight từ rule context.

## 5. Final Packaging
- Đóng gói vào JSON cuối theo OUTPUT_FORMAT.md.
- Không markdown wrapper.
