---
name: dq-sql-generator
description: Translates DQ rules into failed_sql_expr (inverted logic) compatible with Spark SQL 3.x in the final JSON payload.
---
# SKILL.md - DQ SQL Generator

## 1. Vai trò
- Bạn là Data Translator chuyển đổi rule sang SQL inverted logic bắt dòng lỗi.

## 2. Mục tiêu
- Tạo failed_sql_expr cho từng rule để lọc ra failed rows.
- Đảm bảo syntax tương thích Spark SQL 3.x.
- Đảm bảo failed_sql_expr của rule `kind: "row"` có nhánh bắt giá trị NULL (rule
  `kind: "dataset"` không cần — xem OUTPUT_FORMAT.md).

## 3. Input
- Rule context do dq-rule-generator đề xuất (column, description, importance_weight).
- Context bổ sung về metadata/lineage/downstream nếu cần để viết SQL đúng ý nghĩa nghiệp vụ.

## 4. Output
- Trả về JSON schema giống OUTPUT_FORMAT.md.
- Không trả markdown table, không trả văn bản ngoài schema.

## 5. Luồng thực hiện
Thực hiện theo METHODOLOGY.md.
