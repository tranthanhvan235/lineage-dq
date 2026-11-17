# Schema kho Iceberg

Ba file trong thư mục này đều **sinh ra** từ `src/main/java/.../DqTables.java`. Đừng sửa tay —
sửa Java rồi chạy:

```bash
python3 scripts/gen_schema_docs.py --write     # sinh lại
python3 scripts/gen_schema_docs.py --check     # đối chiếu, thoát 1 nếu lệch (dùng cho CI)
```

| file | dùng ở đâu |
|---|---|
| `dq_schema.dbml` | dán vào [dbdiagram.io](https://dbdiagram.io/d) → sơ đồ ERD, tự xếp hình, kéo thả được |
| `dq_schema.mmd` | Mermaid — GitHub/GitLab render thẳng trong `.md`, không cần cài gì |
| `dq_schema.sql` | DDL Trino/Iceberg — nạp vào DBeaver, hoặc `Import → PostgreSQL` trên dbdiagram |

## Chọn công cụ nào

**dbdiagram.io** — nhanh nhất để có cái nhìn kiểu thiết kế CSDL. Mở trang, xoá nội dung mẫu,
dán toàn bộ `dq_schema.dbml`. Xuất được PNG/PDF/SVG. Không cần tài khoản để xem.

**Mermaid** — không phải cài hay mở trang nào. Bọc nội dung `dq_schema.mmd` trong khối
` ```mermaid ` là GitHub, GitLab và Claude artifact đều vẽ. Hợp để nhúng vào tài liệu.
Đổi lại, không kéo thả và sơ đồ 11 bảng sẽ khá cao.

**DBeaver** — nếu muốn xem sơ đồ của bảng **đang chạy trên cụm** chứ không phải bản khai:
tạo connection Trino tới `localhost:30808`, mở schema `iceberg.warehouse_c` (hoặc `warehouse_b`,
`warehouse_a`), chọn các bảng `dq_*` rồi `View Diagram`. DBeaver không đoán được khoá ngoài
(Iceberg không có), nên các đường quan hệ phải tự nối — đó là lý do `dq_schema.dbml` vẫn
đáng dùng hơn cho việc *hiểu* cấu trúc.

## Ba điều cần biết trước khi đọc sơ đồ

**Không có khoá ngoại.** Iceberg không hỗ trợ. Mọi quan hệ trong `dq_schema.dbml` là *quy ước
nối bảng khi truy vấn*, không phải ràng buộc được cơ sở dữ liệu ép. Chúng đã được đối chiếu
với dữ liệu thật trên `iceberg.warehouse_b` ngày 22/09/2026; số dòng nối được ghi trong chú thích
từng `Ref`.

**Khoá bên cha không phải lúc nào cũng duy nhất.** `dq_report_history` có nhiều dòng cùng
`run_id` — mỗi rule một dòng. Nối từ `dq_analysis` sang mà quên `DISTINCT run_id` là nhân bản
kết quả.

**`dq_key` có mặt ở cả 11 bảng** nhưng không được vẽ thành đường quan hệ. Nó là trục phân vùng
theo pipeline; vẽ đủ 11 đường thì sơ đồ thành mạng nhện và che mất các quan hệ thật.

## Bảng thật trên cụm lệch gì so với bản khai

Đối chiếu ngày 22/09/2026 (`DESCRIBE` từng bảng trên cả ba schema):

- `iceberg.warehouse_c.dq_analysis` **chưa tồn tại**. Bảng được tạo lúc ghi lần đầu, mà
  `cell_capacity_daily` chưa từng chạy agent phân tích. Không phải lỗi.
- `iceberg.warehouse_a.dq_rules` và `dq_report_history` **đủ cột nhưng khác thứ tự** — phần đuôi
  đang xếp theo vần. `IcebergSink` ghi bằng `writeTo(...).append()` (DataFrameWriterV2), khớp
  **theo tên** chứ không theo vị trí, nên lệch thứ tự không ảnh hưởng dữ liệu.
- Tám schema/bảng còn lại khớp hoàn toàn với `DqTables.java`.

## Thêm cột thì làm gì

`CREATE TABLE IF NOT EXISTS` **không bao giờ** thêm cột vào bảng đã có. Thêm cột mới vào
`DqTables.java` thì phải chạy `ALTER TABLE ... ADD COLUMN` một lần cho từng schema đã tồn tại,
nếu không `append` bị từ chối và `IcebergSink` nuốt lỗi — job vẫn xanh, chỉ là thiếu dòng.
Vì `ALTER` luôn thêm vào cuối, cột mới trong Java cũng nên đặt ở cuối.
