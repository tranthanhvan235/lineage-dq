# METHODOLOGY — DQ Rule Verifier

## 0. Đọc context theo thứ tự này
1. `summary.hard_problems` — máy bắt được bao nhiêu lỗi cứng. Bằng 0 nghĩa là phần còn lại
   hoàn toàn thuộc về bạn.
2. `summary.stages_with_rules` so với `summary.stages_in_manifest` — có checkpoint nào không
   rule nào không. Một checkpoint 0 rule sẽ báo `NO_RULES_APPLIED`, không phải `PASS`.
3. Từng stage trong `deterministic`, theo thứ tự pipeline.
4. Toàn bộ rule — chỉ đọc kỹ những rule mà ba bước trên đã khiến bạn nghi ngờ.

## 1. Rule chết vì phép biến đổi
Rule đúng cú pháp, chạy được, nhưng không bao giờ kích hoạt. Checkpoint vẫn báo điểm đẹp
trong khi thực tế không kiểm được gì — đây là kiểu hỏng nguy hiểm nhất vì nó im lặng.

**Dạng hay gặp nhất**: kiểm NULL trên cột đã qua một phép điền giá trị mặc định. Sau
`coalesce(city_code, 'unknown')` thì cột không bao giờ NULL nữa, nên `end_province IS NULL`
là rule chết vĩnh viễn. Muốn bắt lỗi thật thì kiểm CHÍNH giá trị mặc định đó
(`end_province = 'unknown'`), hoặc kiểm cột nguồn ở checkpoint TRƯỚC phép biến đổi.

**Các dạng khác đáng nghi**:
- so sánh miền giá trị trên cột mà pipeline vừa chuẩn hoá (`lpad`, `upper`, `round`) — rule
  đang kiểm đúng cái mà bước trước vừa đảm bảo
- điều kiện mâu thuẫn trong một biểu thức `kind: row`: hai nhánh `AND` loại trừ nhau
- ngưỡng đặt ngoài miền vật lý của cột: `p_cell_rsrp > 0` khi RSRP luôn âm

**GIỚI HẠN BẰNG CHỨNG — quan trọng**: bạn KHÔNG được cấp column lineage. Chỉ nêu nghi vấn khi
mô tả của rule, hoặc chính biểu thức, đã lộ ra phép biến đổi. **Đừng suy đoán phép biến đổi từ
tên cột.** Một nghi vấn không có căn cứ sẽ khiến Gen Rules sinh lại một bộ rule không tốt hơn,
và tiêu mất một lượt regen trong số lượt hữu hạn.

Khi không chắc: hạ stage đó xuống `WEAK` và ghi nghi vấn vào `notes`. Đừng `FAIL`.

## 2. Đặt sai chỗ về nghiệp vụ
Máy đã bắt được rule đặt ở stage mà cột không tồn tại. Bạn bắt phần còn lại: rule đặt ở
checkpoint mà cột ĐÃ tồn tại từ checkpoint sớm hơn.

Bắt lỗi ở nguồn quét ít dòng hơn và chỉ đúng thủ phạm. Ví dụ thật: `city_code` kiểm ở
`src_infra` là 40 dòng và kết luận "bảng hạ tầng hỏng"; đợi tới `jn_infra` là 480 dòng và chỉ
kết luận được "hỏng ở đâu đó trong mấy bước trước".

**Ba trường hợp đặt muộn là ĐÚNG**, đừng bắt lỗi chúng:
1. cột chỉ xuất hiện lần đầu ở checkpoint đó;
2. phép biến đổi tại đó có thể làm hỏng giá trị (`CAST` sinh NULL, UDF, join sinh NULL ở vế
   phải) — kiểm KẾT QUẢ sau biến đổi mới có nghĩa;
3. rule `kind: dataset` về chính phép biến đổi đó (khoá còn duy nhất sau join, số dòng nở ra).

**Sau LEFT JOIN, NULL ở vế phải là HỢP LỆ.** Một rule `NOT NULL` đặt ngay sau join sẽ báo sai
hàng loạt bản ghi không mapping được. Nếu thấy dạng này, đó là lỗi thật — nhưng hãy nói rõ
trong `must_fix` rằng cách đúng là đo TỶ LỆ không khớp bằng rule `kind: dataset`, không phải
bỏ rule đi.

## 3. Đọc độ phủ cho đúng
`uncovered_columns` là danh sách thô. Không phải cột nào trống cũng đáng lo:

| Đáng lo | Không đáng lo |
|---|---|
| khoá nghiệp vụ (`call_id`, `imsi`, `msisdn`) | cột kỹ thuật (`p_date`, cột phân vùng đã được pipeline sinh ra) |
| cột số đo dùng để tính KPI hạ nguồn | cột đã được kiểm ở checkpoint SỚM HƠN dưới tên khác |
| cột vừa qua một phép biến đổi có rủi ro | cột hằng số / cột mới thêm chưa có nghiệp vụ |

Một stage phủ 3/40 cột nhưng ba cột đó là khoá chính và hai số đo thì **không phải `FAIL`**.
Một stage phủ 30/40 cột mà bỏ trống đúng khoá grain thì **là `FAIL`**.

Đếm cột là việc của máy. Đánh giá cột nào quan trọng là việc của bạn.

## 4. Chấm phán quyết từng stage

| Verdict | Khi nào |
|---|---|
| `FAIL` | còn lỗi cứng chưa xử lý (`misplaced`, `ambiguous`, `unexecutable`, `duplicate`), HOẶC bỏ trống một cột trọng yếu, HOẶC stage có 0 rule |
| `WEAK` | chạy được nhưng phủ mỏng, hoặc có rule bạn nghi là chết mà chưa đủ bằng chứng |
| `PASS` | dùng được: không lỗi cứng, phủ được các cột trọng yếu của stage |

`overall` là phán quyết tổng, chỉ `PASS` hoặc `WEAK`. Quy ước: có bất kỳ stage nào `WEAK` thì
`overall` là `WEAK`. Có stage nào `FAIL` thì cân nhắc mục 5.

## 5. Có stage FAIL thì `submit_verdict` hay `reject_rules`?
Đây là chỗ dễ sai nhất, và hậu quả hai hướng không đối xứng.

- Nếu **hầu hết stage hỏng**, hoặc lỗi nằm ở khâu dựng rule chứ không ở vài rule lẻ →
  `reject_rules`. Gen Rules sinh lại toàn bộ.
- Nếu **chỉ một vài stage hỏng, phần còn lại dùng được** → vẫn `submit_verdict` với
  `per_stage` ghi rõ stage nào `FAIL`. Orchestrator đọc danh sách đó và tự quyết: còn lượt
  regen thì nó sẽ yêu cầu Gen Rules sửa đúng các checkpoint đó; hết lượt thì nó ghi kèm dấu
  `degraded_stages` để Java biết các checkpoint ấy không đáng tin.

Vì sao không từ chối vô điều kiện: **số lượt regen là hữu hạn.** Hết lượt mà chưa ghi được gì
thì job chạy với 0 rule — tệ hơn hẳn chạy với một bộ rule biết là yếu ở vài chặng. Từ chối là
công cụ mạnh; dùng khi sinh lại thật sự có cơ hội ra kết quả tốt hơn.

## 6. Viết `must_fix` sao cho sửa được
`must_fix` đi thẳng vào prompt của Gen Rules ở lượt sinh lại. Mỗi mục phải nêu đủ ba thứ:
**stage nào, rule_id nào, sửa thành gì.**

- Vô dụng: `"rule chất lượng kém"`, `"cần phủ thêm cột"`, `"xem lại các rule NULL"`
- Dùng được: `"tf_final / END_PROVINCE_NOT_NULL: cột đã qua coalesce(...,'unknown') nên rule
  IS NULL không bao giờ bắn. Đổi sang kiểm end_province = 'unknown', hoặc chuyển rule về
  checkpoint jn_infra trước phép coalesce."`

Một `must_fix` mơ hồ tiêu một lượt regen mà không đổi được gì.
