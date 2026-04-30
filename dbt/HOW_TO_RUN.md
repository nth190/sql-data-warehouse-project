# Hướng dẫn chạy dbt từ A đến Z

## ⚠️ Yêu cầu trước khi bắt đầu

- MySQL đang chạy với 3 schemas: `bronze_dw`, `silver_dw`, `gold_dw`
- `bronze_dw` đã có dữ liệu raw (6 bảng từ CRM và ERP)
- Python virtual environment đã được tạo sẵn trong `.venv/`

---

## PHẦN 1 — Cấu hình (chỉ làm 1 lần duy nhất)

### Bước 1 — Điền thông tin kết nối MySQL vào `profiles.yml`

Mở file `profiles.yml`, sửa các dòng sau:

```yaml
user: your_mysql_user       # ← đổi thành username MySQL của bạn (VD: root)
password: your_mysql_password   # ← đổi thành password MySQL của bạn
database: your_database_name    # ← đổi thành tên database (VD: bronze_dw)
```

### Bước 2 — Cài packages dbt

```bash
source .venv/bin/activate
dbt deps --profiles-dir .
```

> Lệnh này tải `dbt_utils` và `dbt_expectations` vào folder `dbt_packages/`.
> Chỉ cần chạy **1 lần duy nhất**, hoặc khi có thay đổi trong `packages.yml`.

---

## PHẦN 2 — Kiểm tra trước khi chạy (không cần DB)

### Bước 3 — Kích hoạt môi trường

```bash
source .venv/bin/activate
```

> ⚠️ Phải chạy lệnh này **mỗi khi mở terminal mới** trước khi dùng bất kỳ lệnh dbt nào.

### Bước 4 — Kiểm tra syntax toàn bộ project

```bash
dbt parse --profiles-dir .
```

- Kiểm tra tất cả file `.sql` và `.yml` có đúng cú pháp không
- Kiểm tra các `ref()` và `source()` có trỏ đúng model không
- **Không kết nối database**
- Nếu pass → sẵn sàng chạy thật

---

## PHẦN 3 — Chạy thật (cần MySQL đang chạy)

### Bước 5 — Chạy lần đầu (Full Refresh)

```bash
dbt run --full-refresh --profiles-dir .
```

- Xóa toàn bộ views/tables trong `silver_dw` và `gold_dw` rồi tạo lại từ đầu
- Dùng cho lần chạy đầu tiên hoặc khi muốn reset toàn bộ data
- Thứ tự chạy tự động theo dependency:
  ```
  int_crm_cust_info
  int_crm_prd_info
  int_erp_cust_az12
  int_erp_loc_a101
  int_erp_px_cat_g1v2
  int_sales
      ↓
  int_customers_enriched
  int_products_enriched
      ↓
  dim_customers
  dim_products
      ↓
  fact_sales
  ```

### Bước 6 — Chạy incremental (hàng ngày)

```bash
dbt run --profiles-dir .
```

- Chỉ load data mới (dựa trên watermark của từng model)
- Không xóa data cũ
- Dùng cho các lần chạy sau lần đầu

---

## PHẦN 4 — Kiểm tra chất lượng data

### Bước 7 — Chạy tất cả tests

```bash
dbt test --profiles-dir .
```

- Kiểm tra `unique`, `not_null`, referential integrity
- Chạy custom tests trong folder `tests/`
- Kết quả hiện ra PASS / FAIL cho từng test

---

## PHẦN 5 — Xem documentation và lineage

### Bước 8 — Generate docs

```bash
dbt docs generate --no-compile --profiles-dir .
```

- Tạo file `target/catalog.json` chứa toàn bộ metadata
- `--no-compile` = không cần kết nối database
- Phải chạy lại mỗi khi có thay đổi trong models

### Bước 9 — Mở web docs

```bash
dbt docs serve --port 8080 --profiles-dir .
```

- Mở web server tại `http://localhost:8080`
- Xem lineage graph: click vào model bất kỳ → scroll xuống → **"Expand"**
- Tắt server: nhấn `Ctrl + C` trong terminal

---

## Tóm tắt thứ tự từ A đến Z

```
── LẦN ĐẦU TIÊN ──────────────────────────────────────────
[1] Sửa profiles.yml           → điền user/password MySQL
[2] dbt deps --profiles-dir .  → cài packages

── MỖI LẦN CHẠY ──────────────────────────────────────────
[3] source .venv/bin/activate  → kích hoạt môi trường

[4] dbt parse --profiles-dir . → kiểm tra syntax (không cần DB)

[5] dbt run --full-refresh --profiles-dir .   → lần đầu
    HOẶC
    dbt run --profiles-dir .                  → lần sau (incremental)

[6] dbt test --profiles-dir .  → kiểm tra data quality

── XEM DOCS ───────────────────────────────────────────────
[7] dbt docs generate --no-compile --profiles-dir .
[8] dbt docs serve --port 8080 --profiles-dir .
    → Mở trình duyệt: http://localhost:8080
```

---

## Các lệnh hữu ích khác

### Chạy 1 model cụ thể
```bash
dbt run --select int_crm_cust_info --profiles-dir .
```

### Chạy cả 1 layer
```bash
# Chỉ chạy Silver layer
dbt run --select intermediate --profiles-dir .

# Chỉ chạy Gold layer
dbt run --select gold --profiles-dir .
```

### Chạy 1 model và tất cả models phụ thuộc vào nó
```bash
# Dấu + phía sau = chạy dim_customers và tất cả downstream
dbt run --select dim_customers+ --profiles-dir .

# Dấu + phía trước = chạy fact_sales và tất cả upstream
dbt run --select +fact_sales --profiles-dir .
```

### Test 1 model cụ thể
```bash
dbt test --select int_crm_cust_info --profiles-dir .
```

### Xem danh sách tất cả models
```bash
dbt ls --profiles-dir .
```
