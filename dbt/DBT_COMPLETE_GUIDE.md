# Hướng dẫn dbt hoàn chỉnh từ A đến Z
### Dành cho người mới bắt đầu — có thể dùng làm template cho nhiều project

---

## 🧠 dbt là gì? Tại sao dùng nó?

**dbt (data build tool)** là công cụ giúp bạn viết SQL có tổ chức hơn, giống như viết code thay vì viết SQL rời rạc.

| Không có dbt | Có dbt |
|---|---|
| Viết SQL thủ công, chạy theo thứ tự tự quản lý | dbt tự tính thứ tự chạy |
| Không biết bảng nào phụ thuộc bảng nào | Lineage graph hiển thị trực quan |
| Không có test tự động | Tests chạy tự động sau mỗi lần build |
| Không có documentation | Docs tự generate từ code |
| Incremental load phải tự viết watermark | dbt có `is_incremental()` macro |

**Trong project này:** dbt đọc data từ `bronze_dw` (MySQL) → transform → tạo ra `silver_dw` và `gold_dw`.

---

## 📁 Hiểu cấu trúc folder trước khi bắt đầu

```
dw_dbt/
│
├── dbt_project.yml      ← "Hồ sơ" của project: tên, profile dùng, config
├── packages.yml         ← Danh sách thư viện bên ngoài cần cài
├── profiles.yml         ← Thông tin kết nối database (user, password, host)
│
├── models/              ← Chứa tất cả các file SQL (.sql) → đây là nơi chính
│   ├── sources/         ← Khai báo bảng raw đã có sẵn trong DB (bronze)
│   ├── intermediate/    ← Silver layer: clean + enrich data
│   └── gold/            ← Gold layer: dimension và fact tables
│
├── tests/               ← Custom SQL tests kiểm tra data quality
├── macros/              ← Các hàm SQL tái sử dụng
└── target/              ← Folder dbt tự tạo ra khi chạy (không chỉnh sửa)
```

> 💡 **Quy tắc:** Bạn chỉ chỉnh sửa các file trong `models/`, `tests/`, `macros/`,
> `dbt_project.yml`, `packages.yml`, và `profiles.yml`. Các folder khác dbt tự quản lý.

---

## ═══════════════════════════════════════════
## PHẦN A — CÀI ĐẶT MỘT LẦN DUY NHẤT
## ═══════════════════════════════════════════

> Phần này chỉ làm **1 lần** khi bắt đầu project mới.

---

### A1 — Tạo Python virtual environment

**Chạy ở đâu:** Terminal, trong folder `dw_dbt/`

```bash
python3 -m venv .venv
```

**Tại sao:**
- Virtual environment là "hộp cát" riêng biệt chứa Python và các thư viện
- Tránh xung đột với các project Python khác trên máy
- `.venv` là tên folder — có thể đặt tên khác nhưng `.venv` là convention phổ biến

---

### A2 — Kích hoạt virtual environment

**Chạy ở đâu:** Terminal

```bash
# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

**Tại sao:**
- Sau khi kích hoạt, terminal sẽ dùng Python và pip trong `.venv/` thay vì global
- Bạn biết đã kích hoạt khi thấy `(.venv)` xuất hiện ở đầu dòng terminal
- ⚠️ Phải chạy lại lệnh này **mỗi khi mở terminal mới**

---

### A3 — Cài dbt và adapter cho database

**Chạy ở đâu:** Terminal (sau khi đã activate .venv)

```bash
# Nếu dùng MySQL
pip install dbt-mysql

# Nếu dùng PostgreSQL (project khác)
pip install dbt-postgres

# Nếu dùng Snowflake (project khác)
pip install dbt-snowflake

# Nếu dùng BigQuery (project khác)
pip install dbt-bigquery
```

**Tại sao:**
- dbt core không biết cách nói chuyện với từng database — cần "adapter" riêng
- Adapter là "phiên dịch viên" giữa dbt và database cụ thể của bạn
- Trong project này dùng MySQL → cài `dbt-mysql`

**Kiểm tra cài thành công:**
```bash
dbt --version
```
Kết quả mong đợi:
```
Core: 1.7.x
Plugins: mysql: 1.7.x
```

---

### A4 — Tạo file `packages.yml`

**Tạo file:** `packages.yml` trong folder `dw_dbt/`

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

**Tại sao:**
- `packages.yml` khai báo các thư viện bên ngoài dbt cần cài
- `dbt_utils` chứa các hàm tiện ích như `generate_surrogate_key()`
- File này giống `requirements.txt` của Python

---

### A5 — Cài packages dbt

**Chạy ở đâu:** Terminal, trong folder `dw_dbt/`

```bash
dbt deps --profiles-dir .
```

**Tại sao:**
- `dbt deps` tải packages được khai báo trong `packages.yml` về folder `dbt_packages/`
- Chạy lại khi có thay đổi trong `packages.yml`

**Giải thích `--profiles-dir .`:**
- Mặc định dbt tìm `profiles.yml` ở `~/.dbt/`
- Flag này bảo dbt tìm `profiles.yml` ngay trong folder hiện tại (`.`)
- **Luôn thêm flag này** vào mọi lệnh dbt nếu `profiles.yml` để trong project folder

---

### A6 — Điền thông tin kết nối vào `profiles.yml`

**Chỉnh sửa file:** `profiles.yml` trong folder `dw_dbt/`

```yaml
dw_mysql:           # ← Tên profile — phải khớp với "profile:" trong dbt_project.yml
  target: dev
  outputs:
    dev:
      type: mysql
      host: localhost         # ← địa chỉ MySQL server (localhost nếu chạy trên máy)
      port: 3306              # ← port MySQL (mặc định 3306)
      user: root              # ← username MySQL của bạn
      password: your_pass     # ← password MySQL của bạn
      database: bronze_dw     # ← tên database chính
      schema: silver_dw       # ← schema dbt dùng làm mặc định khi output
      threads: 4              # ← số models chạy song song
```

**Tại sao:**
- `profiles.yml` là nơi duy nhất chứa thông tin nhạy cảm (password)
- Nên thêm `profiles.yml` vào `.gitignore` để không push lên GitHub
- `profile:` trong `dbt_project.yml` phải **khớp tên** với tên trong `profiles.yml`

> ⚠️ **Lỗi thường gặp:** `profile: 'dw_dbt'` trong `dbt_project.yml` nhưng
> `profiles.yml` đặt tên là `dw_mysql` → dbt báo lỗi "profile not found"

---

### A7 — Kiểm tra kết nối database

**Chạy ở đâu:** Terminal

```bash
dbt debug --profiles-dir .
```

**Tại sao:**
- Kiểm tra tất cả cấu hình trước khi làm bất cứ thứ gì
- Kết quả hiển thị từng check: profiles, project, database connection
- Phải thấy `All checks passed` mới tiếp tục

**Kết quả mong đợi:**
```
All checks passed!
```

**Nếu lỗi:**
- `Connection refused` → MySQL chưa chạy, khởi động MySQL trước
- `Access denied` → sai username/password trong `profiles.yml`
- `Profile not found` → tên profile trong `dbt_project.yml` không khớp `profiles.yml`

---

## ═══════════════════════════════════════════
## PHẦN B — KIỂM TRA TRƯỚC KHI CHẠY
## ═══════════════════════════════════════════

> Phần này làm **mỗi khi bạn chỉnh sửa code** để kiểm tra không có lỗi syntax.
> **Không cần database đang chạy.**

---

### B1 — Kích hoạt môi trường (nhắc lại)

**Chạy ở đâu:** Terminal

```bash
source .venv/bin/activate
```

> ⚠️ Luôn làm bước này đầu tiên khi mở terminal mới. Nếu quên,
> terminal sẽ không tìm thấy lệnh `dbt`.

---

### B2 — Kiểm tra syntax toàn bộ project

**Chạy ở đâu:** Terminal, trong folder `dw_dbt/`

```bash
dbt parse --profiles-dir .
```

**Tại sao:**
- dbt đọc tất cả file `.sql` và `.yml`, kiểm tra:
  - Cú pháp Jinja (`{{ ref() }}`, `{{ source() }}`, `{{ config() }}`) có đúng không
  - Các `ref('model_name')` có trỏ đến model thực sự tồn tại không
  - Các `source('source_name', 'table_name')` có được khai báo trong `_sources.yml` không
- **Không kết nối database**, chạy rất nhanh
- Kết quả tạo ra `target/manifest.json` — file này chứa toàn bộ metadata của project

**Kết quả mong đợi:** Không có dòng `Error` nào.

**Lỗi thường gặp:**
```
Compilation Error: Model 'int_customers_enriched' depends on 'stg_crm_cust_info'
which was not found
```
→ Có `ref('stg_crm_cust_info')` nhưng file `stg_crm_cust_info.sql` không tồn tại

---

### B3 — Xem danh sách tất cả models

**Chạy ở đâu:** Terminal

```bash
dbt ls --profiles-dir .
```

**Tại sao:**
- Xác nhận dbt nhận ra đúng tất cả models, sources, tests
- Hữu ích để kiểm tra sau khi thêm/xóa file

**Kết quả mong đợi trong project này:**
```
dw_dbt.gold.dim_customers
dw_dbt.gold.dim_products
dw_dbt.gold.fact_sales
dw_dbt.intermediate.int_crm_cust_info
dw_dbt.intermediate.int_crm_prd_info
dw_dbt.intermediate.int_customers_enriched
dw_dbt.intermediate.int_erp_cust_az12
dw_dbt.intermediate.int_erp_loc_a101
dw_dbt.intermediate.int_erp_px_cat_g1v2
dw_dbt.intermediate.int_products_enriched
dw_dbt.intermediate.int_sales
```

---

## ═══════════════════════════════════════════
## PHẦN C — CHẠY LẦN ĐẦU (FULL REFRESH)
## ═══════════════════════════════════════════

> Phần này làm **1 lần đầu tiên** hoặc khi muốn **reset toàn bộ data**.
> **Cần MySQL đang chạy và có data trong `bronze_dw`.**

---

### C1 — Chuẩn bị database trong MySQL

**Chạy ở đâu:** MySQL Workbench hoặc terminal MySQL

```sql
-- Tạo schema silver và gold nếu chưa có
CREATE SCHEMA IF NOT EXISTS silver_dw;
CREATE SCHEMA IF NOT EXISTS gold_dw;

-- Kiểm tra bronze_dw có data chưa
SELECT COUNT(*) FROM bronze_dw.crm_cust_info;
SELECT COUNT(*) FROM bronze_dw.crm_prd_info;
SELECT COUNT(*) FROM bronze_dw.crm_sales_details;
SELECT COUNT(*) FROM bronze_dw.erp_cust_az12;
SELECT COUNT(*) FROM bronze_dw.erp_loc_a101;
SELECT COUNT(*) FROM bronze_dw.erp_px_cat_g1v2;
```

**Tại sao:**
- dbt không tạo bronze — bronze đã được load sẵn bởi MySQL scripts của bạn
- dbt cần `silver_dw` và `gold_dw` schemas tồn tại để tạo tables/views
- Nếu bronze chưa có data → silver và gold sẽ rỗng

---

### C2 — Chạy full refresh toàn bộ

**Chạy ở đâu:** Terminal

```bash
dbt run --full-refresh --profiles-dir .
```

**Tại sao:**
- `--full-refresh` = xóa toàn bộ tables/views hiện có rồi tạo lại từ đầu
- Dùng cho lần chạy đầu tiên khi `silver_dw` và `gold_dw` còn trống
- dbt tự động tính thứ tự chạy dựa trên `ref()`:

```
Bước 1 — Clean models (đọc từ bronze, không phụ thuộc nhau → chạy song song):
  int_crm_cust_info
  int_crm_prd_info
  int_erp_cust_az12
  int_erp_loc_a101
  int_erp_px_cat_g1v2
  int_sales

Bước 2 — Enrich models (phụ thuộc clean models → chạy sau):
  int_customers_enriched   ← cần int_crm_cust_info, int_erp_cust_az12, int_erp_loc_a101
  int_products_enriched    ← cần int_crm_prd_info, int_erp_px_cat_g1v2

Bước 3 — Gold models (phụ thuộc enrich models → chạy cuối):
  dim_customers            ← cần int_customers_enriched
  dim_products             ← cần int_products_enriched
  fact_sales               ← cần int_sales, dim_customers, dim_products
```

**Kết quả mong đợi:**
```
Completed successfully
Done. PASS=11 WARN=0 ERROR=0 SKIP=0 TOTAL=11
```

---

### C3 — Kiểm tra kết quả trong MySQL

**Chạy ở đâu:** MySQL Workbench hoặc terminal MySQL

```sql
-- Kiểm tra silver layer đã có data
SELECT 'int_crm_cust_info'    as model, COUNT(*) as rows FROM silver_dw.int_crm_cust_info
UNION ALL
SELECT 'int_crm_prd_info',    COUNT(*) FROM silver_dw.int_crm_prd_info
UNION ALL
SELECT 'int_sales',           COUNT(*) FROM silver_dw.int_sales
UNION ALL
SELECT 'int_erp_cust_az12',   COUNT(*) FROM silver_dw.int_erp_cust_az12
UNION ALL
SELECT 'int_erp_loc_a101',    COUNT(*) FROM silver_dw.int_erp_loc_a101
UNION ALL
SELECT 'int_erp_px_cat_g1v2', COUNT(*) FROM silver_dw.int_erp_px_cat_g1v2
UNION ALL
SELECT 'int_customers_enriched', COUNT(*) FROM silver_dw.int_customers_enriched
UNION ALL
SELECT 'int_products_enriched',  COUNT(*) FROM silver_dw.int_products_enriched;

-- Kiểm tra gold layer đã có data
SELECT 'dim_customers', COUNT(*) FROM gold_dw.dim_customers
UNION ALL
SELECT 'dim_products',  COUNT(*) FROM gold_dw.dim_products
UNION ALL
SELECT 'fact_sales',    COUNT(*) FROM gold_dw.fact_sales;
```

**Tại sao:**
- Xác nhận data đã được load đúng vào đúng schema
- Row count phải > 0 ở tất cả các bảng

---

## ═══════════════════════════════════════════
## PHẦN D — KIỂM TRA DATA QUALITY (TESTS)
## ═══════════════════════════════════════════

---

### D1 — Chạy tất cả tests

**Chạy ở đâu:** Terminal

```bash
dbt test --profiles-dir .
```

**Tại sao:**
- dbt tự động chạy các tests được khai báo trong `_intermediate.yml` và `_gold.yml`
- Tests kiểm tra: `unique`, `not_null`, `relationships` (foreign key integrity)
- Ngoài ra chạy custom tests trong folder `tests/`

**Kết quả mong đợi:**
```
Done. PASS=49 WARN=0 ERROR=0 SKIP=0 TOTAL=49
```

**Nếu có test FAIL:**
```bash
# Xem chi tiết test nào fail
dbt test --profiles-dir . 2>&1 | grep -A5 "FAIL"
```

---

### D2 — Kiểm tra thủ công trong MySQL

**Chạy ở đâu:** MySQL Workbench

```sql
-- 1. Kiểm tra SCD2: lần đầu tất cả records phải is_current = true
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_count
FROM gold_dw.dim_customers;
-- Kết quả mong đợi: total = current_count (tất cả đều current)

-- 2. Kiểm tra không có orphaned records trong fact
SELECT COUNT(*) as orphaned_customers
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;
-- Kết quả mong đợi: 0

SELECT COUNT(*) as orphaned_products
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;
-- Kết quả mong đợi: 0

-- 3. Kiểm tra không có NULL ở surrogate keys
SELECT COUNT(*) as null_keys
FROM gold_dw.fact_sales
WHERE sales_key IS NULL
   OR customer_key IS NULL
   OR product_key IS NULL;
-- Kết quả mong đợi: 0
```

---

## ═══════════════════════════════════════════
## PHẦN E — CHẠY INCREMENTAL (HÀNG NGÀY)
## ═══════════════════════════════════════════

> Sau lần đầu full refresh, các lần chạy tiếp theo dùng incremental.
> Chỉ load data mới, không đụng vào data cũ.

---

### E1 — Chạy incremental load

**Chạy ở đâu:** Terminal

```bash
dbt run --profiles-dir .
```

**Tại sao:**
- Không có `--full-refresh` → dbt chạy ở chế độ incremental
- Mỗi model tự tính watermark: `SELECT MAX(watermark_column) FROM {{ this }}`
- Chỉ đọc data mới từ bronze (data sau watermark)
- Nhanh hơn nhiều so với full refresh vì không process lại toàn bộ data

**Watermark của từng model trong project này:**

| Model | Watermark | Loại |
|---|---|---|
| `int_crm_cust_info` | `cst_create_date` | Business date |
| `int_crm_prd_info` | `prd_start_dt` | Business date |
| `int_sales` | `sls_order_dt` | Business date |
| `int_erp_cust_az12` | `ingestion_ts` | Technical timestamp |
| `int_erp_loc_a101` | `ingestion_ts` | Technical timestamp |
| `int_erp_px_cat_g1v2` | `ingestion_ts` | Technical timestamp |

---

### E2 — Chạy tests sau incremental

**Chạy ở đâu:** Terminal

```bash
dbt test --profiles-dir .
```

**Tại sao:**
- Mỗi lần load data mới → chạy tests để đảm bảo data quality không bị ảnh hưởng
- Nếu test fail → có vấn đề với data mới vừa load

---

### E3 — Kiểm tra SCD2 sau incremental

**Chạy ở đâu:** MySQL Workbench

```sql
-- Kiểm tra có customer nào có nhiều hơn 1 record không
-- (dấu hiệu SCD2 đang hoạt động sau khi có thay đổi)
SELECT
    customer_id,
    COUNT(*) as version_count
FROM gold_dw.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
LIMIT 10;

-- Xem lịch sử thay đổi của 1 customer cụ thể
SELECT
    customer_id,
    first_name,
    last_name,
    valid_from,
    valid_to,
    is_current
FROM gold_dw.dim_customers
WHERE customer_id = 123   -- thay bằng customer_id thật
ORDER BY valid_from;
```

---

## ═══════════════════════════════════════════
## PHẦN F — XEM DOCUMENTATION VÀ LINEAGE
## ═══════════════════════════════════════════

---

### F1 — Generate docs

**Chạy ở đâu:** Terminal

```bash
dbt docs generate --no-compile --profiles-dir .
```

**Tại sao:**
- Tạo file `target/catalog.json` chứa metadata của toàn bộ project
- `--no-compile` = bỏ qua bước compile SQL → **không cần database đang chạy**
- Phải chạy lại mỗi khi bạn thay đổi models hoặc thêm descriptions trong YAML

---

### F2 — Mở web documentation

**Chạy ở đâu:** Terminal

```bash
dbt docs serve --port 8080 --profiles-dir .
```

**Tại sao:**
- Khởi động web server local tại `http://localhost:8080`
- Không cần internet, chạy hoàn toàn trên máy local
- Tắt server: nhấn `Ctrl + C`

**Sau đó mở trình duyệt và vào:** `http://localhost:8080`

**Cách xem lineage graph:**
1. Click vào tên model bất kỳ trong danh sách bên trái
2. Scroll xuống cuối trang
3. Click **"Expand"** trong phần Lineage
4. Sẽ thấy toàn bộ luồng: `bronze → silver → gold`

---

## ═══════════════════════════════════════════
## PHẦN G — CÁC LỆNH HỮU ÍCH KHÁC
## ═══════════════════════════════════════════

### Chạy 1 model cụ thể
```bash
# Chỉ chạy 1 model
dbt run --select int_crm_cust_info --profiles-dir .
```
**Dùng khi:** Debug 1 model cụ thể, không muốn chạy lại toàn bộ

---

### Chạy 1 model và tất cả models phụ thuộc vào nó (downstream)
```bash
# Dấu + phía SAU = chạy model đó + tất cả models downstream
dbt run --select int_customers_enriched+ --profiles-dir .
```
**Dùng khi:** Sửa `int_customers_enriched` → muốn chạy lại nó và `dim_customers` luôn

---

### Chạy 1 model và tất cả models nó phụ thuộc vào (upstream)
```bash
# Dấu + phía TRƯỚC = chạy model đó + tất cả models upstream
dbt run --select +fact_sales --profiles-dir .
```
**Dùng khi:** Muốn đảm bảo `fact_sales` chạy đúng từ đầu đến cuối

---

### Chạy cả 1 layer
```bash
# Chỉ Silver layer
dbt run --select intermediate --profiles-dir .

# Chỉ Gold layer
dbt run --select gold --profiles-dir .
```

---

### Test 1 model cụ thể
```bash
dbt test --select int_crm_cust_info --profiles-dir .
```

---

### Xóa toàn bộ files trong target/ (dọn dẹp)
```bash
dbt clean
```
**Dùng khi:** Gặp lỗi lạ, muốn reset cache của dbt

---

## ═══════════════════════════════════════════
## TỔNG KẾT — THỨ TỰ TỪ A ĐẾN Z
## ═══════════════════════════════════════════

```
╔══════════════════════════════════════════════════════════════╗
║           CHỈ LÀM 1 LẦN KHI BẮT ĐẦU PROJECT                ║
╚══════════════════════════════════════════════════════════════╝

[A1] python3 -m venv .venv
         └─ Tạo virtual environment

[A2] source .venv/bin/activate
         └─ Kích hoạt môi trường

[A3] pip install dbt-mysql
         └─ Cài dbt + adapter cho MySQL

[A4] dbt deps --profiles-dir .
         └─ Cài packages (dbt_utils, dbt_expectations)

[A5] Sửa profiles.yml
         └─ Điền user/password/host MySQL

[A6] dbt debug --profiles-dir .
         └─ Kiểm tra kết nối OK chưa


╔══════════════════════════════════════════════════════════════╗
║         LÀM MỖI LẦN CHẠY (MỞ TERMINAL MỚI)                 ║
╚══════════════════════════════════════════════════════════════╝

[B1] source .venv/bin/activate
         └─ Luôn làm đầu tiên

[B2] dbt parse --profiles-dir .
         └─ Kiểm tra syntax (không cần DB)


╔══════════════════════════════════════════════════════════════╗
║              LẦN ĐẦU TIÊN (FULL REFRESH)                    ║
╚══════════════════════════════════════════════════════════════╝

[C1] MySQL: tạo silver_dw, gold_dw schemas
         └─ Chạy trong MySQL Workbench

[C2] dbt run --full-refresh --profiles-dir .
         └─ Tạo toàn bộ tables từ đầu

[C3] MySQL: kiểm tra row counts
         └─ Xác nhận data đã load đúng

[D1] dbt test --profiles-dir .
         └─ Kiểm tra data quality


╔══════════════════════════════════════════════════════════════╗
║            CÁC LẦN SAU (INCREMENTAL - HÀNG NGÀY)           ║
╚══════════════════════════════════════════════════════════════╝

[E1] dbt run --profiles-dir .
         └─ Load data mới (watermark-based)

[E2] dbt test --profiles-dir .
         └─ Kiểm tra data quality sau load


╔══════════════════════════════════════════════════════════════╗
║                    XEM DOCUMENTATION                        ║
╚══════════════════════════════════════════════════════════════╝

[F1] dbt docs generate --no-compile --profiles-dir .
         └─ Tạo catalog.json

[F2] dbt docs serve --port 8080 --profiles-dir .
         └─ Mở http://localhost:8080
```

---

## 🔧 Khi gặp lỗi — Cách debug

| Lỗi | Nguyên nhân | Cách sửa |
|---|---|---|
| `command not found: dbt` | Chưa activate venv | Chạy `source .venv/bin/activate` |
| `profile not found` | Tên profile không khớp | Kiểm tra `profile:` trong `dbt_project.yml` = tên trong `profiles.yml` |
| `Connection refused` | MySQL chưa chạy | Khởi động MySQL |
| `Access denied` | Sai user/password | Sửa `profiles.yml` |
| `Source not found` | Bảng bronze chưa tồn tại | Chạy MySQL scripts để tạo bronze tables |
| `Model not found` | `ref('xyz')` nhưng file `xyz.sql` không có | Kiểm tra tên file và tên trong `ref()` |
| Packages error | Chưa chạy `dbt deps` | Chạy `dbt deps --profiles-dir .` |

---

## 📌 Cách dùng làm template cho project khác

Khi bắt đầu project dbt mới, chỉ cần thay đổi:

1. **`dbt_project.yml`** → đổi `name:` và `profile:`
2. **`profiles.yml`** → đổi thông tin kết nối database mới
3. **`models/sources/_sources.yml`** → khai báo tables raw của project mới
4. **`models/intermediate/`** → viết clean + enrich models cho data mới
5. **`models/gold/`** → viết dimension và fact tables mới
6. **`packages.yml`** → thêm/bớt packages nếu cần

Các lệnh dbt (`deps`, `parse`, `run`, `test`, `docs`) **dùng y hệt**, chỉ cần thêm `--profiles-dir .`

---

*Template này áp dụng được cho: MySQL, PostgreSQL, Snowflake, BigQuery, DuckDB.*
*Chỉ thay đổi adapter trong bước A3 và thông tin kết nối trong `profiles.yml`.*
