# Music Data Pipeline

## Tổng quan

**Music Data Pipeline** là một project mô phỏng hệ thống xử lý dữ liệu âm nhạc theo thời gian (batch + near-real-time), được thiết kế theo hướng **Data Engineering**.

Pipeline này xử lý dữ liệu sự kiện nghe nhạc thông qua các bước:

**Kafka → Spark → Google Cloud Storage (GCS) → Airflow → dbt → BigQuery**

Mục tiêu chính của project:

* Thu thập dữ liệu sự kiện
* Xử lý và làm sạch dữ liệu
* Orchestrate pipeline bằng Airflow
* Xây dựng tầng phân tích bằng dbt
* Sẵn sàng cho phân tích và BI

---

## Kiến trúc hệ thống

```
Producer
   ↓
Kafka
   ↓
Spark Structured Streaming / Batch
   ↓
Google Cloud Storage
   ↓
Airflow
   ↓
dbt
   ↓
BigQuery
```

---

## Luồng hoạt động của Pipeline

### Ingest dữ liệu (Kafka)

* Producer gửi các sự kiện nghe nhạc (user, song, timestamp, action...)
* Kafka đóng vai trò message broker

### Xử lý dữ liệu bằng Spark

* Spark đọc dữ liệu từ Kafka
* Parse JSON, validate schema
* Ghi dữ liệu dạng **Parquet** lên GCS
* Phân vùng theo `event_date`

```
/data/status=raw/event_date=YYYY-MM-DD/*.parquet
```

### Gộp file bằng Dataproc Serverless

* Airflow kích hoạt job Spark Serverless
* Gộp nhiều file Parquet nhỏ thành một dataset

```
/data/status=merged/event_date=YYYY-MM-DD/
```

### Orchestrate bằng Airflow

* Airflow quản lý các job:

  1. Gộp các file Parquet theo ngày
  2. Xóa dữ liệu raw sau khi merge thành công
  3. Chạy dbt để transform dữ liệu

### Transform dữ liệu bằng dbt

* dbt đọc dữ liệu từ BigQuery (external hoặc staging tables)
* Xây dựng các bảng phân tích:

  * top_songs
  * top_artists
  * top_countries

---

## Cách chạy project

### Yêu cầu

* Docker và Docker Compose
* Python 3.9+ (cho local development)
* Tài khoản Google Cloud
* Service Account key (quyền BigQuery và GCS)
* Kafka

---

### Cấu hình môi trường

Tạo file `.env` tại thư mục root của project:

```env
GCS_BUCKET=your-gcs-bucket
GCP_KEY_PATH=/path/to/service_account.json
```

---
### Chạy Kafka
```bash
cd kafka
docker-compose up -d
```
---

### Chạy Airflow bằng Docker

```bash
cd airflow
docker-compose up -d
```
* Kafka: `localhost:9092`
* Airflow UI: [http://localhost:8080](http://localhost:8080)
* Thêm các biến bucket_name, project_id, region_name của bucket trong GCS trong Airflow UI
* Tạo foler scripts/ trên GCS và đưa file spark/batch-merge/dataproc_merge_file.py vào đó
---
### Chạy Producer
```bash
python -m producer.producer
```
---
### Chạy Spark_streaming

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 --jars "music-pipeline\spark\jars\gcs-connector-hadoop3-latest.jar" spark\streaming\spark_streaming.py
```

---

### Chạy dbt (nếu muốn chạy bằng tay)

```bash
cd dbt
dbt debug
dbt run
```