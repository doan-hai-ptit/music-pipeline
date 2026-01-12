import glob

def merge_parquet(spark, input_path: str, output_path: str):
    """
    Gộp tất cả file parquet trong input_path → parquet output (1 file).
    """
    parquet_files = glob.glob(f"{input_path}/**/*.parquet", recursive=True)

    if not parquet_files:
        print(f"[WARN] Không tìm thấy file parquet trong {input_path}")
        return

    print(f"[INFO] Đang gộp {len(parquet_files)} file Parquet...")

    df = spark.read.parquet(*parquet_files)

    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    print(f"[SUCCESS] Gộp Parquet hoàn tất → {output_path}")
