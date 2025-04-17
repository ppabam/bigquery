from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name
import os, sys
from datetime import datetime

IS_REAL = True

def load_pageviews(spark, file_path):
    return spark.read.option("delimiter", " ").csv(file_path, inferSchema=True) \
        .toDF("domain", "title", "views", "size") \
        .withColumn("file_path", input_file_name()) \
        # .withColumn("date", lit(dt)) \

def cprint(msg: str, size=33):
    print(msg.center(size, "*"))

def run(data_path: str, dt: str, save_base: str):
    # SparkSession 생성 (마스터 URL 지정 X, spark-submit에서 설정됨)
    spark = SparkSession.builder.appName(f"wiki_load_{dt}").getOrCreate()

    cprint("LOAD START")
    cprint(data_path)
    raw = load_pageviews(spark, data_path, dt)
    raw.createOrReplaceTempView("temp_raw")
    cprint("LOAD END")

    # 시간 추출, 한국 도메인 필터링
    df = spark.sql("""
    SELECT
        domain, title, views, size,
        INT(SUBSTRING(ELEMENT_AT(SPLIT(file_path, '/'), -1), 20, 2)) AS hour
    FROM temp_raw
    WHERE domain = 'ko' OR domain LIKE 'ko.%'
    """)

    cprint("SAVE START")
    save_path = f'{save_base}/date={dt}/'
    df.write.mode("overwrite").option("compression", "gzip").parquet(save_path)
    assert_df = spark.read.parquet(save_path)
    assert_df.show()
    cprint(f"{save_path},cnt:{assert_df.count()}")
    cprint("SAVE END")

    # Spark 세션 종료
    spark.stop()

def extract_prefix_and_partition(date_str: str) -> tuple[str, str]:
    """
    "2024-01-01" → ("2024-01", "20240101")
    """
    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
    prefix = parsed_date.strftime("%Y-%m")
    partition = parsed_date.strftime("%Y%m%d")
    return prefix, partition

if __name__ == "__main__":
    DT = sys.argv[1]
    RAW_BASE = "gs://sunsin-bucket/wiki/"
    SAVE_BASE = "gs://mansour-bucket/wiki/parquet/ko"

    prefix, partition = extract_prefix_and_partition(DT)
    raw_path = f"{RAW_BASE}/{prefix}/dt={partition}"
    run(raw_path, DT, SAVE_BASE)
