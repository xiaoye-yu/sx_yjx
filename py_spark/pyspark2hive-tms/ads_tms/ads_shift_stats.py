from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, coalesce
from pyspark.sql import DataFrame


def get_spark_session():
    spark = SparkSession.builder \
        .appName("TMSShiftStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_shift_stats(df: DataFrame):
    try:
        target_table = "tms.ads_shift_stats"
        spark = get_spark_session()
        target_df = spark.table(target_table)
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])
        df.write \
            .mode("append") \
            .insertInto(target_table)
        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}")
    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e


def execute_shift_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行班次统计ETL，目标日期：{target_date}")

    current_data = spark.table("tms.ads_shift_stats") \
        .select(
        col("dt"),
        col("recent_days"),
        col("shift_id"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("trans_finish_order_count")
    )

    new_data = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date) \
        .select(
        lit(target_date).alias("dt"),
        col("recent_days"),
        col("shift_id"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("trans_finish_order_count")
    )

    final_df = current_data.unionByName(new_data)

    target_columns = [
        "dt", "recent_days", "shift_id",
        "trans_finish_count", "trans_finish_distance",
        "trans_finish_dur_sec", "trans_finish_order_count"
    ]
    final_df = final_df.select(target_columns)

    final_df.show(5)
    write_to_ads_shift_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'
    execute_shift_stats_etl(target_date)