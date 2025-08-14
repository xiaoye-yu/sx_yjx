from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col
from pyspark.sql import DataFrame


def get_spark_session():
    spark = SparkSession.builder \
        .appName("TMSTransStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_trans_stats(df: DataFrame):
    try:
        target_table = "tms.ads_trans_stats"
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


def execute_trans_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行运输统计ETL，目标日期：{target_date}")

    # 处理1天数据
    part1d = spark.table("tms.dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("dt") == target_date) \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 处理多天数据
    partnd = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date))

    # 获取当前表数据
    current_data = spark.table("tms.ads_trans_stats") \
        .select(
        col("dt"),
        col("recent_days"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec")
    )

    # 合并数据
    final_df = current_data.unionByName(part1d).unionByName(partnd)

    # 按目标表列顺序整理
    target_columns = [
        "dt", "recent_days",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec"
    ]
    final_df = final_df.select(target_columns)

    final_df.show(5)
    # 写入目标表
    write_to_ads_trans_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'
    execute_trans_stats_etl(target_date)