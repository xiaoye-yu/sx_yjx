from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TruckStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_truck_stats(df: DataFrame):
    """写入目标表ads_truck_stats，增加类型校验确保结构一致"""
    try:
        # 1. 读取目标表结构进行校验和对齐
        target_table = "tms.ads_truck_stats"
        target_df = get_spark_session().table(target_table)
        # 按目标表的列顺序和数据类型强制转换
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


def execute_truck_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行卡车统计ETL，目标日期：{target_date}")

    # 1. 仅处理目标日期的新数据（不合并历史数据）
    part2 = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("truck_model_type", "truck_model_type_name", "recent_days") \
        .agg(
            coalesce(sum("trans_finish_count"), lit(0)).alias("trans_finish_count"),
            coalesce(sum("trans_finish_distance"), lit(0)).alias("trans_finish_distance"),
            coalesce(sum("trans_finish_dur_sec"), lit(0)).alias("trans_finish_dur_sec"),
            # 平均值计算（保留2位小数，避免除零错误）
            round(
                coalesce(sum("trans_finish_distance") / sum("trans_finish_count"), lit(0)),
                2
            ).alias("avg_trans_finish_distance"),
            round(
                coalesce(sum("trans_finish_dur_sec") / sum("trans_finish_count"), lit(0)),
                2
            ).alias("avg_trans_finish_dur_sec")
        ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", col("recent_days").cast(IntegerType())) \
        .withColumn("truck_model_type", coalesce(col("truck_model_type"), lit(""))) \
        .withColumn("truck_model_type_name", coalesce(col("truck_model_type_name"), lit(""))) \
        .select(
            "dt", "recent_days", "truck_model_type", "truck_model_type_name",
            "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
            "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
        )

    # 2. 直接使用新数据作为最终数据（无需合并历史数据）
    final_df = part2

    # 3. 写入前校验数据样例
    print("[INFO] 待写入数据类型校验：")
    final_df.show(5, truncate=False)

    # 4. 写入目标表（通过write函数自动对齐类型）
    write_to_ads_truck_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'  # 可通过调度工具动态传入
    execute_truck_stats_etl(target_date)