from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, round, cast
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

        # 按目标表字段类型强制转换（尤其是tinyint和decimal）
        df = df.select(
            col("dt").cast(target_df.schema["dt"].dataType),
            col("recent_days").cast("tinyint"),  # 匹配目标表的tinyint
            col("trans_finish_count").cast(target_df.schema["trans_finish_count"].dataType),
            col("trans_finish_distance").cast("decimal(16,2)"),  # 匹配decimal(16,2)
            col("trans_finish_dur_sec").cast(target_df.schema["trans_finish_dur_sec"].dataType)
        )

        # 使用overwrite模式，匹配SQL的insert overwrite逻辑
        df.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功覆盖写入 {df.count()} 条数据到 {target_table}")
    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e


def execute_trans_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行运输统计ETL，目标日期：{target_date}")

    # 1. 读取原表历史数据（SQL中的第一个union部分）
    current_data = spark.table("tms.ads_trans_stats") \
        .select(
        col("dt"),
        col("recent_days"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec")
    )
    print(f"[INFO] 原表历史数据条数：{current_data.count()}")

    # 2. 处理1天数据（recent_days=1）
    part1d = spark.table("tms.dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("dt") == target_date) \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        round(sum("trans_finish_distance"), 2).alias("trans_finish_distance"),  # 保留2位小数
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1).cast("tinyint"))  # 显式转换为tinyint
    print(f"[INFO] 1天数据条数：{part1d.count()}")
    part1d.show(2)  # 调试用，查看中间数据

    # 3. 处理多天数据（按recent_days分组）
    partnd = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        round(sum("trans_finish_distance"), 2).alias("trans_finish_distance"),  # 保留2位小数
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", col("recent_days").cast("tinyint"))  # 转换为tinyint
    print(f"[INFO] 多天数据条数：{partnd.count()}")
    partnd.show(2)  # 调试用，查看中间数据

    # 4. 合并所有数据（历史数据 + 新数据），匹配SQL的union逻辑
    final_df = current_data.unionByName(part1d).unionByName(partnd)

    # 5. 按目标表列顺序整理
    target_columns = [
        "dt", "recent_days",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec"
    ]
    final_df = final_df.select(target_columns)

    # 6. 写入目标表
    write_to_ads_trans_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'
    execute_trans_stats_etl(target_date)