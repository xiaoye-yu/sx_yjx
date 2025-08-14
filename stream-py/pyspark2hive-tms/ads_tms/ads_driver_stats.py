from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, array, explode, when, round
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSDriverStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_driver_stats(df: DataFrame):
    """写入目标表ads_driver_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构并对齐字段类型
        target_table = "tms.ads_driver_stats"
        target_df = get_spark_session().table(target_table)
        # 按目标表列顺序和类型调整DataFrame
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        df.write \
            .mode("overwrite") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e  # 抛出异常，终止作业


def execute_driver_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行司机统计ETL，目标日期：{target_date}")

    # 1. 读取源表数据（仅当前日期）
    dws_df = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date)

    # 2. 处理无副司机的情况
    part1 = dws_df.filter(col("driver2_emp_id").isNull()) \
        .select(
            col("recent_days"),
            col("driver1_emp_id").alias("driver_id"),
            col("driver1_name").alias("driver_name"),
            col("trans_finish_count"),
            col("trans_finish_distance"),
            col("trans_finish_dur_sec"),
            col("trans_finish_delay_count").alias("trans_finish_late_count")
        )

    # 3. 处理有副司机的情况（数据均分）
    part2 = dws_df.filter(col("driver2_emp_id").isNotNull()) \
        .select(
            col("recent_days"),
            array(
                array(col("driver1_emp_id"), col("driver1_name")),
                array(col("driver2_emp_id"), col("driver2_name"))
            ).alias("driver_arr"),
            col("trans_finish_count"),
            (col("trans_finish_distance") / 2).alias("trans_finish_distance"),
            (col("trans_finish_dur_sec") / 2).alias("trans_finish_dur_sec"),
            col("trans_finish_delay_count").alias("trans_finish_late_count")
        ) \
        .selectExpr(
            "recent_days",
            "explode(driver_arr) as driver_info",
            "trans_finish_count",
            "trans_finish_distance",
            "trans_finish_dur_sec",
            "trans_finish_late_count"
        ) \
        .select(
            col("recent_days"),
            col("driver_info")[0].cast("bigint").alias("driver_id"),
            col("driver_info")[1].alias("driver_name"),
            col("trans_finish_count"),
            col("trans_finish_distance"),
            col("trans_finish_dur_sec"),
            col("trans_finish_late_count")
        )

    # 4. 合并数据并聚合
    aggregated_df = part1.unionByName(part2) \
        .groupBy("driver_id", "driver_name", "recent_days") \
        .agg(
            sum("trans_finish_count").alias("trans_finish_count"),
            sum("trans_finish_distance").alias("trans_finish_distance"),
            sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
            sum("trans_finish_late_count").alias("trans_finish_late_count")
        ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn(
            "avg_trans_finish_distance",
            when(
                col("trans_finish_count") > 0,
                round(col("trans_finish_distance") / col("trans_finish_count"), 2)
            ).otherwise(lit(0))
        ) \
        .withColumn(
            "avg_trans_finish_dur_sec",
            when(
                col("trans_finish_count") > 0,
                round(col("trans_finish_dur_sec") / col("trans_finish_count"), 2)
            ).otherwise(lit(0))
        )

    # 5. 调整列顺序与目标表一致
    new_data_df = aggregated_df.select(
        "dt", "recent_days",
        col("driver_id").alias("driver_emp_id"),
        "driver_name", "trans_finish_count", "trans_finish_distance",
        "trans_finish_dur_sec", "avg_trans_finish_distance",
        "avg_trans_finish_dur_sec", "trans_finish_late_count"
    )

    # 6. 写入目标表（仅新增当前日期数据，与线路统计逻辑一致）
    new_data_df.show(5)
    write_to_ads_driver_stats(new_data_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'  # 可通过调度工具动态传入
    execute_driver_stats_etl(target_date)