from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, when, round, coalesce
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSCityStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_city_stats(df: DataFrame):
    """写入目标表ads_city_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构并对齐字段类型
        target_table = "tms.ads_city_stats"
        target_df = get_spark_session().table(target_table)
        # 按目标表列顺序和类型调整DataFrame
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        df.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e  # 抛出异常，终止作业


def execute_city_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行城市统计ETL，目标日期：{target_date}")

    # 1. 处理1天数据（1d表）的full outer join
    # 1.1 生成city_order_1d
    city_order_1d = spark.table("tms.dws_trade_org_cargo_type_order_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("city_id", "city_name") \
        .agg(
            sum("order_count").alias("order_count"),
            sum("order_amount").alias("order_amount")
        ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.2 生成city_trans_1d
    # 1.2.1 内部子查询trans_1d
    trans_origin = spark.table("tms.dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("dt") == target_date) \
        .select("org_id", "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec")

    organ = spark.table("tms.dim_organ_full") \
        .filter(col("dt") == target_date) \
        .select("id", "org_level", "region_id")

    city_for_level1 = spark.table("tms.dim_region_full") \
        .filter(col("dt") == target_date) \
        .select("id", "name", "parent_id")

    city_for_level2 = spark.table("tms.dim_region_full") \
        .filter(col("dt") == target_date) \
        .select("id", "name")

    trans_1d = trans_origin \
        .join(organ, trans_origin["org_id"] == organ["id"], "left") \
        .join(city_for_level1, col("region_id") == city_for_level1["id"], "left") \
        .join(city_for_level2, city_for_level1["parent_id"] == city_for_level2["id"], "left") \
        .select(
            # 处理city_id：如果org_level=1用city_for_level1.id，否则用city_for_level2.id
            when(col("org_level") == 1, city_for_level1["id"]).otherwise(city_for_level2["id"]).alias("city_id"),
            # 处理city_name：如果org_level=1用city_for_level1.name，否则用city_for_level2.name
            when(col("org_level") == 1, city_for_level1["name"]).otherwise(city_for_level2["name"]).alias("city_name"),
            "trans_finish_count",
            "trans_finish_distance",
            "trans_finish_dur_sec"
        )

    # 1.2.2 聚合生成city_trans_1d
    city_trans_1d = trans_1d \
        .groupBy("city_id", "city_name") \
        .agg(
            sum("trans_finish_count").alias("trans_finish_count"),
            sum("trans_finish_distance").alias("trans_finish_distance"),
            sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
        ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1)) \
        .withColumn(
            "avg_trans_finish_distance",
            when(
                col("trans_finish_count") > 0,
                round(col("trans_finish_distance") / col("trans_finish_count"), 2)
            ).otherwise(lit(0))  # 处理除零情况
        ) \
        .withColumn(
            "avg_trans_finish_dur_sec",
            when(
                col("trans_finish_count") > 0,
                round(col("trans_finish_dur_sec") / col("trans_finish_count"), 2)
            ).otherwise(lit(0))  # 处理除零情况
        )

    # 1.3 1d部分的full outer join
    part2 = city_order_1d \
        .join(
            city_trans_1d,
            on=["dt", "recent_days", "city_id", "city_name"],
            how="full_outer"
        ) \
        .select(
            coalesce(city_order_1d["dt"], city_trans_1d["dt"]).alias("dt"),
            coalesce(city_order_1d["recent_days"], city_trans_1d["recent_days"]).alias("recent_days"),
            coalesce(city_order_1d["city_id"], city_trans_1d["city_id"]).alias("city_id"),
            coalesce(city_order_1d["city_name"], city_trans_1d["city_name"]).alias("city_name"),
            col("order_count"),
            col("order_amount"),
            col("trans_finish_count"),
            col("trans_finish_distance"),
            col("trans_finish_dur_sec"),
            col("avg_trans_finish_distance"),
            col("avg_trans_finish_dur_sec")
        )

    # 2. 处理多天数据（nd表）的full outer join
    # 2.1 生成city_order_nd
    city_order_nd = spark.table("tms.dws_trade_org_cargo_type_order_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("city_id", "city_name", "recent_days") \
        .agg(
            sum("order_count").alias("order_count"),
            sum("order_amount").alias("order_amount")
        ) \
        .withColumn("dt", lit(target_date))

    # 2.2 生成city_trans_nd
    city_trans_nd = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("city_id", "city_name", "recent_days") \
        .agg(
            sum("trans_finish_count").alias("trans_finish_count"),
            sum("trans_finish_distance").alias("trans_finish_distance"),
            sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
        ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn(
            "avg_trans_finish_distance",
            when(
                col("trans_finish_count") > 0,
                round(col("trans_finish_distance") / col("trans_finish_count"), 2)
            ).otherwise(lit(0))  # 处理除零情况
        ) \
        .withColumn(
            "avg_trans_finish_dur_sec",
            when(
                col("trans_finish_count") > 0,
                round(col("trans_finish_dur_sec") / col("trans_finish_count"), 2)
            ).otherwise(lit(0))  # 处理除零情况
        )

    # 2.3 nd部分的full outer join
    part3 = city_order_nd \
        .join(
            city_trans_nd,
            on=["dt", "recent_days", "city_id", "city_name"],
            how="full_outer"
        ) \
        .select(
            coalesce(city_order_nd["dt"], city_trans_nd["dt"]).alias("dt"),
            coalesce(city_order_nd["recent_days"], city_trans_nd["recent_days"]).alias("recent_days"),
            coalesce(city_order_nd["city_id"], city_trans_nd["city_id"]).alias("city_id"),
            coalesce(city_order_nd["city_name"], city_trans_nd["city_name"]).alias("city_name"),
            col("order_count"),
            col("order_amount"),
            col("trans_finish_count"),
            col("trans_finish_distance"),
            col("trans_finish_dur_sec"),
            col("avg_trans_finish_distance"),
            col("avg_trans_finish_dur_sec")
        )

    # 3. 合并当天处理的两部分数据（仅保留当前日期的新数据）
    final_df = part2.unionByName(part3)

    # 4. 处理空值（确保数值型字段不为null，与目标表类型兼容）
    final_df = final_df.fillna({
        "order_count": 0,
        "order_amount": 0,
        "trans_finish_count": 0,
        "trans_finish_distance": 0,
        "trans_finish_dur_sec": 0,
        "avg_trans_finish_distance": 0,
        "avg_trans_finish_dur_sec": 0
    }).fillna({
        "city_id": "",
        "city_name": ""
    })

    # 5. 调整列顺序与目标表一致
    target_columns = [
        "dt", "recent_days", "city_id", "city_name",
        "order_count", "order_amount", "trans_finish_count",
        "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    ]
    final_df = final_df.select(target_columns)

    # 6. 写入前校验
    print("[INFO] 待写入数据样例：")
    final_df.show(5, truncate=False)
    # 7. 写入目标表
    write_to_ads_city_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'  # 可通过调度工具动态传入
    execute_city_stats_etl(target_date)