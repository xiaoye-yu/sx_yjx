from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when, date_format, lit
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用test数据库"""
    spark = SparkSession.builder \
        .appName("DwsPageStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE test")
    return spark


def write_to_dws_table(df: DataFrame, target_table: str, pt: str):
    """写入DWS层表通用方法，保持与现有代码风格一致"""
    try:
        # 校验并对齐目标表结构（参考PysparkToDwd.py的写法）
        spark = get_spark_session()
        target_df = spark.table(target_table)
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        df.write \
            .mode("overwrite") \
            .partitionBy("ds") \
            .saveAsTable(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条数据到 {target_table} 分区 ds={pt}")

    except Exception as e:
        print(f"[ERROR] 写入 {target_table} 失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e


def process_dws_page_stats(pt: str):
    """处理dws_page_stats表数据，对应SQL逻辑"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理dws_page_stats，分区日期：{pt}")

    # 读取DWD层数据
    dwd_df = spark.table("test.dwd_user_page_behavior_detail") \
        .filter(col("ds") == pt)

    # 按照SQL逻辑计算指标
    page_stats_df = dwd_df.groupBy(
        col("page_type"),
        date_format(col("click_time"), "yyyy-MM-dd").alias("stat_date")  # 对应DATE(click_time)
    ).agg(
        # 访客数（排除直播间等渠道）
        countDistinct(when(col("is_exclude_channel") == 0, col("user_id"))).alias("visitor_count"),
        # 总点击量
        count(when(col("is_click") == 1, 1)).alias("click_count"),
        # 点击人数
        countDistinct(when(col("is_click") == 1, col("user_id"))).alias("click_user_count")
    ).withColumn("ds", lit(pt))  # 添加分区字段

    # 数据预览
    print("[INFO] dws_page_stats 待写入数据样例：")
    page_stats_df.show(5, truncate=False)

    # 写入目标表
    write_to_dws_table(page_stats_df, "test.dws_page_stats", pt)
    print(f"[INFO] dws_page_stats 处理完成，分区日期：{pt}")


if __name__ == "__main__":
    pt = '20250730'  # 可通过调度工具动态传入
    process_dws_page_stats(pt)