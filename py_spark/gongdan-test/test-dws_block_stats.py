from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, when, lit
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用test数据库"""
    spark = SparkSession.builder \
        .appName("DwsBlockStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE test")
    return spark


def write_to_dws_block_stats(df: DataFrame, pt: str):
    """写入目标表test.dws_block_stats，增加表结构校验"""
    try:
        target_table = "test.dws_block_stats"
        target_df = get_spark_session().table(target_table)
        # 按目标表列顺序和类型调整DataFrame
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        df.write \
            .mode("overwrite") \
            .partitionBy("ds") \
            .saveAsTable(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条数据到 {target_table} 分区 ds={pt}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e


def execute_dws_block_stats_etl(pt: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行dws_block_stats ETL，分区日期：{pt}")

    # 读取DWD层数据
    dwd_df = spark.table("test.dwd_user_page_behavior_detail") \
        .filter(col("ds") == pt) \
        .alias("d")

    # 计算页面总点击量子查询（对应SQL中的t表）
    page_total_click = dwd_df.groupBy(col("page_type")) \
        .agg(count(when(col("is_click") == 1, 1)).alias("total_click")) \
        .alias("t")

    # 关联计算板块统计指标
    result_df = dwd_df.join(
        page_total_click,
        col("d.page_type") == col("t.page_type"),
        "inner"
    ).groupBy(
        col("d.page_type"),
        col("d.block_id"),
        col("d.block_name"),
        col("t.total_click")
    ).agg(
        count(when(col("d.is_click") == 1, 1)).alias("block_click_count"),
        countDistinct(when(col("d.is_click") == 1, col("d.user_id"))).alias("block_click_user_count"),
        sum(col("d.pay_amount")).alias("guide_pay_amount")
    ).select(
        col("d.page_type"),
        col("d.block_id"),
        col("d.block_name"),
        col("block_click_count"),
        col("block_click_user_count"),
        col("guide_pay_amount"),
        col("t.total_click").alias("page_total_click"),
        lit(pt).alias("ds")  # 添加分区字段
    )

    # 数据预览
    print("[INFO] 待写入数据样例：")
    result_df.show(5, truncate=False)

    # 写入目标表
    write_to_dws_block_stats(result_df, pt)
    print(f"[INFO] dws_block_stats ETL执行完成，分区日期：{pt}")


if __name__ == "__main__":
    pt = '20250730'  # 可通过调度工具动态传入
    execute_dws_block_stats_etl(pt)