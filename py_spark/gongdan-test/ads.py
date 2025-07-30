from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, lit
from pyspark.sql import DataFrame
from datetime import datetime, timedelta


def get_spark_session():
    """初始化SparkSession，使用test数据库"""
    spark = SparkSession.builder \
        .appName("AdsPageAnalysisReportETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE test")
    return spark


def write_to_ads_table(df: DataFrame, target_table: str, pt: str):
    """写入ADS层表通用方法，保持表结构一致性"""
    try:
        spark = get_spark_session()
        target_df = spark.table(target_table)
        # 按目标表结构调整字段类型和顺序
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


def process_ads_page_overview_report(pt: str):
    """处理页面概览指标报表"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理 ads_page_overview_report，分区日期：{pt}")

    # 读取DWS层数据并转换
    dws_df = spark.table("test.dws_page_stats").filter(col("ds") == pt)

    result_df = dws_df.select(
        col("page_type"),
        col("stat_date"),
        col("click_count"),
        col("click_user_count"),
        col("visitor_count"),
        lit(pt).alias("ds")
    )

    # 写入目标表
    write_to_ads_table(result_df, "test.ads_page_overview_report", pt)
    print(f"[INFO] ads_page_overview_report 处理完成")


def process_ads_block_click_report(pt: str):
    """处理点击分布指标报表"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理 ads_block_click_report，分区日期：{pt}")

    # 读取DWS层数据并转换
    dws_df = spark.table("test.dws_block_stats").filter(col("ds") == pt)

    result_df = dws_df.select(
        col("page_type"),
        col("block_id"),
        col("block_name"),
        col("block_click_count"),
        col("block_click_user_count"),
        col("guide_pay_amount").alias("block_guide_pay_amount"),
        lit(pt).alias("ds")
    )

    # 写入目标表
    write_to_ads_table(result_df, "test.ads_block_click_report", pt)
    print(f"[INFO] ads_block_click_report 处理完成")


def process_ads_page_trend_report(pt: str):
    """处理数据趋势指标报表（近30天数据）"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理 ads_page_trend_report，分区日期：{pt}")

    # 计算30天前的日期（yyyyMMdd格式）
    pt_date = datetime.strptime(pt, "%Y%m%d")
    start_date = (pt_date - timedelta(days=29)).strftime("%Y%m%d")

    # 读取DWS层近30天数据
    dws_df = spark.table("test.dws_page_stats") \
        .filter(col("ds") >= start_date) \
        .filter(col("ds") <= pt)

    result_df = dws_df.select(
        col("page_type"),
        col("stat_date"),
        col("visitor_count").alias("daily_visitor_count"),
        col("click_user_count").alias("daily_click_user_count"),
        lit(pt).alias("ds")
    )

    # 写入目标表
    write_to_ads_table(result_df, "test.ads_page_trend_report", pt)
    print(f"[INFO] ads_page_trend_report 处理完成")


def process_ads_guide_detail_report(pt: str):
    """处理引导详情报表（商品相关板块）"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理 ads_guide_detail_report，分区日期：{pt}")

    # 读取DWS层数据并筛选商品相关板块
    dws_df = spark.table("test.dws_block_stats") \
        .filter(col("ds") == pt) \
        .filter(col("block_name").like("%商品%"))

    result_df = dws_df.select(
        col("page_type"),
        col("block_id").alias("guide_product_id"),
        col("block_click_count").alias("guide_click_count"),
        col("block_click_user_count").alias("guide_visitor_count"),
        lit(pt).alias("ds")
    )

    # 写入目标表
    write_to_ads_table(result_df, "test.ads_guide_detail_report", pt)
    print(f"[INFO] ads_guide_detail_report 处理完成")


def process_ads_module_detail_report(pt: str):
    """处理模块详情报表（含点击占比计算）"""
    spark = get_spark_session()
    print(f"[INFO] 开始处理 ads_module_detail_report，分区日期：{pt}")

    # 读取DWS层数据并计算占比
    dws_df = spark.table("test.dws_block_stats").filter(col("ds") == pt)

    result_df = dws_df.select(
        col("page_type"),
        col("block_id").alias("module_id"),
        col("block_name").alias("module_name"),
        col("block_click_count").alias("module_click_count"),
        round((col("block_click_count") / col("page_total_click")) * 100, 2)
        .alias("module_click_ratio_percent"),
        lit(pt).alias("ds")
    )

    # 写入目标表
    write_to_ads_table(result_df, "test.ads_module_detail_report", pt)
    print(f"[INFO] ads_module_detail_report 处理完成")


def main(pt: str):
    """主函数：依次处理所有ADS报表数据导入"""
    # 按顺序处理各报表数据导入
    process_ads_page_overview_report(pt)
    process_ads_block_click_report(pt)
    process_ads_page_trend_report(pt)
    process_ads_guide_detail_report(pt)
    process_ads_module_detail_report(pt)

    print(f"[INFO] 所有ADS层报表数据导入完成，分区日期：{pt}")


if __name__ == "__main__":
    pt = '20250730'  # 可通过调度工具动态传入
    main(pt)