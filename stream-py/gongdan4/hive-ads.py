from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum  # 引入lit函数

def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("AdsTradeAllETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE work_ord")
    return spark


# ------------------------------
# 一、销售分析ADS表（ads_sales_analysis）
# ------------------------------
def create_ads_sales_analysis(spark):
    spark.sql("DROP TABLE IF EXISTS ads_sales_analysis")
    create_sql = """
    create external table if not exists ads_sales_analysis (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        item_name string comment '商品名称',
        time_type string comment '时间粒度（1d/7d/30d）',
        total_sales decimal(12,2) comment '总销售额',
        total_quantity int comment '总销量',
        item_sales_rank int comment '品类内商品销售排名',
        monthly_pay_progress decimal(5,2) comment '月支付金额进度（%）',
        monthly_pay_contribution decimal(5,2) comment '月支付金额贡献（%）',
        last_month_rank int comment '上月类目销售排名'
    )
    partitioned by (dt string comment '分区日期（与time_value结束日期一致）')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/ads/ads_sales_analysis'
    tblproperties (
        'orc.compress' = 'SNAPPY',
        'comment' = '销售分析ADS表，支撑销售核心概况报表'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 销售分析ADS表创建完成")


def load_ads_sales_analysis(spark, target_dt):
    dws_df = spark.table("dws_sales_analysis") \
        .filter(col("dt") == "2025-01-30")

    ads_df = dws_df.select(
        "category_id",
        "category_name",
        "item_name",
        "time_type",
        "total_sales",
        "total_quantity",
        "item_sales_rank",
        "monthly_pay_progress",
        "monthly_pay_contribution",
        "last_month_rank",
        lit(target_dt).alias("dt")  # 用lit将字符串转为常量值
    )

    ads_df.write \
        .mode("overwrite") \
        .insertInto("ads_sales_analysis")
    print(f"[INFO] 销售分析ADS表数据写入完成，分区：{target_dt}")


# ------------------------------
# 二、属性分析ADS表（ads_attribute_analysis）
# ------------------------------
def create_ads_attribute_analysis(spark):
    spark.sql("DROP TABLE IF EXISTS ads_attribute_analysis")
    create_sql = """
    create external table if not exists ads_attribute_analysis (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        attr_id string comment '属性ID',
        attr_value string comment '属性值（如红色、XL）',
        time_type string comment '时间粒度（1d/7d/30d）',
        attr_visitor_count int comment '属性流量（独立访客数）',
        attr_pay_conversion decimal(5,2) comment '属性支付转化率（%）',
        active_item_count int comment '动销商品数'
    )
    partitioned by (dt string comment '分区日期（与time_value结束日期一致）')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/ads/ads_attribute_analysis'
    tblproperties (
        'orc.compress' = 'SNAPPY',
        'comment' = '属性分析ADS表，支撑属性流量及转化报表'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 属性分析ADS表创建完成")


def load_ads_attribute_analysis(spark, target_dt):
    dws_df = spark.table("dws_attribute_analysis") \
        .filter(col("dt") == "2025-01-30")

    ads_df = dws_df.select(
        "category_id",
        "category_name",
        "attr_id",
        "attr_value",
        "time_type",
        "attr_visitor_count",
        "attr_pay_conversion",
        "active_item_count",
        lit(target_dt).alias("dt")  # 用lit将字符串转为常量值
    )

    ads_df.write \
        .mode("overwrite") \
        .insertInto("ads_attribute_analysis")
    print(f"[INFO] 属性分析ADS表数据写入完成，分区：{target_dt}")


# ------------------------------
# 三、流量分析ADS表（ads_traffic_analysis）
# ------------------------------
def create_ads_traffic_analysis(spark):
    spark.sql("DROP TABLE IF EXISTS ads_traffic_analysis")
    create_sql = """
    create external table if not exists ads_traffic_analysis (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        channel string comment '流量渠道（如搜索、直通车）',
        search_word string comment '热搜词（非搜索渠道为null）',
        channel_visitor_count int comment '渠道访客数',
        channel_pay_conversion decimal(5,2) comment '渠道支付转化率（%）',
        hot_word_visitor_count int comment '热搜词访客量',
        hot_word_rank int comment '热搜词排名'
    )
    partitioned by (dt string comment '分区日期（月份最后一天，如2025-01-31）')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/ads/ads_traffic_analysis'
    tblproperties (
        'orc.compress' = 'SNAPPY',
        'comment' = '流量分析ADS表，支撑渠道及热搜词报表'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 流量分析ADS表创建完成")


def load_ads_traffic_analysis(spark, target_dt):
    dws_df = spark.table("dws_traffic_analysis") \
        .filter(col("dt") == "2025-01-30")

    ads_df = dws_df.select(
        "category_id",
        "category_name",
        "channel",
        "search_word",
        "channel_visitor_count",
        "channel_pay_conversion",
        "hot_word_visitor_count",
        "hot_word_rank",
        lit(target_dt).alias("dt")  # 用lit将字符串转为常量值
    )

    ads_df.write \
        .mode("overwrite") \
        .insertInto("ads_traffic_analysis")
    print(f"[INFO] 流量分析ADS表数据写入完成，分区：{target_dt}")


# ------------------------------
# 四、客群洞察ADS表（ads_customer_insight）
# ------------------------------
def create_ads_customer_insight(spark):
    spark.sql("DROP TABLE IF EXISTS ads_customer_insight")
    create_sql = """
    create external table if not exists ads_customer_insight (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        behavior_type string comment '用户行为类型（search/visit/pay）',
        crowd_count int comment '对应行为人群的总数量'
    )
    partitioned by (dt string)
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/ads/ads_customer_insight'
    tblproperties (
        'orc.compress' = 'SNAPPY',
        'comment' = '客群洞察ADS表，聚焦搜索/访问/支付人群数量统计'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 客群洞察ADS表创建完成")


def load_ads_customer_insight(spark, target_dt):
    dws_df = spark.table("dws_customer_insight")

    ads_df = dws_df.groupBy(
        "category_id",
        "category_name",
        "behavior_type"
    ).agg(
        _sum("crowd_count").alias("crowd_count")
    ).select(
        "category_id",
        "category_name",
        "behavior_type",
        "crowd_count",
        lit(target_dt).alias("dt")  # 用lit将字符串转为常量值
    )

    ads_df.write \
        .mode("overwrite") \
        .insertInto("ads_customer_insight")
    print(f"[INFO] 客群洞察ADS表数据写入完成，分区：{target_dt}")


def process_all_ads_tables(spark, target_dt):
    create_ads_sales_analysis(spark)
    load_ads_sales_analysis(spark, target_dt)

    create_ads_attribute_analysis(spark)
    load_ads_attribute_analysis(spark, target_dt)

    create_ads_traffic_analysis(spark)
    load_ads_traffic_analysis(spark, target_dt)

    create_ads_customer_insight(spark)
    load_ads_customer_insight(spark, target_dt)


if __name__ == "__main__":
    spark = get_spark_session()
    target_date = "20250806"  # 目标分区日期
    process_all_ads_tables(spark, target_date)
    spark.stop()
    print("[INFO] 所有ADS表处理完成")