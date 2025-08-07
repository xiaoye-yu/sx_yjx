from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, rank, concat, date_sub, when, lit, round
from pyspark.sql.window import Window


def get_spark_session():
    """创建并返回启用Hive支持的SparkSession，配置动态分区参数"""
    spark = SparkSession.builder \
        .appName("DwsTradeAllETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE work_ord")  # 使用目标数据库
    return spark


# ------------------------------
# 一、销售分析DWS表（dws_sales_analysis）
# ------------------------------
def create_dws_sales_analysis(spark):
    """创建销售分析DWS表"""
    spark.sql("DROP TABLE IF EXISTS dws_sales_analysis")

    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dws_sales_analysis (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        item_name string comment '商品名称',
        time_type string comment '时间粒度（1d/7d/30d）',
        time_value string comment '时间值（如2025-01-01/2025-01-01至2025-01-07/2025-01）',
        total_sales decimal(12,2) comment '总销售额',
        total_quantity int comment '总销量',
        item_sales_rank int comment '品类内商品销售排名（按销售额降序）',
        monthly_pay_progress decimal(5,2) comment '月支付金额进度（仅30d维度有值）',
        monthly_pay_contribution decimal(5,2) comment '月支付金额贡献（仅30d维度有值）',
        last_month_rank int comment '上月类目在本店同级类目的销售排名'
    )
    PARTITIONED BY (dt string comment '分区日期（与time_value的结束日期一致）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/dws/dws_sales_analysis'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'comment' = '销售分析DWS表，聚合品类及商品的销售核心指标'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 销售分析DWS表创建完成")


def load_dws_sales_analysis(spark, target_dt):
    """加载销售分析DWS表数据（1d/7d/30d维度聚合）"""
    # 读取源表
    dwd_df = spark.table("dwd_trade_sales_detail") \
        .filter(col("dt") <= target_dt)

    # 1. 1d维度聚合
    df_1d = dwd_df.groupBy(
        col("category_id"),
        col("category_name"),
        col("item_name"),
        col("create_dt"),
        col("last_month_rank")
    ).agg(
        sum("pay_amount").alias("total_sales"),
        sum("pay_quantity").alias("total_quantity")
    ).withColumn("time_type", lit("1d")) \
        .withColumn("time_value", col("create_dt")) \
        .withColumn("monthly_pay_progress", lit(None).cast("decimal(5,2)")) \
        .withColumn("monthly_pay_contribution", lit(None).cast("decimal(5,2)")) \
        .withColumn("dt", col("create_dt"))

    # 添加排名并严格指定列顺序
    window_1d = Window.partitionBy("category_id", "create_dt").orderBy(col("total_sales").desc())
    df_1d = df_1d.withColumn("item_sales_rank", rank().over(window_1d)) \
        .select(
        "category_id", "category_name", "item_name",
        "time_type", "time_value",
        "total_sales", "total_quantity",
        "item_sales_rank",
        "monthly_pay_progress", "monthly_pay_contribution",
        "last_month_rank", "dt"
    )

    # 2. 7d维度聚合
    df_7d = dwd_df.groupBy(
        col("category_id"),
        col("category_name"),
        col("item_name"),
        col("create_dt"),
        col("last_month_rank")
    ).agg(
        sum("pay_amount").alias("total_sales"),
        sum("pay_quantity").alias("total_quantity")
    ).withColumn("time_type", lit("7d")) \
        .withColumn("time_value", concat(date_sub(col("create_dt"), 6), lit("至"), col("create_dt"))) \
        .withColumn("monthly_pay_progress", lit(None).cast("decimal(5,2)")) \
        .withColumn("monthly_pay_contribution", lit(None).cast("decimal(5,2)")) \
        .withColumn("dt", col("create_dt"))

    # 添加排名并严格指定列顺序
    window_7d = Window.partitionBy("category_id", "create_dt").orderBy(col("total_sales").desc())
    df_7d = df_7d.withColumn("item_sales_rank", rank().over(window_7d)) \
        .select(
        "category_id", "category_name", "item_name",
        "time_type", "time_value",
        "total_sales", "total_quantity",
        "item_sales_rank",
        "monthly_pay_progress", "monthly_pay_contribution",
        "last_month_rank", "dt"
    )

    # 3. 30d维度聚合
    total_30d_sales = dwd_df.groupBy("create_dt") \
        .agg(sum("pay_amount").alias("total_30d_sales")) \
        .withColumnRenamed("create_dt", "end_dt")

    df_30d = dwd_df.alias("t") \
        .join(total_30d_sales.alias("d"), col("t.create_dt") == col("d.end_dt"), "inner") \
        .groupBy(
        col("t.category_id"),
        col("t.category_name"),
        col("t.item_name"),
        col("t.create_dt"),
        col("t.target_gmv"),
        col("t.last_month_rank"),
        col("d.total_30d_sales")
    ).agg(
        sum("t.pay_amount").alias("total_sales"),
        sum("t.pay_quantity").alias("total_quantity")
    ).withColumn("time_type", lit("30d")) \
        .withColumn("time_value", concat(date_sub(col("t.create_dt"), 29), lit("至"), col("t.create_dt"))) \
        .withColumn("monthly_pay_progress",
                    round((col("total_sales") / col("t.target_gmv")) * 100, 2).cast("decimal(5,2)")) \
        .withColumn("monthly_pay_contribution",
                    round((col("total_sales") / col("d.total_30d_sales")) * 100, 2).cast("decimal(5,2)")) \
        .withColumn("dt", col("t.create_dt"))

    # 添加排名并严格指定列顺序
    window_30d = Window.partitionBy("t.category_id", "t.create_dt").orderBy(col("total_sales").desc())
    df_30d = df_30d.withColumn("item_sales_rank", rank().over(window_30d)) \
        .select(
        "t.category_id", "t.category_name", "t.item_name",
        "time_type", "time_value",
        "total_sales", "total_quantity",
        "item_sales_rank",
        "monthly_pay_progress", "monthly_pay_contribution",
        "t.last_month_rank", "dt"
    )

    # 合并数据并写入
    union_df = df_1d.unionByName(df_7d).unionByName(df_30d)
    union_df.write \
        .mode("overwrite") \
        .insertInto("dws_sales_analysis")

    print(f"[INFO] 销售分析DWS表数据写入完成，目标分区日期：{target_dt}")


# ------------------------------
# 二、属性分析DWS表（dws_attribute_analysis）
# ------------------------------
def create_dws_attribute_analysis(spark):
    """创建属性分析DWS表"""
    spark.sql("DROP TABLE IF EXISTS dws_attribute_analysis")

    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dws_attribute_analysis (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        attr_id string comment '属性ID',
        attr_value string comment '属性值（如红色、XL）',
        time_type string comment '时间粒度（1d/7d/30d）',
        time_value string comment '时间值（如2025-01-01/2025-01-01至2025-01-30）',
        attr_visitor_count int comment '属性流量（独立访客数）',
        attr_pay_conversion decimal(5,2) comment '属性支付转化率（%）',
        active_item_count int comment '动销商品数'
    )
    PARTITIONED BY (dt string comment '分区日期（与time_value的结束日期一致）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/dws/dws_attribute_analysis'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'comment' = '属性分析DWS表，聚合品类属性的流量及转化指标'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 属性分析DWS表创建完成")


def load_dws_attribute_analysis(spark, target_dt):
    """加载属性分析DWS表数据（1d/7d/30d维度聚合）"""
    # 读取源表
    dwd_df = spark.table("dwd_item_attribute_detail") \
        .filter(col("dt") <= target_dt)

    # 1. 1d维度聚合（严格对齐表结构列顺序）
    df_1d = dwd_df.groupBy(
        col("category_id"),
        col("category_name"),
        col("attr_id"),
        col("attr_value"),
        col("visit_dt")
    ).agg(
        countDistinct("visitor_id").alias("attr_visitor_count"),
        countDistinct(when(col("is_pay") == 1, col("visitor_id"))).alias("pay_visitors"),
        countDistinct("item_id").alias("active_item_count")
    ).withColumn("time_type", lit("1d")) \
        .withColumn("time_value", col("visit_dt")) \
        .withColumn("attr_pay_conversion",
                    round((col("pay_visitors") / col("attr_visitor_count")) * 100, 2).cast("decimal(5,2)")) \
        .withColumn("dt", col("visit_dt")) \
        .drop("pay_visitors") \
        .select(
        "category_id", "category_name", "attr_id", "attr_value",
        "time_type", "time_value",
        "attr_visitor_count", "attr_pay_conversion", "active_item_count",
        "dt"
    )

    # 2. 7d维度聚合（严格对齐表结构列顺序）
    df_7d = dwd_df.groupBy(
        col("category_id"),
        col("category_name"),
        col("attr_id"),
        col("attr_value"),
        col("visit_dt")
    ).agg(
        countDistinct("visitor_id").alias("attr_visitor_count"),
        countDistinct(when(col("is_pay") == 1, col("visitor_id"))).alias("pay_visitors"),
        countDistinct("item_id").alias("active_item_count")
    ).withColumn("time_type", lit("7d")) \
        .withColumn("time_value", concat(date_sub(col("visit_dt"), 6), lit("至"), col("visit_dt"))) \
        .withColumn("attr_pay_conversion",
                    round((col("pay_visitors") / col("attr_visitor_count")) * 100, 2).cast("decimal(5,2)")) \
        .withColumn("dt", col("visit_dt")) \
        .drop("pay_visitors") \
        .select(
        "category_id", "category_name", "attr_id", "attr_value",
        "time_type", "time_value",
        "attr_visitor_count", "attr_pay_conversion", "active_item_count",
        "dt"
    )

    # 3. 30d维度聚合（严格对齐表结构列顺序）
    df_30d = dwd_df.groupBy(
        col("category_id"),
        col("category_name"),
        col("attr_id"),
        col("attr_value"),
        col("visit_dt")
    ).agg(
        countDistinct("visitor_id").alias("attr_visitor_count"),
        countDistinct(when(col("is_pay") == 1, col("visitor_id"))).alias("pay_visitors"),
        countDistinct("item_id").alias("active_item_count")
    ).withColumn("time_type", lit("30d")) \
        .withColumn("time_value", concat(date_sub(col("visit_dt"), 29), lit("至"), col("visit_dt"))) \
        .withColumn("attr_pay_conversion",
                    round((col("pay_visitors") / col("attr_visitor_count")) * 100, 2).cast("decimal(5,2)")) \
        .withColumn("dt", col("visit_dt")) \
        .drop("pay_visitors") \
        .select(
        "category_id", "category_name", "attr_id", "attr_value",
        "time_type", "time_value",
        "attr_visitor_count", "attr_pay_conversion", "active_item_count",
        "dt"
    )

    # 合并数据并写入
    union_df = df_1d.unionByName(df_7d).unionByName(df_30d)
    union_df.write \
        .mode("overwrite") \
        .insertInto("dws_attribute_analysis")

    print(f"[INFO] 属性分析DWS表数据写入完成，目标分区日期：{target_dt}")


# ------------------------------
# 三、流量分析DWS表（dws_traffic_analysis）
# ------------------------------
def create_dws_traffic_analysis(spark):
    """创建流量分析DWS表"""
    spark.sql("DROP TABLE IF EXISTS dws_traffic_analysis")

    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dws_traffic_analysis (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        channel string comment '流量渠道（如搜索、直通车、推荐）',
        search_word string comment '热搜词（非搜索渠道为null）',
        time_value string comment '时间值（如2025-01-01至2025-01-30）',
        channel_visitor_count int comment '渠道访客数',
        channel_pay_conversion decimal(5,2) comment '渠道支付转化率（%）',
        hot_word_visitor_count int comment '热搜词访客量',
        hot_word_rank int comment '热搜词排名'
    )
    PARTITIONED BY (dt string comment '分区日期（与time_value的结束日期一致）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/dws/dws_traffic_analysis'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'comment' = '流量分析DWS表，聚合品类流量渠道及热搜词指标'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 流量分析DWS表创建完成")


def load_dws_traffic_analysis(spark, target_dt):
    """加载流量分析DWS表数据（月维度聚合）"""
    # 读取源表并聚合
    agg_df = spark.table("dwd_traffic_source_detail") \
        .filter(col("dt") <= target_dt) \
        .groupBy(
        col("category_id"),
        col("category_name"),
        col("channel"),
        col("search_word"),
        col("visit_dt")
    ).agg(
        countDistinct("visitor_id").alias("channel_visitor_count"),
        countDistinct(when(col("is_pay") == 1, col("visitor_id"))).alias("pay_visitors"),
        countDistinct(when(col("search_word").isNotNull(), col("visitor_id"))).alias("hot_word_visitor_count")
    ).withColumn("time_value", concat(date_sub(col("visit_dt"), 29), lit("至"), col("visit_dt"))) \
        .withColumn("channel_pay_conversion",
                    round((col("pay_visitors") / col("channel_visitor_count")) * 100, 2).cast("decimal(5,2)")) \
        .withColumn("dt", col("visit_dt")) \
        .drop("pay_visitors")

    # 添加热搜词排名并对齐列顺序
    window = Window.partitionBy("category_id", "visit_dt").orderBy(col("hot_word_visitor_count").desc())
    final_df = agg_df.withColumn("hot_word_rank", rank().over(window)) \
        .select(
        "category_id", "category_name", "channel", "search_word",
        "time_value",
        "channel_visitor_count", "channel_pay_conversion",
        "hot_word_visitor_count", "hot_word_rank",
        "dt"
    )

    # 写入数据
    final_df.write \
        .mode("overwrite") \
        .insertInto("dws_traffic_analysis")

    print(f"[INFO] 流量分析DWS表数据写入完成，目标分区日期：{target_dt}")


# ------------------------------
# 四、客群洞察DWS表（dws_customer_insight）
# ------------------------------
def create_dws_customer_insight(spark):
    """创建客群洞察DWS表"""
    spark.sql("DROP TABLE IF EXISTS dws_customer_insight")

    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dws_customer_insight (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        behavior_type string comment '用户行为类型（search/visit/pay）',
        age int comment '用户年龄',
        gender string comment '用户性别',
        region string comment '用户地域',
        consumption_level string comment '用户消费层级',
        time_value string comment '时间值（如2025-01-01至2025-01-30）',
        crowd_count int comment '人群数量',
        crowd_ratio decimal(5,2) comment '人群占比（%）'
    )
    PARTITIONED BY (dt string comment '分区日期（与time_value的结束日期一致）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/dws/dws_customer_insight'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'comment' = '客群洞察DWS表，聚合品类用户行为及人群属性指标'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 客群洞察DWS表创建完成")


def load_dws_customer_insight(spark, target_dt):
    """加载客群洞察DWS表数据（月维度聚合）"""
    # 读取源表
    dwd_df = spark.table("dwd_user_behavior_detail") \
        .filter(col("dt") <= target_dt)

    # 计算总用户数子查询
    total_users = dwd_df.groupBy(
        col("category_id"),
        col("behavior_type"),
        col("behavior_dt")
    ).agg(
        countDistinct("user_id").alias("total_users")
    )

    # 主聚合（关联总用户数）并对齐列顺序
    final_df = dwd_df.alias("t") \
        .join(
        total_users.alias("total"),
        [
            col("t.category_id") == col("total.category_id"),
            col("t.behavior_type") == col("total.behavior_type"),
            col("t.behavior_dt") == col("total.behavior_dt")
        ],
        "inner"
    ) \
        .groupBy(
        col("t.category_id"),
        col("t.category_name"),
        col("t.behavior_type"),
        col("t.age"),
        col("t.gender"),
        col("t.region"),
        col("t.consumption_level"),
        col("t.behavior_dt"),
        col("total.total_users")
    ).agg(
        countDistinct("t.user_id").alias("crowd_count")
    ).withColumn("time_value", concat(date_sub(col("t.behavior_dt"), 29), lit("至"), col("t.behavior_dt"))) \
        .withColumn("crowd_ratio", round((col("crowd_count") / col("total.total_users")) * 100, 2).cast("decimal(5,2)")) \
        .withColumn("dt", col("t.behavior_dt")) \
        .select(
        "t.category_id", "t.category_name", "t.behavior_type",
        "t.age", "t.gender", "t.region", "t.consumption_level",
        "time_value",
        "crowd_count", "crowd_ratio",
        "dt"
    )

    # 写入数据
    final_df.write \
        .mode("overwrite") \
        .insertInto("dws_customer_insight")

    print(f"[INFO] 客群洞察DWS表数据写入完成，目标分区日期：{target_dt}")


def process_all_dws_tables(spark, target_dt):
    """处理所有DWS表的创建和数据加载"""
    # 1. 销售分析DWS表
    create_dws_sales_analysis(spark)
    load_dws_sales_analysis(spark, target_dt)

    # 2. 属性分析DWS表
    create_dws_attribute_analysis(spark)
    load_dws_attribute_analysis(spark, target_dt)

    # 3. 流量分析DWS表
    create_dws_traffic_analysis(spark)
    load_dws_traffic_analysis(spark, target_dt)

    # 4. 客群洞察DWS表
    create_dws_customer_insight(spark)
    load_dws_customer_insight(spark, target_dt)


if __name__ == "__main__":
    # 初始化Spark会话
    spark = get_spark_session()

    # 目标日期（分区日期，格式：yyyyMMdd）
    target_date = "20250806"

    # 执行所有DWS表的处理
    process_all_dws_tables(spark, target_date)

    # 停止Spark会话
    spark.stop()
    print("[INFO] 所有DWS表处理完成")