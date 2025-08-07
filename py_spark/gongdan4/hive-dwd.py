from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format


def get_spark_session():
    """创建并返回启用Hive支持的SparkSession，配置动态分区参数"""
    spark = SparkSession.builder \
        .appName("DwdTradeAllETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE work_ord")  # 使用目标数据库
    return spark


def create_dwd_trade_sales_detail(spark):
    """创建销售明细DWD表并加载数据"""
    # 删除已存在的表
    spark.sql("DROP TABLE IF EXISTS dwd_trade_sales_detail")

    # 创建表
    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dwd_trade_sales_detail (
        item_id string comment '商品ID',
        item_name string comment '商品名称',
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        pay_amount decimal(10,2) comment '单条记录支付金额',
        pay_quantity int comment '单条记录支付数量',
        target_gmv decimal(12,2) comment '类目目标GMV',
        last_month_rank int comment '上月该类目在本店同级类目的销售排名',
        create_dt string comment '销售日期（yyyy-MM-dd）'
    )
    PARTITIONED BY (dt string comment '分区日期（yyyy-MM-dd）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/dwd/dwd_trade_sales_detail'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'external.table.purge' = 'true',
        'comment' = '销售明细DWD表，用于支撑销售分析的基础明细数据'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 销售明细DWD表创建完成")


def load_dwd_trade_sales_detail(spark, target_dt):
    """加载销售明细数据"""
    source_df = spark.table("ods_sales_full") \
        .select(
        col("item_id"),
        col("item_name"),
        col("category_id"),
        col("category_name"),
        col("pay_amount"),
        col("pay_quantity"),
        col("target_gmv"),
        col("last_month_rank"),
        date_format(col("create_time"), "yyyy-MM-dd").alias("create_dt"),
        col("ds").alias("dt")
    )

    # 写入数据
    source_df.write \
        .mode("overwrite") \
        .insertInto("dwd_trade_sales_detail")
    print(f"[INFO] 销售明细数据写入完成，分区：{target_dt}")


def create_dwd_item_attribute_detail(spark):
    """创建商品属性明细DWD表并加载数据"""
    # 删除已存在的表
    spark.sql("DROP TABLE IF EXISTS dwd_item_attribute_detail")

    # 创建表
    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dwd_item_attribute_detail (
        item_id string comment '商品ID',
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        attr_id string comment '属性ID（如颜色、尺寸对应的ID）',
        attr_value string comment '属性值（如红色、XL）',
        visitor_id string comment '访客ID',
        is_pay tinyint comment '是否支付（1=支付，0=未支付）',
        visit_dt string comment '访问日期（yyyy-MM-dd）'
    )
    PARTITIONED BY (dt string comment '分区日期（yyyy-MM-dd）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/dwd/dwd_item_attribute_detail'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'external.table.purge' = 'true',
        'comment' = '商品属性明细DWD表，用于支撑属性分析的基础明细数据'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 商品属性明细DWD表创建完成")


def load_dwd_item_attribute_detail(spark, target_dt):
    """加载商品属性明细数据"""
    source_df = spark.table("ods_attribute_info") \
        .filter(col("ds") == target_dt) \
        .select(
        col("item_id"),
        col("category_id"),
        col("category_name"),
        col("attr_id"),
        col("attr_value"),
        col("visitor_id"),
        col("is_pay"),
        date_format(col("visit_time"), "yyyy-MM-dd").alias("visit_dt"),
        col("ds").alias("dt")
    )

    # 写入数据
    source_df.write \
        .mode("overwrite") \
        .insertInto("dwd_item_attribute_detail")
    print(f"[INFO] 商品属性明细数据写入完成，分区：{target_dt}")


def create_dwd_traffic_source_detail(spark):
    """创建流量来源明细DWD表并加载数据"""
    # 删除已存在的表
    spark.sql("DROP TABLE IF EXISTS dwd_traffic_source_detail")

    # 创建表
    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dwd_traffic_source_detail (
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        channel string comment '流量渠道（如搜索、直通车、推荐）',
        search_word string comment '热搜词（非搜索渠道为null）',
        visitor_id string comment '访客ID',
        is_pay tinyint comment '是否支付（1=支付，0=未支付）',
        visit_dt string comment '访问日期（yyyy-MM-dd）'
    )
    PARTITIONED BY (dt string comment '分区日期（yyyy-MM-dd）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/dwd/dwd_traffic_source_detail'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'external.table.purge' = 'true',
        'comment' = '流量来源明细DWD表，用于支撑流量分析的基础明细数据'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 流量来源明细DWD表创建完成")


def load_dwd_traffic_source_detail(spark, target_dt):
    """加载流量来源明细数据"""
    source_df = spark.table("ods_traffic_detail") \
        .filter(col("ds") == target_dt) \
        .select(
        col("category_id"),
        col("category_name"),
        col("channel"),
        col("search_word"),
        col("visitor_id"),
        col("is_pay"),
        date_format(col("visit_time"), "yyyy-MM-dd").alias("visit_dt"),
        col("ds").alias("dt")
    )

    # 写入数据
    source_df.write \
        .mode("overwrite") \
        .insertInto("dwd_traffic_source_detail")
    print(f"[INFO] 流量来源明细数据写入完成，分区：{target_dt}")


def create_dwd_user_behavior_detail(spark):
    """创建用户行为明细DWD表并加载数据"""
    # 删除已存在的表
    spark.sql("DROP TABLE IF EXISTS dwd_user_behavior_detail")

    # 创建表
    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dwd_user_behavior_detail (
        user_id string comment '用户ID',
        category_id string comment '品类ID',
        category_name string comment '品类名称',
        behavior_type string comment '用户行为类型（search/visit/pay）',
        behavior_dt string comment '行为日期（yyyy-MM-dd）',
        age int comment '用户年龄',
        gender string comment '用户性别（男/女）',
        region string comment '用户地域（如省/市）',
        consumption_level string comment '用户消费层级（如高/中/低）'
    )
    PARTITIONED BY (dt string comment '分区日期（yyyy-MM-dd）')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/dwd/dwd_user_behavior_detail'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
        'external.table.purge' = 'true',
        'comment' = '用户行为明细DWD表，用于支撑客群洞察的基础明细数据'
    )
    """
    spark.sql(create_sql)
    print("[INFO] 用户行为明细DWD表创建完成")


def load_dwd_user_behavior_detail(spark, target_dt):
    """加载用户行为明细数据（关联用户行为表与用户信息表）"""
    # 读取用户行为表
    behavior_df = spark.table("ods_user_behavior_full") \
        .filter(col("ds") == target_dt)

    # 读取用户信息表
    user_df = spark.table("ods_user_info") \
        .filter(col("ds") == target_dt)

    # 关联两表
    source_df = behavior_df.join(
        user_df,
        on="user_id",
        how="left"
    ).select(
        col("user_id"),
        col("category_id"),
        col("category_name"),
        col("behavior_type"),
        date_format(col("behavior_time"), "yyyy-MM-dd").alias("behavior_dt"),
        col("age"),
        col("gender"),
        col("region"),
        col("consumption_level"),
        behavior_df["ds"].alias("dt")  # 使用行为表的分区日期
    )

    # 写入数据
    source_df.write \
        .mode("overwrite") \
        .insertInto("dwd_user_behavior_detail")
    print(f"[INFO] 用户行为明细数据写入完成，分区：{target_dt}")


def process_all_dwd_tables(spark, target_dt):
    """处理所有DWD表的创建和数据加载"""
    # 1. 销售明细DWD表
    create_dwd_trade_sales_detail(spark)
    load_dwd_trade_sales_detail(spark, target_dt)

    # 2. 商品属性明细DWD表
    create_dwd_item_attribute_detail(spark)
    load_dwd_item_attribute_detail(spark, target_dt)

    # 3. 流量来源明细DWD表
    create_dwd_traffic_source_detail(spark)
    load_dwd_traffic_source_detail(spark, target_dt)

    # 4. 用户行为明细DWD表
    create_dwd_user_behavior_detail(spark)
    load_dwd_user_behavior_detail(spark, target_dt)


if __name__ == "__main__":
    # 初始化Spark会话
    spark = get_spark_session()

    # 目标日期（分区日期）
    target_date = "20250806"

    # 执行所有DWD表的处理
    process_all_dwd_tables(spark, target_date)

    # 停止Spark会话
    spark.stop()
    print("[INFO] 所有DWD表处理完成")