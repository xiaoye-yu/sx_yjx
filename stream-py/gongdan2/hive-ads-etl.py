from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL_ADS_Multiple_Tables") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gmall_ins")
    return spark


def execute_hive_table_creation(spark, table_name, create_sql):
    """执行表创建语句"""
    print(f"[INFO] 开始创建表: {table_name}")
    for sql in create_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {table_name} 创建成功")


def execute_hive_insert(spark, table_name, insert_sql, pt_date):
    """执行数据插入操作，支持pt参数"""
    print(f"[INFO] 开始执行SQL插入 {table_name}，分区日期：{pt_date}")
    # 替换SQL中的分区日期参数
    insert_sql = insert_sql.format(pt=pt_date)
    df = spark.sql(insert_sql)

    # 添加分区字段
    df_with_partition = df.withColumn("ds", lit(pt_date))

    print(f"[INFO] 开始写入 {table_name} 分区{pt_date}")
    # 写入Hive
    df_with_partition.write \
        .mode('overwrite') \
        .insertInto(f"gmall_ins.{table_name}")

    print(f"[INFO] {table_name} 分区{pt_date}写入成功\n")


def main():
    # ====================== 手动设置区域 - 根据需要修改 ======================
    # 可根据需要注释/取消注释需要处理的表
    tables_to_process = [
        'ads_product_sales_ranking',
        'ads_traffic_source_top10',
        'ads_sku_sales_top5',
        'ads_search_word_top10',
        'ads_price_strength_overview',
        'ads_product_warning_list'
    ]
    # 设置分区日期(pt)，格式为yyyyMMdd
    pt_date = '20250806'  # 分区日期，对应SQL中的${pt}
    # ======================================================================

    # 初始化Spark会话
    spark = get_spark_session()

    # 定义各表的创建SQL和插入SQL
    table_sqls = {
        # 1. 全品类商品销售排行
        'ads_product_sales_ranking': {
            'create_sql': """
DROP TABLE IF EXISTS ads_product_sales_ranking;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_product_sales_ranking (
  product_name STRING COMMENT '商品名称',
  product_category STRING COMMENT '商品分类',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  sales_amount DECIMAL(12,2) COMMENT '销售额',
  sales_quantity INT COMMENT '销量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/ads/ads_product_sales_ranking';
            """,
            'insert_sql': """
SELECT
  product_name,
  product_category,
  time_type,
  sum(total_sales_amount) AS sales_amount,
  sum(total_sales_quantity) AS sales_quantity
FROM gmall_ins.dws_product_sales
WHERE ds = '{pt}'
group by product_name,product_category,time_type
            """
        },

        # 2. 流量来源 TOP10
        'ads_traffic_source_top10': {
            'create_sql': """
DROP TABLE IF EXISTS ads_traffic_source_top10;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_traffic_source_top10 (
  source_name STRING COMMENT '流量来源渠道',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  visitor_count INT COMMENT '访客数',
  pay_conversion_rate DECIMAL(6,4) COMMENT '支付转化率'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/ads/ads_traffic_source_top10';
            """,
            'insert_sql': """
SELECT
  source_name,
  time_type,
  visitor_count,
  pay_conversion_rate
FROM (
  SELECT
    source_name,
    time_type,
    total_visitor_count AS visitor_count,
    total_pay_conversion_rate AS pay_conversion_rate,
    ROW_NUMBER() OVER (PARTITION BY time_type ORDER BY total_visitor_count DESC) AS rn
  FROM gmall_ins.dws_traffic_source
  WHERE ds = '{pt}'
) t
WHERE rn <= 10
            """
        },

        # 3. 热销 SKU TOP5
        'ads_sku_sales_top5': {
            'create_sql': """
DROP TABLE IF EXISTS ads_sku_sales_top5;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_sku_sales_top5 (
  sku_info STRING COMMENT 'SKU信息',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  pay_quantity INT COMMENT '支付件数',
  pay_quantity_ratio DECIMAL(6,4) COMMENT '支付件数占比',
  current_stock INT COMMENT '当前库存',
  stock_available_days DECIMAL(6,2) COMMENT '库存可售天数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/ads/ads_sku_sales_top5';
            """,
            'insert_sql': """
SELECT
  sku_info,
  time_type,
  pay_quantity,
  pay_quantity_ratio,
  current_stock,
  stock_available_days
FROM (
  SELECT
    sku_info,
    time_type,
    total_pay_quantity AS pay_quantity,
    total_pay_quantity / SUM(total_pay_quantity) OVER (PARTITION BY time_type) AS pay_quantity_ratio,
    current_stock,
    stock_available_days,
    ROW_NUMBER() OVER (PARTITION BY time_type ORDER BY total_pay_quantity DESC) AS rn
  FROM gmall_ins.dws_sku_sales
  WHERE ds = '{pt}'
) t
WHERE rn <= 5
            """
        },

        # 4. 热门搜索词 TOP10 报表
        'ads_search_word_top10': {
            'create_sql': """
DROP TABLE IF EXISTS ads_search_word_top10;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_search_word_top10 (
  search_word STRING COMMENT '搜索词',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  visitor_count INT COMMENT '访客数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/ads/ads_search_word_top10';
            """,
            'insert_sql': """
SELECT
  search_word,
  time_type,
  visitor_count
FROM (
  SELECT
    search_word,
    time_type,
    total_visitor_count AS visitor_count,
    ROW_NUMBER() OVER (PARTITION BY time_type ORDER BY total_visitor_count DESC) AS rn
  FROM gmall_ins.dws_search_word
  WHERE ds = '{pt}'
) t
WHERE rn <= 10
            """
        },

        # 5. 价格力商品概览
        'ads_price_strength_overview': {
            'create_sql': """
DROP TABLE IF EXISTS ads_price_strength_overview;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_price_strength_overview (
  price_strength_level STRING COMMENT '价格力等级',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m）',
  product_count INT COMMENT '商品数量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/ads/ads_price_strength_overview';
            """,
            'insert_sql': """
SELECT
  price_strength_level,
  time_type,
  SUM(product_count) AS product_count
FROM gmall_ins.dws_price_strength
WHERE ds = '{pt}'
GROUP BY price_strength_level, time_type
            """
        },

        # 6. 商品预警清单
        'ads_product_warning_list': {
            'create_sql': """
DROP TABLE IF EXISTS ads_product_warning_list;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_product_warning_list (
  product_name STRING COMMENT '商品名称',
  warning_type STRING COMMENT '预警类型',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m）',
  warning_reason STRING COMMENT '预警原因'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/ads/ads_product_warning_list';
            """,
            'insert_sql': """
SELECT
  product_name,
  warning_type,
  time_type,
  warning_reason
FROM gmall_ins.dws_product_warning
WHERE ds = '{pt}'
            """
        }
    }

    # 循环处理需要的表
    for table_name in tables_to_process:
        try:
            # 获取当前表的SQL配置
            sql_config = table_sqls[table_name]
            # 创建表
            execute_hive_table_creation(spark, table_name, sql_config['create_sql'])
            # 插入数据（传入分区日期参数）
            execute_hive_insert(spark, table_name, sql_config['insert_sql'], pt_date)
        except Exception as e:
            print(f"[ERROR] 处理 {table_name} 时发生错误: {str(e)}")
            continue

    # 关闭Spark会话
    spark.stop()
    print("[INFO] 所有指定表处理完毕，Spark会话已关闭")


if __name__ == "__main__":
    main()
