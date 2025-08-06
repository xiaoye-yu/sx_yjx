from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL_DWS_Multiple_Tables") \
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


def execute_hive_insert(spark, table_name, insert_sql, pt_date, biz_date):
    """执行数据插入操作，支持pt和bizdate两个参数"""
    print(f"[INFO] 开始执行SQL插入 {table_name}，分区日期：{pt_date}，业务日期：{biz_date}")
    # 替换SQL中的两个日期参数
    insert_sql = insert_sql.format(pt=pt_date, bizdate=biz_date)
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
        'dws_product_sales',
        'dws_traffic_source',
        'dws_sku_sales',
        'dws_search_word',
        'dws_price_strength',
        'dws_product_warning'
    ]
    # 设置分区日期(pt)和业务日期(bizdate)
    pt_date = '20250806'  # 分区日期
    biz_date = '2025-08-06'  # 业务日期
    # ======================================================================

    # 初始化Spark会话
    spark = get_spark_session()

    # 定义各表的创建SQL和插入SQL
    table_sqls = {
        # 1. 商品销售汇总表
        'dws_product_sales': {
            'create_sql': """
DROP TABLE IF EXISTS dws_product_sales;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_product_sales (
  product_name STRING COMMENT '商品名称',
  product_category STRING COMMENT '商品分类',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_sales_amount DECIMAL(12,2) COMMENT '总销售额',
  total_sales_quantity INT COMMENT '总销量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/dws/dws_product_sales'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '商品销售多粒度汇总');
            """,
            'insert_sql': """
SELECT
   product_name, product_category, 'd' AS time_type,
  stat_date AS start_date, stat_date AS end_date,
  SUM(sales_amount) AS total_sales_amount, SUM(sales_quantity) AS total_sales_quantity
FROM gmall_ins.dwd_product_sales_daily
GROUP BY  product_name, product_category, stat_date
-- 7天
UNION ALL
SELECT
   product_name, product_category, '7d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 6) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(sales_amount) AS total_sales_amount, SUM(sales_quantity) AS total_sales_quantity
FROM gmall_ins.dwd_product_sales_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 6) AND TO_DATE('{bizdate}')
GROUP BY  product_name, product_category
-- 30天
UNION ALL
SELECT
  product_name, product_category, '30d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 29) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(sales_amount) AS total_sales_amount, SUM(sales_quantity) AS total_sales_quantity
FROM gmall_ins.dwd_product_sales_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 29) AND TO_DATE('{pt}')
GROUP BY product_name, product_category
            """
        },

        # 2. 流量来源汇总表
        'dws_traffic_source': {
            'create_sql': """
DROP TABLE IF EXISTS dws_traffic_source;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_traffic_source (
  source_name STRING COMMENT '流量来源渠道',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_visitor_count INT COMMENT '总访客数',
  total_pay_conversion_rate DECIMAL(6,4) COMMENT '总支付转化率'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/dws/dws_traffic_source'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '流量来源多粒度汇总');
            """,
            'insert_sql': """
SELECT
  source_name, 'd' AS time_type,
  stat_date AS start_date, stat_date AS end_date,
  SUM(visitor_count) AS total_visitor_count,
  SUM(pay_buyer_count)/SUM(visitor_count) AS total_pay_conversion_rate
FROM gmall_ins.dwd_traffic_source_daily
GROUP BY source_name,stat_date
union
SELECT
  source_name, '7d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 6) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(visitor_count) AS total_visitor_count,
  SUM(pay_buyer_count)/SUM(visitor_count) AS total_pay_conversion_rate
FROM gmall_ins.dwd_traffic_source_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 6) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 6), 'yyyyMMdd') AND '{pt}'
GROUP BY source_name
union
SELECT
  source_name, '30d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 29) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(visitor_count) AS total_visitor_count,
  SUM(pay_buyer_count)/SUM(visitor_count) AS total_pay_conversion_rate
FROM gmall_ins.dwd_traffic_source_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 29) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 29), 'yyyyMMdd') AND '{pt}'
GROUP BY source_name
            """
        },

        # 3. SKU 销售汇总表
        'dws_sku_sales': {
            'create_sql': """
DROP TABLE IF EXISTS dws_sku_sales;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_sku_sales (
  sku_id STRING COMMENT 'SKU ID',
  product_name STRING COMMENT '商品名称',
  sku_info STRING COMMENT 'SKU信息',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_pay_quantity INT COMMENT '总支付件数',
  current_stock INT COMMENT '当前库存',
  stock_available_days DECIMAL(6,2) COMMENT '库存可售天数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/dws/dws_sku_sales'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = 'SKU销售多粒度汇总');
            """,
            'insert_sql': """
SELECT
  sku_id, product_name, sku_info, 'd' AS time_type,
  stat_date   AS start_date, stat_date AS end_date,
  SUM(pay_quantity) AS total_pay_quantity,
  MAX(current_stock) AS current_stock,  -- 取结束日库存
  AVG(stock_available_days) AS stock_available_days
FROM gmall_ins.dwd_sku_sales_inventory_daily
GROUP BY sku_id, product_name, sku_info,stat_date
union
SELECT
  sku_id, product_name, sku_info, '7d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 6) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(pay_quantity) AS total_pay_quantity,
  MAX(current_stock) AS current_stock,  -- 取结束日库存
  AVG(stock_available_days) AS stock_available_days
FROM gmall_ins.dwd_sku_sales_inventory_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 6) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 6), 'yyyyMMdd') AND '{pt}'
GROUP BY sku_id, product_name, sku_info
union
SELECT
  sku_id, product_name, sku_info, '30d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 29) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(pay_quantity) AS total_pay_quantity,
  MAX(current_stock) AS current_stock,  -- 取结束日库存
  AVG(stock_available_days) AS stock_available_days
FROM gmall_ins.dwd_sku_sales_inventory_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 29) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 29), 'yyyyMMdd') AND '{pt}'
GROUP BY sku_id, product_name, sku_info
            """
        },

        # 4. 搜索词汇总表
        'dws_search_word': {
            'create_sql': """
DROP TABLE IF EXISTS dws_search_word;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_search_word (
  search_word STRING COMMENT '搜索词',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_visitor_count INT COMMENT '总访客数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/dws/dws_search_word'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '搜索词多粒度汇总');
            """,
            'insert_sql': """
SELECT
  search_word, 'd' AS time_type,
 stat_date AS start_date, stat_date AS end_date,
 SUM(visitor_count) AS total_visitor_count
FROM gmall_ins.dwd_search_word_daily
GROUP BY search_word,stat_date
union
SELECT
  search_word, '7d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 6) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(visitor_count) AS total_visitor_count
FROM gmall_ins.dwd_search_word_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 6) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 6), 'yyyyMMdd') AND '{pt}'
GROUP BY search_word
union
SELECT
  search_word, '30d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 29) AS start_date, TO_DATE('{bizdate}') AS end_date,
  SUM(visitor_count) AS total_visitor_count
FROM gmall_ins.dwd_search_word_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 29) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 29), 'yyyyMMdd') AND '{pt}'
GROUP BY search_word
            """
        },

        # 5. 价格力商品汇总表
        'dws_price_strength': {
            'create_sql': """
DROP TABLE IF EXISTS dws_price_strength;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_price_strength (
  price_strength_level STRING COMMENT '价格力等级（优秀/良好/较差）',
  product_category STRING COMMENT '商品分类',
  time_type STRING COMMENT '时间粒度（d/7d/30d）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  product_count INT COMMENT '商品数量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/dws/dws_price_strength'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '价格力商品多粒度汇总');
            """,
            'insert_sql': """
SELECT
  price_strength_level, product_category, 'd' AS time_type,
  TO_DATE('{bizdate}') AS start_date, TO_DATE('{bizdate}') AS end_date,
  COUNT(DISTINCT product_id) AS product_count
FROM gmall_ins.dwd_price_strength_daily
GROUP BY price_strength_level, product_category
union
SELECT
  price_strength_level, product_category, '7d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 6) AS start_date, TO_DATE('{bizdate}') AS end_date,
  COUNT(DISTINCT product_id) AS product_count
FROM gmall_ins.dwd_price_strength_daily
WHERE ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 6), 'yyyyMMdd') AND '{pt}'
GROUP BY price_strength_level, product_category
union
SELECT
  price_strength_level, product_category, '30d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 29) AS start_date, TO_DATE('{bizdate}') AS end_date,
  COUNT(DISTINCT product_id) AS product_count
FROM gmall_ins.dwd_price_strength_daily
WHERE ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 29), 'yyyyMMdd') AND '{pt}'
GROUP BY price_strength_level, product_category
            """
        },

        # 6. 商品预警汇总表
        'dws_product_warning': {
            'create_sql': """
DROP TABLE IF EXISTS dws_product_warning;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_product_warning (
  product_name STRING COMMENT '商品名称',
  warning_type STRING COMMENT '预警类型（价格力/商品力）',
  warning_reason STRING COMMENT '预警原因',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/gmall_ins/dws/dws_product_warning'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '商品预警多粒度汇总');
            """,
            'insert_sql': """
SELECT
  product_name, warning_type, warning_reason, 'd' AS time_type,
  TO_DATE('{bizdate}') AS start_date, TO_DATE('{bizdate}') AS end_date
FROM gmall_ins.dwd_product_warning_daily
union
SELECT
  product_name, warning_type, warning_reason, '7d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 6) AS start_date, TO_DATE('{bizdate}') AS end_date
FROM gmall_ins.dwd_product_warning_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 6) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 6), 'yyyyMMdd') AND '{pt}'
union
SELECT
  product_name, warning_type, warning_reason, '30d' AS time_type,
  DATE_SUB(TO_DATE('{bizdate}'), 29) AS start_date, TO_DATE('{bizdate}') AS end_date
FROM gmall_ins.dwd_product_warning_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE('{bizdate}'), 29) AND TO_DATE('{bizdate}')
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE('{bizdate}'), 29), 'yyyyMMdd') AND '{pt}'
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
            # 插入数据（传入两个日期参数）
            execute_hive_insert(spark, table_name, sql_config['insert_sql'], pt_date, biz_date)
        except Exception as e:
            print(f"[ERROR] 处理 {table_name} 时发生错误: {str(e)}")
            continue

    # 关闭Spark会话
    spark.stop()
    print("[INFO] 所有指定表处理完毕，Spark会话已关闭")


if __name__ == "__main__":
    main()