from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gmall")
    return spark


# 2. 执行Hive表创建操作
def execute_hive_table_creation():
    spark = get_spark_session()

    table_name = 'dwd_tool_coupon_used_inc'
    hdfs_path = "hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_tool_coupon_used_inc/"

    create_table_sql = f"""
DROP TABLE IF EXISTS {table_name};
CREATE EXTERNAL TABLE {table_name}
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS orc
    LOCATION '{hdfs_path}';
    """


    print(f"[INFO] 开始创建表: {table_name}")
    # 执行多条SQL语句
    for sql in create_table_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {table_name} 创建成功")


# 3. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str):
    spark = get_spark_session()
    table_name = 'dwd_tool_coupon_used_inc'

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    select_sql = f"""
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time,'yyyy-MM-dd') date_id,
    data.used_time as payment_time  -- 重命名为 payment_time 以匹配表结构
from ods_coupon_use data
where ds='20250627'
and data.used_time is not null;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    df_with_partition.write \
        .mode('append') \
        .insertInto(f"gmall.{table_name}")


# 4. 主函数（示例调用）
if __name__ == "__main__":
    # 设置目标分区日期
    target_date = '2025-06-27'

    # 执行表创建操作
    execute_hive_table_creation()

    # 执行插入操作
    execute_hive_insert(target_date)