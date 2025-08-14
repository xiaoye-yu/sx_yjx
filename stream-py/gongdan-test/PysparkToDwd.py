from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, date_format, lit
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用test数据库"""
    spark = SparkSession.builder \
        .appName("UserPageBehaviorETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE test")
    return spark


def write_to_dwd_user_page_behavior_detail(df: DataFrame, pt: str):
    """写入目标表test.dwd_user_page_behavior_detail，增加表结构校验"""
    try:
        # 校验目标表结构并对齐字段类型（参考ads_city_stats.py的写法）
        target_table = "test.dwd_user_page_behavior_detail"
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


def execute_user_page_behavior_etl(pt: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行用户页面行为ETL，分区日期：{pt}")

    # 读取用户行为表
    ods_user_behavior = spark.table("test.ods_user_behavior") \
        .filter(col("ds") == pt) \
        .filter(col("page_type").isin("首页", "自定义承接页", "商品详情页"))

    # 读取页面结构表
    ods_page_structure = spark.table("test.ods_page_structure") \
        .filter(col("ds") == pt) \
        .select("page_type", "block_id", "block_name", "block_position")

    # 读取交易数据表
    ods_trade_data = spark.table("test.ods_trade_data") \
        .filter(col("ds") == pt) \
        .select("user_id", "guide_block_id", "trade_id", "pay_amount", "pay_time")

    # 关联三张表并计算目标字段
    result_df = ods_user_behavior.alias("u") \
        .join(
            ods_page_structure.alias("s"),
            (col("u.page_type") == col("s.page_type")) &
            (col("u.block_id") == col("s.block_id")),
            "left"
        ) \
        .join(
            ods_trade_data.alias("t"),
            (col("u.user_id") == col("t.user_id")) &
            (col("u.block_id") == col("t.guide_block_id")) &
            (date_format(col("u.click_time"), "yyyy-MM-dd") == date_format(col("t.pay_time"), "yyyy-MM-dd")),
            "left"
        ) \
        .select(
            col("u.user_id"),
            col("u.click_time"),
            col("u.page_type"),
            col("u.block_id"),
            col("s.block_name"),
            col("s.block_position"),
            col("u.channel"),
            col("u.is_click"),
            when(
                col("u.channel").isin("直播间", "短视频", "图文", "微详情"),
                lit(1)
            ).otherwise(lit(0)).alias("is_exclude_channel"),
            col("t.trade_id"),
            coalesce(col("t.pay_amount"), lit(0)).alias("pay_amount"),
            col("t.pay_time"),
            lit(pt).alias("ds")  # 添加分区字段
        )

    # 处理空值（参考ads_city_stats.py的风格，按字段类型分别处理）
    # 字符串类型字段用空字符串填充
    result_df = result_df.fillna({
        "block_name": "",
        "block_position": "",
        "trade_id": ""
    })
    # 数值类型字段用0填充
    result_df = result_df.fillna({
        "pay_amount": 0
    })

    # 打印Schema确认字段类型（调试用）
    print("[INFO] 数据Schema信息：")
    result_df.printSchema()

    # 数据预览
    print("[INFO] 待写入数据样例：")
    result_df.show(5, truncate=False)

    # 写入目标表
    write_to_dwd_user_page_behavior_detail(result_df, pt)
    print(f"[INFO] ETL执行完成，分区日期：{pt}")


if __name__ == "__main__":
    pt = '20250730'  # 可通过调度度工具动态传入
    execute_user_page_behavior_etl(pt)