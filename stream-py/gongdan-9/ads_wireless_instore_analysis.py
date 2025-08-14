from pyspark.sql import SparkSession
from pyspark.sql.types import   DecimalType, LongType
from pyspark.sql.functions import col, coalesce, substring, to_date, lit, trim, length, round, when


def generate_ads_wireless_instore_analysis():
    spark = SparkSession.builder \
        .appName("Ads-Wireless-Instore-Analysis") \
        .getOrCreate()

    # JDBC连接基础参数
    jdbc_base_params = {
        "url": "jdbc:mysql://cdh01:9030/test_yi?useSSL=false&serverTimezone=Asia/Shanghai",
        "user": "root",
        "password": "123456",
        "driver": "com.mysql.cj.jdbc.Driver",
        "batchsize": "5000"  # 批量写入优化
    }

    try:
        # 从源表读取数据
        read_sql = """
        (
        SELECT
            dt,
            store_id,
            page_type,
            wireless_uv,
            wireless_buyer_num
        FROM dws_wireless_pc_summary
        ) t
        """
        read_params = jdbc_base_params.copy()
        read_params["dbtable"] = read_sql
        df = spark.read.format("jdbc").options(**read_params).load()

        # 数据预览
        print(f"✅ 读取原始数据记录数：{df.count()}")
        df.show(5, truncate=False)

        # -------------------------- 数据清洗与转换 --------------------------
        # 1. 处理日期字段：确保有效日期格式
        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")) \
            .filter(col("dt").isNotNull())  # 过滤无效日期

        # 2. 处理店铺ID：非空处理+长度限制
        df = df.withColumn("store_id", coalesce(trim(col("store_id")), lit("unknown_store"))) \
            .withColumn("store_id", substring(col("store_id"), 1, 50))  # 限制50字符

        # 3. 处理页面类型：空值处理为unknown_type+长度限制
        df = df.withColumn("page_type", coalesce(trim(col("page_type")), lit("unknown_type"))) \
            .withColumn("page_type", substring(col("page_type"), 1, 50))  # 限制50字符

        # 4. 处理访客数字段：空值转换为0
        df = df.withColumn("total_uv", coalesce(col("wireless_uv").cast(LongType()), lit(0)))

        # 5. 处理下单买家数字段：空值转换为0
        df = df.withColumn("total_buyer_num", coalesce(col("wireless_buyer_num").cast(LongType()), lit(0)))

        # 6. 计算转化率：避免除零+四舍五入保留4位小数
        df = df.withColumn(
            "conversion_rate",
            round(
                col("total_buyer_num") / when(col("total_uv") == 0, lit(None)).otherwise(col("total_uv")),
                4
            ).cast(DecimalType(10, 4))
        )

        # -------------------------- 数据校验 --------------------------
        print("清洗后数据校验：")
        print(f"- 无效日期记录数：{df.filter(col('dt').isNull()).count()}")
        print(f"- 异常店铺ID记录数：{df.filter((col('store_id').isNull()) | (length(col('store_id')) > 50)).count()}")
        print(
            f"- 异常页面类型记录数：{df.filter((col('page_type').isNull()) | (length(col('page_type')) > 50)).count()}")
        print(f"- 访客数为负数记录数：{df.filter(col('total_uv') < 0).count()}")

        # -------------------------- 写入目标表 --------------------------
        write_params = jdbc_base_params.copy()
        write_params["dbtable"] = "ads_wireless_instore_analysis"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, "
            "store_id VARCHAR(50) NOT NULL, "
            "page_type VARCHAR(50), "
            "total_uv BIGINT, "
            "total_buyer_num BIGINT, "
            "conversion_rate DECIMAL(10,4)"
        )

        df.select(
            "dt", "store_id", "page_type",
            "total_uv", "total_buyer_num", "conversion_rate"
        ).write.format("jdbc").options(**write_params).mode("append").save()

        print("✅ 数据成功写入ads_wireless_instore_analysis")

    except Exception as e:
        print(f"❌ 处理失败：{str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    generate_ads_wireless_instore_analysis()