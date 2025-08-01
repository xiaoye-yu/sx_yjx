from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, LongType
from pyspark.sql.functions import col, coalesce, substring, to_date, lit, trim, length, count, sum, expr

def process_instore_flow_summary():
    spark = SparkSession.builder \
        .appName("Instore-Flow-Summary") \
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
        # 读取源数据
        read_sql = """
        (
         SELECT
            dt,
            store_id,
            source_page,
            target_page,
            COUNT(visitor_id) AS flow_uv,
            SUM(COUNT(visitor_id)) OVER (PARTITION BY dt, store_id) AS total_store_flow
        FROM dwd_traffic_unified_detail
        WHERE data_type = 'instore_flow'
        GROUP BY dt, store_id, source_page, target_page
        ) t
        """
        read_params = jdbc_base_params.copy()
        read_params["dbtable"] = read_sql
        df = spark.read.format("jdbc").options(**read_params).load()

        # 数据预览
        print("✅ 读取记录数：{}".format(df.count()))
        df.show(5, truncate=False)

        # -------------------------- 数据清洗 --------------------------
        # 1. 处理日期字段
        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")) \
               .filter(col("dt").isNotNull())

        # 2. 处理店铺ID
        df = df.withColumn("store_id", coalesce(trim(col("store_id")), lit("unknown_store"))) \
               .withColumn("store_id", substring(col("store_id"), 1, 50))

        # 3. 处理来源页面
        df = df.withColumn("source_page", coalesce(trim(col("source_page")), lit("unknown_source"))) \
               .withColumn("source_page", substring(col("source_page"), 1, 100))

        # 4. 处理去向页面
        df = df.withColumn("target_page", coalesce(trim(col("target_page")), lit("unknown_target"))) \
               .withColumn("target_page", substring(col("target_page"), 1, 100))

        # 确保数值字段非空
        result_df = df.withColumn("flow_uv", coalesce(col("flow_uv").cast(LongType()), lit(0))) \
                             .withColumn("total_store_flow", coalesce(col("total_store_flow"), lit(0)))

        # -------------------------- 验证清洗结果 --------------------------
        print("清洗后校验：")
        print(f"- dt为NULL的记录数：{result_df.filter(col('dt').isNull()).count()}")
        print(f"- store_id异常记录数：{result_df.filter((col('store_id').isNull()) | (length(col('store_id')) > 50)).count()}")
        print(f"- source_page异常记录数：{result_df.filter((col('source_page').isNull()) | (length(col('source_page')) > 100)).count()}")
        print(f"- target_page异常记录数：{result_df.filter((col('target_page').isNull()) | (length(col('target_page')) > 100)).count()}")

        # -------------------------- 写入数据 --------------------------
        write_params = jdbc_base_params.copy()
        write_params["dbtable"] = "dws_instore_flow_summary"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, "
            "store_id VARCHAR(50) NOT NULL, "
            "source_page VARCHAR(100) NOT NULL, "
            "target_page VARCHAR(100) NOT NULL, "
            "flow_uv BIGINT, "
            "total_store_flow BIGINT"
        )

        result_df.write.format("jdbc").options(** write_params).mode("append").save()
        print("✅ 数据写入成功")

    except Exception as e:
        print(f"❌ 错误：{str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    process_instore_flow_summary()