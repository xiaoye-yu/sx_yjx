from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, DecimalType
from pyspark.sql.functions import col, coalesce, substring, to_date, lit, trim, length, when

def read_and_write_unified_traffic():
    spark = SparkSession.builder \
        .appName("Unified-Traffic-Detail") \
        .getOrCreate()

    jdbc_base_params = {
        "url": "jdbc:mysql://cdh01:9030/test_yi?useSSL=false&serverTimezone=Asia/Shanghai",
        "user": "root",
        "password": "123456",
        "driver": "com.mysql.cj.jdbc.Driver",
        "batchsize": "5000"  # 优化批量写入
    }

    try:
        # 读取数据（SQL不变）
        union_sql = """
        (
        SELECT
            dt,
            store_id,
            page_category,
            page_subtype,
            COUNT(DISTINCT visitor_id) AS page_uv,
            COUNT(*) AS page_pv,
            AVG(stay_time) AS avg_stay_time
        FROM dwd_traffic_unified_detail
        WHERE data_type = 'page_visit'
        GROUP BY dt, store_id, page_category, page_subtype
        ) t
        """
        read_params = jdbc_base_params.copy()
        read_params["dbtable"] = union_sql
        df = spark.read.format("jdbc").options(**read_params).load()

        # 数据预览
        print("✅ 读取记录数：{}".format(df.count()))
        df.show(5, truncate=False)

        # -------------------------- 核心：数据清洗 --------------------------
        # 1. 处理dt：只保留有效日期（显式指定格式，过滤无效值）
        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")) .filter(col("dt").isNotNull())

        # 2. 处理store_id：非空+长度限制（50字符）
        df = df.withColumn("store_id", coalesce(trim(col("store_id")), lit("unknown_store"))) \
               .withColumn("store_id", substring(col("store_id"), 1, 50))

        # 3. 处理page_category：非空+长度限制（50字符）
        df = df.withColumn("page_category", coalesce(trim(col("page_category")), lit("unknown_category"))) \
               .withColumn("page_category", substring(col("page_category"), 1, 50))

        # 4. 处理page_subtype：允许空值，但限制长度（50字符）
        df = df.withColumn("page_subtype",
                          when(col("page_subtype").isNotNull(),
                               substring(trim(col("page_subtype")), 1, 50)))  # 只处理非空值

        # 5. 处理数值字段：确保非NULL+类型匹配
        df = df.withColumn("page_uv", coalesce(col("page_uv").cast("BIGINT"), lit(0))) \
               .withColumn("page_pv", coalesce(col("page_pv").cast("BIGINT"), lit(0))) \
               .withColumn("avg_stay_time",
                          coalesce(col("avg_stay_time").cast(DecimalType(10, 2)),
                                   lit(0.00).cast(DecimalType(10, 2))))

        # -------------------------- 验证清洗结果 --------------------------
        print("清洗后校验：")
        print(f"- dt为NULL的记录数：{df.filter(col('dt').isNull()).count()}")
        print(f"- store_id为NULL/超长的记录数：{df.filter((col('store_id').isNull()) | (length(col('store_id')) > 50)).count()}")
        print(f"- page_category为NULL/超长的记录数：{df.filter((col('page_category').isNull()) | (length(col('page_category')) > 50)).count()}")

        # -------------------------- 写入数据 --------------------------
        write_params = jdbc_base_params.copy()
        write_params["dbtable"] = "dws_page_visit_summary"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, "
            "store_id VARCHAR(50) NOT NULL, "
            "page_category VARCHAR(50) NOT NULL, "
            "page_subtype VARCHAR(50), "
            "page_uv BIGINT, "
            "page_pv BIGINT, "
            "avg_stay_time DECIMAL(10,2)"
        )

        df.write.format("jdbc").options(** write_params).mode("append").save()
        print("✅ 数据写入成功")

    except Exception as e:
        print(f"❌ 错误：{str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    read_and_write_unified_traffic()