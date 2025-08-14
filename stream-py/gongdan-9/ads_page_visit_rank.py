from pyspark.sql import SparkSession
from pyspark.sql.types import  DecimalType, IntegerType, LongType
from pyspark.sql.functions import col, coalesce, substring, to_date, lit, trim, length,when

def generate_ads_page_visit_rank():
    spark = SparkSession.builder \
        .appName("Ads-Page-Visit-Rank") \
        .getOrCreate()

    jdbc_base_params = {
        "url": "jdbc:mysql://cdh01:9030/test_yi?useSSL=false&serverTimezone=Asia/Shanghai",
        "user": "root",
        "password": "123456",
        "driver": "com.mysql.cj.jdbc.Driver",
        "batchsize": "5000"  # 优化批量写入
    }

    try:
        # 读取数据（包含排名计算的SQL）
        union_sql = """
        (
        SELECT
            dt,
            store_id,
            page_category,
            page_subtype,
            page_uv,
            page_pv,
            avg_stay_time,
            ROW_NUMBER() OVER (PARTITION BY dt, store_id ORDER BY page_uv DESC) AS rank_num
        FROM dws_page_visit_summary
        ) t
        """
        read_params = jdbc_base_params.copy()
        read_params["dbtable"] = union_sql
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

        # 3. 处理页面大类
        df = df.withColumn("page_category", coalesce(trim(col("page_category")), lit("unknown_category"))) \
               .withColumn("page_category", substring(col("page_category"), 1, 50))

        # 4. 处理页面细分类型
        df = df.withColumn("page_subtype",when(col("page_subtype").isNotNull(),
                               substring(trim(col("page_subtype")), 1, 50)))

        # 5. 处理数值字段
        df = df.withColumn("page_uv", coalesce(col("page_uv").cast(LongType()), lit(0))) \
               .withColumn("page_pv", coalesce(col("page_pv").cast(LongType()), lit(0))) \
               .withColumn("avg_stay_time",
                          coalesce(col("avg_stay_time").cast(DecimalType(10, 2)),
                                   lit(0.00).cast(DecimalType(10, 2)))) \
               .withColumn("rank_num", coalesce(col("rank_num").cast(IntegerType()), lit(0)))

        # -------------------------- 验证清洗结果 --------------------------
        print("清洗后校验：")
        print(f"- dt为NULL的记录数：{df.filter(col('dt').isNull()).count()}")
        print(f"- store_id异常记录数：{df.filter((col('store_id').isNull()) | (length(col('store_id')) > 50)).count()}")
        print(f"- page_category异常记录数：{df.filter((col('page_category').isNull()) | (length(col('page_category')) > 50)).count()}")

        # -------------------------- 写入数据 --------------------------
        write_params = jdbc_base_params.copy()
        write_params["dbtable"] = "ads_page_visit_rank"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, "
            "store_id VARCHAR(50) NOT NULL, "
            "page_category VARCHAR(50) NOT NULL, "
            "page_subtype VARCHAR(50), "
            "page_uv BIGINT, "
            "page_pv BIGINT, "
            "avg_stay_time DECIMAL(10,2), "
            "rank_num INT"
        )

        df.write.format("jdbc").options(** write_params).mode("append").save()
        print("✅ 数据写入ads_page_visit_rank成功")

    except Exception as e:
        print(f"❌ 错误：{str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    generate_ads_page_visit_rank()