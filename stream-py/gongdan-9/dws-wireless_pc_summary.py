from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType

def read_and_write_unified_traffic():
    # 初始化SparkSession
    spark = SparkSession.builder \
        .appName("Unified-Traffic-Detail") \
        .getOrCreate()

    # JDBC连接基础参数
    jdbc_base_params = {
        "url": "jdbc:mysql://cdh01:9030/test_yi?useSSL=false&serverTimezone=Asia/Shanghai",
        "user": "root",
        "password": "123456",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        # 定义合并数据的SQL查询（修复分号问题）
        union_sql = """
        (
        SELECT
            dt,
            store_id,
            page_type,
            NULL AS source_page,
            COUNT(DISTINCT visitor_id) AS wireless_uv,
            COUNT(DISTINCT CASE WHEN is_buy = 1 THEN visitor_id END) AS wireless_buyer_num,
            NULL AS pc_uv
        FROM dwd_traffic_unified_detail
        WHERE data_type = 'wireless_instore'
        GROUP BY dt, store_id, page_type
        UNION ALL
        SELECT
            dt,
            store_id,
            NULL AS page_type,
            source_page,
            NULL AS wireless_uv,
            NULL AS wireless_buyer_num,
            COUNT(DISTINCT visitor_id) AS pc_uv
        FROM dwd_traffic_unified_detail
        WHERE data_type = 'pc_entry'
        GROUP BY dt, store_id, source_page
        ) t
        """

        # 读取合并后的数据集
        read_params = jdbc_base_params.copy()
        read_params["dbtable"] = union_sql
        df = spark.read \
            .format("jdbc") \
            .options(**read_params) \
            .load()

        # 显示读取结果信息
        print("✅ 成功读取合并数据，共 {} 条记录".format(df.count()))
        print("数据结构：")
        df.printSchema()
        print("前5条数据预览：")
        df.show(5, truncate=False)

        # 写入到目标表dws_wireless_pc_summary
        write_params = jdbc_base_params.copy()
        write_params["dbtable"] = "dws_wireless_pc_summary"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE, "
            "store_id VARCHAR(50), "
            "page_type VARCHAR(50), "
            "source_page VARCHAR(100), "
            "wireless_uv BIGINT, "
            "wireless_buyer_num BIGINT, "
            "pc_uv BIGINT"
        )

        # 数据类型转换（匹配新表字段）
        df = df.withColumn("dt", df["dt"].cast(DateType())) \
            .withColumn("store_id", df["store_id"].cast(StringType())) \
            .withColumn("page_type", df["page_type"].cast(StringType())) \
            .withColumn("source_page", df["source_page"].cast(StringType())) \
            .withColumn("wireless_uv", df["wireless_uv"].cast("BIGINT")) \
            .withColumn("wireless_buyer_num", df["wireless_buyer_num"].cast("BIGINT")) \
            .withColumn("pc_uv", df["pc_uv"].cast("BIGINT"))

        df.write \
            .format("jdbc") \
            .options(** write_params) \
            .mode("append") \
            .save()

        print("✅ 数据已成功写入 dws_wireless_pc_summary 表")

    except Exception as e:
        print(f"❌ 错误：{str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    read_and_write_unified_traffic()