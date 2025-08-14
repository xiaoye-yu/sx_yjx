from pyspark.sql import SparkSession
from pyspark.sql.types import   DecimalType, IntegerType, LongType
from pyspark.sql.functions import col, coalesce, substring, to_date, lit, trim,  when


def create_spark_session(app_name):
    """创建SparkSession实例"""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def get_jdbc_params():
    """获取JDBC连接参数（复用现有配置）"""
    return {
        "url": "jdbc:mysql://cdh01:9030/test_yi?useSSL=false&serverTimezone=Asia/Shanghai",
        "user": "root",
        "password": "123456",
        "driver": "com.mysql.cj.jdbc.Driver",
        "batchsize": "5000"
    }


def process_ads_wireless_instore_analysis(spark):
    """处理无线端入店与承接分析表"""
    print("\n===== 开始处理ads_wireless_instore_analysis =====")
    jdbc_params = get_jdbc_params()

    try:
        # 读取源数据
        union_sql = """
        (
        SELECT
            dt,
            store_id,
            page_type,
            wireless_uv AS total_uv,
            wireless_buyer_num AS total_buyer_num,
            ROUND(wireless_buyer_num / NULLIF(wireless_uv, 0), 4) AS conversion_rate
        FROM dws_wireless_pc_summary
        WHERE page_type IS NOT NULL
        ) t
        """
        read_params = jdbc_params.copy()
        read_params["dbtable"] = union_sql
        df = spark.read.format("jdbc").options(**read_params).load()

        # 数据清洗
        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")).filter(col("dt").isNotNull())
        df = df.withColumn("store_id", coalesce(trim(col("store_id")), lit("unknown_store"))) \
            .withColumn("store_id", substring(col("store_id"), 1, 50))
        df = df.withColumn("page_type", coalesce(trim(col("page_type")), lit("unknown_type"))) \
            .withColumn("page_type", substring(col("page_type"), 1, 50))
        df = df.withColumn("total_uv", coalesce(col("total_uv").cast(LongType()), lit(0))) \
            .withColumn("total_buyer_num", coalesce(col("total_buyer_num").cast(LongType()), lit(0))) \
            .withColumn("conversion_rate", coalesce(col("conversion_rate").cast(DecimalType(10, 4)), lit(0.0000)))

        # 写入目标表
        write_params = jdbc_params.copy()
        write_params["dbtable"] = "ads_wireless_instore_analysis"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, store_id VARCHAR(50) NOT NULL, page_type VARCHAR(50), "
            "total_uv BIGINT, total_buyer_num BIGINT, conversion_rate DECIMAL(10,4)"
        )
        df.write.format("jdbc").options(**write_params).mode("append").save()
        print("✅ ads_wireless_instore_analysis处理完成")

    except Exception as e:
        print(f"❌ ads_wireless_instore_analysis处理失败: {str(e)}")
        raise


def process_ads_page_visit_rank(spark):
    """处理页面访问排行表"""
    print("\n===== 开始处理ads_page_visit_rank =====")
    jdbc_params = get_jdbc_params()

    try:
        # 读取源数据
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
        read_params = jdbc_params.copy()
        read_params["dbtable"] = union_sql
        df = spark.read.format("jdbc").options(**read_params).load()

        # 数据清洗（复用dws_page_visit_summary的清洗逻辑风格）
        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")).filter(col("dt").isNotNull())
        df = df.withColumn("store_id", coalesce(trim(col("store_id")), lit("unknown_store"))) \
            .withColumn("store_id", substring(col("store_id"), 1, 50))
        df = df.withColumn("page_category", coalesce(trim(col("page_category")), lit("unknown_category"))) \
            .withColumn("page_category", substring(col("page_category"), 1, 50))
        df = df.withColumn("page_subtype",
                           when(col("page_subtype").isNotNull(),
                                substring(trim(col("page_subtype")), 1, 50)))
        df = df.withColumn("page_uv", coalesce(col("page_uv").cast(LongType()), lit(0))) \
            .withColumn("page_pv", coalesce(col("page_pv").cast(LongType()), lit(0))) \
            .withColumn("avg_stay_time",
                        coalesce(col("avg_stay_time").cast(DecimalType(10, 2)),
                                 lit(0.00).cast(DecimalType(10, 2)))) \
            .withColumn("rank_num", coalesce(col("rank_num").cast(IntegerType()), lit(0)))

        # 写入目标表
        write_params = jdbc_params.copy()
        write_params["dbtable"] = "ads_page_visit_rank"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, store_id VARCHAR(50) NOT NULL, page_category VARCHAR(50) NOT NULL, "
            "page_subtype VARCHAR(50), page_uv BIGINT, page_pv BIGINT, "
            "avg_stay_time DECIMAL(10,2), rank_num INT"
        )
        df.write.format("jdbc").options(**write_params).mode("append").save()
        print("✅ ads_page_visit_rank处理完成")

    except Exception as e:
        print(f"❌ ads_page_visit_rank处理失败: {str(e)}")
        raise


def process_ads_instore_flow_analysis(spark):
    """处理店内流转路径分析表"""
    print("\n===== 开始处理ads_instore_flow_analysis =====")
    jdbc_params = get_jdbc_params()

    try:
        # 读取源数据
        union_sql = """
        (
        SELECT
            dt,
            store_id,
            source_page,
            target_page,
            flow_uv,
            ROUND(flow_uv / NULLIF(total_store_flow, 0), 4) AS source_ratio
        FROM dws_instore_flow_summary
        ) t
        """
        read_params = jdbc_params.copy()
        read_params["dbtable"] = union_sql
        df = spark.read.format("jdbc").options(**read_params).load()

        # 数据清洗
        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")).filter(col("dt").isNotNull())
        df = df.withColumn("store_id", coalesce(trim(col("store_id")), lit("unknown_store"))) \
            .withColumn("store_id", substring(col("store_id"), 1, 50))
        df = df.withColumn("source_page", coalesce(trim(col("source_page")), lit("unknown_source"))) \
            .withColumn("source_page", substring(col("source_page"), 1, 100))
        df = df.withColumn("target_page", coalesce(trim(col("target_page")), lit("unknown_target"))) \
            .withColumn("target_page", substring(col("target_page"), 1, 100))
        df = df.withColumn("flow_uv", coalesce(col("flow_uv").cast(LongType()), lit(0))) \
            .withColumn("source_ratio", coalesce(col("source_ratio").cast(DecimalType(10, 4)), lit(0.0000)))

        # 写入目标表
        write_params = jdbc_params.copy()
        write_params["dbtable"] = "ads_instore_flow_analysis"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, store_id VARCHAR(50) NOT NULL, source_page VARCHAR(100) NOT NULL, "
            "target_page VARCHAR(100) NOT NULL, flow_uv BIGINT, source_ratio DECIMAL(10,4)"
        )
        df.write.format("jdbc").options(**write_params).mode("append").save()
        print("✅ ads_instore_flow_analysis处理完成")

    except Exception as e:
        print(f"❌ ads_instore_flow_analysis处理失败: {str(e)}")
        raise


def process_ads_pc_entry_analysis(spark):
    """处理PC端流量入口分析表"""
    print("\n===== 开始处理ads_pc_entry_analysis =====")
    jdbc_params = get_jdbc_params()

    try:
        # 读取源数据
        union_sql = """
        (
        SELECT *
        FROM (
            SELECT
                dt,
                store_id,
                source_page,
                pc_uv AS source_uv,
                ROUND(pc_uv / NULLIF(SUM(pc_uv) OVER (PARTITION BY dt, store_id), 0), 4) AS source_ratio,
                ROW_NUMBER() OVER (PARTITION BY dt, store_id ORDER BY pc_uv DESC) AS top_rank
            FROM dws_wireless_pc_summary
            WHERE source_page IS NOT NULL
        ) t
        WHERE top_rank <= 20
        ) t
        """
        read_params = jdbc_params.copy()
        read_params["dbtable"] = union_sql
        df = spark.read.format("jdbc").options(**read_params).load()

        # 数据清洗
        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")).filter(col("dt").isNotNull())
        df = df.withColumn("store_id", coalesce(trim(col("store_id")), lit("unknown_store"))) \
            .withColumn("store_id", substring(col("store_id"), 1, 50))
        df = df.withColumn("source_page", coalesce(trim(col("source_page")), lit("unknown_source"))) \
            .withColumn("source_page", substring(col("source_page"), 1, 100))
        df = df.withColumn("source_uv", coalesce(col("source_uv").cast(LongType()), lit(0))) \
            .withColumn("source_ratio", coalesce(col("source_ratio").cast(DecimalType(10, 4)), lit(0.0000))) \
            .withColumn("top_rank", coalesce(col("top_rank").cast(IntegerType()), lit(0)))

        # 写入目标表
        write_params = jdbc_params.copy()
        write_params["dbtable"] = "ads_pc_entry_analysis"
        write_params["createTableIfNotExists"] = "false"
        write_params["createTableColumnTypes"] = (
            "dt DATE NOT NULL, store_id VARCHAR(50) NOT NULL, source_page VARCHAR(100) NOT NULL, "
            "source_uv BIGINT, source_ratio DECIMAL(10,4), top_rank INT"
        )
        df.write.format("jdbc").options(**write_params).mode("append").save()
        print("✅ ads_pc_entry_analysis处理完成")

    except Exception as e:
        print(f"❌ ads_pc_entry_analysis处理失败: {str(e)}")
        raise


def main():
    """主函数：按顺序执行4张表的处理流程"""
    spark = None
    try:
        spark = create_spark_session("Ads-Flow-Analysis-Tables-Processing")

        # 依次处理4张表（可根据依赖关系调整执行顺序）
        process_ads_wireless_instore_analysis(spark)
        process_ads_page_visit_rank(spark)
        process_ads_instore_flow_analysis(spark)
        process_ads_pc_entry_analysis(spark)

        print("\n===== 所有表处理完成 =====")

    except Exception as e:
        print(f"\n❌ 整体处理失败: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()