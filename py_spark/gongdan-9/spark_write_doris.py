from pyspark.sql import SparkSession


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
        # 定义合并数据的SQL查询
        union_sql = """
        (
            SELECT
                'wireless_instore' AS data_type,
                visitor_id,
                store_id,
                page_type,
                NULL AS page_category,
                NULL AS page_subtype,
                NULL AS source_page,
                NULL AS target_page,
                visit_time,
                is_buy,
                NULL AS stay_time,
                dt
            FROM ods_wireless_instore_log
            WHERE visit_time IS NOT NULL
            UNION ALL
            SELECT
                'page_visit' AS data_type,
                visitor_id,
                store_id,
                NULL AS page_type,
                page_category,
                page_subtype,
                NULL AS source_page,
                NULL AS target_page,
                visit_time,
                NULL AS is_buy,
                stay_time,
                dt
            FROM ods_page_visit_log
            WHERE stay_time > 0
            UNION ALL
            SELECT
                'instore_flow' AS data_type,
                visitor_id,
                store_id,
                NULL AS page_type,
                NULL AS page_category,
                NULL AS page_subtype,
                source_page,
                target_page,
                jump_time AS visit_time,
                NULL AS is_buy,
                NULL AS stay_time,
                dt
            FROM ods_instore_flow_log
            WHERE source_page IS NOT NULL AND target_page IS NOT NULL
            UNION ALL
            SELECT
                'pc_entry' AS data_type,
                visitor_id,
                store_id,
                NULL AS page_type,
                NULL AS page_category,
                NULL AS page_subtype,
                source_page,
                NULL AS target_page,
                visit_time,
                NULL AS is_buy,
                NULL AS stay_time,
                dt
            FROM ods_pc_entry_log
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

        # 写入到目标表dwd_traffic_unified_detail
        write_params = jdbc_base_params.copy()
        write_params["dbtable"] = "dwd_traffic_unified_detail"

        # 写入模式：如果表存在则覆盖，可根据需要修改为append（追加）或ignore（忽略）
        df.write \
            .format("jdbc") \
            .options(**write_params) \
            .mode("append") \
            .save()

        print("✅ 数据已成功写入 dwd_traffic_unified_detail 表")

    except Exception as e:
        print(f"❌ 错误：{str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    read_and_write_unified_traffic()