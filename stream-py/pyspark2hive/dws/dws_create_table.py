from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "HiveETL"):
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE gmall")
    return spark


def create_hive_tables(spark):
    """执行创建 Hive 外部表的 DDL"""
    ddl_statements = [
        # 你提供的新表 DDL
        """
        DROP TABLE IF EXISTS dws_interaction_sku_favor_add_1d;
        CREATE EXTERNAL TABLE dws_interaction_sku_favor_add_1d
        (
            `sku_id`             STRING COMMENT 'SKU_ID',
            `sku_name`           STRING COMMENT 'SKU名称',
            `category1_id`       STRING COMMENT '一级品类ID',
            `category1_name`     STRING COMMENT '一级品类名称',
            `category2_id`       STRING COMMENT '二级品类ID',
            `category2_name`     STRING COMMENT '二级品类名称',
            `category3_id`       STRING COMMENT '三级品类ID',
            `category3_name`     STRING COMMENT '三级品类名称',
            `tm_id`              STRING COMMENT '品牌ID',
            `tm_name`            STRING COMMENT '品牌名称',
            `favor_add_count_1d` BIGINT COMMENT '商品被收藏次数'
        ) COMMENT '互动域商品粒度收藏商品最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS orc 
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_interaction_sku_favor_add_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_tool_user_coupon_coupon_used_1d;
        CREATE EXTERNAL TABLE dws_tool_user_coupon_coupon_used_1d
        (
            `user_id`          STRING COMMENT '用户ID',
            `coupon_id`        STRING COMMENT '优惠券ID',
            `coupon_name`      STRING COMMENT '优惠券名称',
            `coupon_type_code` STRING COMMENT '优惠券类型编码',
            `coupon_type_name` STRING COMMENT '优惠券类型名称',
            `benefit_rule`     STRING COMMENT '优惠规则',
            `used_count_1d`    STRING COMMENT '使用(支付)次数'
        ) COMMENT '工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_tool_user_coupon_coupon_used_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_trade_province_order_1d;
        CREATE EXTERNAL TABLE dws_trade_province_order_1d
        (
            `province_id`               STRING COMMENT '省份ID',
            `province_name`             STRING COMMENT '省份名称',
            `area_code`                 STRING COMMENT '地区编码',
            `iso_code`                  STRING COMMENT '旧版国际标准地区编码',
            `iso_3166_2`                STRING COMMENT '新版国际标准地区编码',
            `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
            `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
            `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
            `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
            `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
        ) COMMENT '交易域省份粒度订单最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_trade_province_order_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
        CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
        (
            `user_id`           STRING COMMENT '用户ID',
            `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
            `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
        ) COMMENT '交易域用户粒度加购最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_trade_user_cart_add_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_trade_user_order_1d;
        CREATE EXTERNAL TABLE dws_trade_user_order_1d
        (
            `user_id`                   STRING COMMENT '用户ID',
            `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
            `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
            `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
            `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
            `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
            `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
        ) COMMENT '交易域用户粒度订单最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_trade_user_order_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_trade_user_payment_1d;
        CREATE EXTERNAL TABLE dws_trade_user_payment_1d
        (
            `user_id`           STRING COMMENT '用户ID',
            `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
            `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
            `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
        ) COMMENT '交易域用户粒度支付最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_trade_user_payment_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;
        CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
        (
            `user_id`                   STRING COMMENT '用户ID',
            `sku_id`                    STRING COMMENT 'SKU_ID',
            `sku_name`                  STRING COMMENT 'SKU名称',
            `category1_id`              STRING COMMENT '一级品类ID',
            `category1_name`            STRING COMMENT '一级品类名称',
            `category2_id`              STRING COMMENT '二级品类ID',
            `category2_name`            STRING COMMENT '二级品类名称',
            `category3_id`              STRING COMMENT '三级品类ID',
            `category3_name`            STRING COMMENT '三级品类名称',
            `tm_id`                      STRING COMMENT '品牌ID',
            `tm_name`                    STRING COMMENT '品牌名称',
            `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
            `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
            `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
            `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
            `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
            `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
        ) COMMENT '交易域用户商品粒度订单最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_trade_user_sku_order_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
        CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
        (
            `mid_id`         STRING COMMENT '访客ID',
            `brand`          string comment '手机品牌',
            `model`          string comment '手机型号',
            `operate_system` string comment '操作系统',
            `page_id`        STRING COMMENT '页面ID',
            `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
            `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
        ) COMMENT '流量域访客页面粒度页面浏览最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d';
        """,
        """
        DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
        CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
        (
            `session_id`     STRING COMMENT '会话ID',
            `mid_id`         string comment '设备ID',
            `brand`          string comment '手机品牌',
            `model`          string comment '手机型号',
            `operate_system` string comment '操作系统',
            `version_code`   string comment 'APP版本号',
            `channel`        string comment '渠道',
            `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
            `page_count_1d`  BIGINT COMMENT '最近1日浏览页面数'
        ) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
            PARTITIONED BY (`ds` STRING)
            STORED AS ORC
            LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_traffic_session_page_view_1d';
        """
    ]

    for ddl in ddl_statements:
        sql_lines = ddl.strip().split(';')  # 拆分多条语句
        for sql in sql_lines:
            sql = sql.strip()
            if sql:
                print(f"[EXEC] Executing SQL:\n{sql}")
                try:
                    spark.sql(sql)
                except Exception as e:
                    print(f"[ERROR] Failed to execute SQL:\n{sql}\n[Exception] {e}")
        print("[INFO] ✅ Executed DDL Block.\n" + "-" * 60)


if __name__ == "__main__":
    spark = get_spark_session("CreateHiveTables")
    create_hive_tables(spark)
