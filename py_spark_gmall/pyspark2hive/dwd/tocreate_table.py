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
        # 交易域：购物车周期快照事实表
        """
        DROP TABLE IF EXISTS dwd_trade_cart_full;
        CREATE EXTERNAL TABLE dwd_trade_cart_full (
            id STRING COMMENT '编号',
            user_id STRING COMMENT '用户ID',
            sku_id STRING COMMENT 'SKU_ID',
            sku_name STRING COMMENT '商品名称',
            sku_num BIGINT COMMENT '现存商品件数'
        )
        COMMENT '交易域购物车周期快照事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_cart_full/';
        """,

        # 交易域：加购事务事实表
        """
        DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
        CREATE EXTERNAL TABLE dwd_trade_cart_add_inc (
            id STRING COMMENT '编号',
            user_id STRING COMMENT '用户ID',
            sku_id STRING COMMENT 'SKU_ID',
            date_id STRING COMMENT '日期ID',
            create_time STRING COMMENT '加购时间',
            sku_num STRING COMMENT '加购物车件数'
        )
        COMMENT '交易域加购事务事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_cart_add_inc/';
        """,

        # 交易域：下单事务事实表
        """
        DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
        CREATE EXTERNAL TABLE dwd_trade_order_detail_inc (
            id STRING COMMENT '编号',
            order_id STRING COMMENT '订单ID',
            user_id STRING COMMENT '用户ID',
            sku_id STRING COMMENT '商品ID',
            province_id STRING COMMENT '省份ID',
            activity_id STRING COMMENT '参与活动ID',
            activity_rule_id STRING COMMENT '参与活动规则ID',
            coupon_id STRING COMMENT '使用优惠券ID',
            date_id STRING COMMENT '下单日期ID',
            create_time STRING COMMENT '下单时间',
            sku_num BIGINT COMMENT '商品数量',
            split_original_amount DECIMAL(16,2) COMMENT '原始价格',
            split_activity_amount DECIMAL(16,2) COMMENT '活动优惠分摊',
            split_coupon_amount DECIMAL(16,2) COMMENT '优惠券优惠分摊',
            split_total_amount DECIMAL(16,2) COMMENT '最终价格分摊'
        )
        COMMENT '交易域下单事务事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_order_detail_inc/';
        """,

        # 交易域：支付成功事务事实表
        """
        DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
        CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc (
            id STRING COMMENT '编号',
            order_id STRING COMMENT '订单ID',
            user_id STRING COMMENT '用户ID',
            sku_id STRING COMMENT 'SKU_ID',
            province_id STRING COMMENT '省份ID',
            activity_id STRING COMMENT '参与活动ID',
            activity_rule_id STRING COMMENT '参与活动规则ID',
            coupon_id STRING COMMENT '使用优惠券ID',
            payment_type_code STRING COMMENT '支付类型编码',
            payment_type_name STRING COMMENT '支付类型名称',
            date_id STRING COMMENT '支付日期ID',
            callback_time STRING COMMENT '支付成功时间',
            sku_num BIGINT COMMENT '商品数量',
            split_original_amount DECIMAL(16,2) COMMENT '应支付原始金额',
            split_activity_amount DECIMAL(16,2) COMMENT '支付活动优惠分摊',
            split_coupon_amount DECIMAL(16,2) COMMENT '支付优惠券优惠分摊',
            split_payment_amount DECIMAL(16,2) COMMENT '支付金额'
        )
        COMMENT '交易域支付成功事务事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/';
        """,

        # 交易域：交易流程累积快照事实表
        """
        DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
        CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc (
            order_id STRING COMMENT '订单ID',
            user_id STRING COMMENT '用户ID',
            province_id STRING COMMENT '省份ID',
            order_date_id STRING COMMENT '下单日期ID',
            order_time STRING COMMENT '下单时间',
            payment_date_id STRING COMMENT '支付日期ID',
            payment_time STRING COMMENT '支付时间',
            finish_date_id STRING COMMENT '确认收货日期ID',
            finish_time STRING COMMENT '确认收货时间',
            order_original_amount DECIMAL(16,2) COMMENT '下单原始价格',
            order_activity_amount DECIMAL(16,2) COMMENT '下单活动优惠分摊',
            order_coupon_amount DECIMAL(16,2) COMMENT '下单优惠券优惠分摊',
            order_total_amount DECIMAL(16,2) COMMENT '下单最终价格分摊',
            payment_amount DECIMAL(16,2) COMMENT '支付金额'
        )
        COMMENT '交易域交易流程累积快照事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_trade_flow_acc/';
        """,

        # 流量域：页面浏览事实表
        """
        DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
        CREATE EXTERNAL TABLE dwd_traffic_page_view_inc (
            province_id STRING COMMENT '省份ID',
            brand STRING COMMENT '手机品牌',
            channel STRING COMMENT '渠道',
            is_new STRING COMMENT '是否首次启动',
            model STRING COMMENT '手机型号',
            mid_id STRING COMMENT '设备ID',
            operate_system STRING COMMENT '操作系统',
            user_id STRING COMMENT '会员ID',
            version_code STRING COMMENT 'APP版本号',
            page_item STRING COMMENT '目标ID',
            page_item_type STRING COMMENT '目标类型',
            last_page_id STRING COMMENT '上页ID',
            page_id STRING COMMENT '页面ID',
            from_pos_id STRING COMMENT '点击坑位ID',
            from_pos_seq STRING COMMENT '点击坑位位置',
            refer_id STRING COMMENT '营销渠道ID',
            date_id STRING COMMENT '日期ID',
            view_time STRING COMMENT '跳入时间',
            session_id STRING COMMENT '所属会话ID',
            during_time BIGINT COMMENT '持续时间毫秒'
        )
        COMMENT '流量域页面浏览事务事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_traffic_page_view_inc/';
        """,

        # 用户域：登录事务事实表
        """
        DROP TABLE IF EXISTS dwd_user_login_inc;
        CREATE EXTERNAL TABLE dwd_user_login_inc (
            user_id STRING COMMENT '用户ID',
            date_id STRING COMMENT '日期ID',
            login_time STRING COMMENT '登录时间',
            channel STRING COMMENT '应用下载渠道',
            province_id STRING COMMENT '省份ID',
            version_code STRING COMMENT '应用版本',
            mid_id STRING COMMENT '设备ID',
            brand STRING COMMENT '设备品牌',
            model STRING COMMENT '设备型号',
            operate_system STRING COMMENT '设备操作系统'
        )
        COMMENT '用户域用户登录事务事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_user_login_inc/';
        """,

        # 用户域：注册事务事实表
        """
        DROP TABLE IF EXISTS dwd_user_register_inc;
        CREATE EXTERNAL TABLE dwd_user_register_inc (
            user_id STRING COMMENT '用户ID',
            date_id STRING COMMENT '日期ID',
            create_time STRING COMMENT '注册时间',
            channel STRING COMMENT '应用下载渠道',
            province_id STRING COMMENT '省份ID',
            version_code STRING COMMENT '应用版本',
            mid_id STRING COMMENT '设备ID',
            brand STRING COMMENT '设备品牌',
            model STRING COMMENT '设备型号',
            operate_system STRING COMMENT '设备操作系统'
        )
        COMMENT '用户域用户注册事务事实表'
        PARTITIONED BY (ds STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_user_register_inc/';
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
    spark.stop()
