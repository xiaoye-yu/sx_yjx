
drop table if exists ods_attribute_info;
CREATE external TABLE IF NOT EXISTS ods_attribute_info (
    id INT,
    item_id STRING,
    category_id STRING,
    category_name STRING,
    attr_id STRING,
    attr_value STRING,
    visitor_id STRING,
    is_pay TINYINT,
    visit_time STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/ods/ods_attribute_info'
TBLPROPERTIES (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);

drop table if exists ods_sales_full;
CREATE external TABLE IF NOT EXISTS ods_sales_full (
    id INT,
    item_id STRING,
    item_name STRING,
    category_id STRING,
    category_name STRING,
    pay_amount DECIMAL(10,2),
    pay_quantity INT,
    target_gmv DECIMAL(12,2),
    last_month_rank INT,
    create_time STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/ods/ods_sales_full'
    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

drop table if exists ods_traffic_detail;
CREATE external TABLE IF NOT EXISTS ods_traffic_detail (
    id INT,
    category_id STRING,
    category_name STRING,
    channel STRING,
    search_word STRING,
    visitor_id STRING,
    is_pay TINYINT,
    visit_time STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/ods/ods_traffic_detail'
    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

drop table if exists ods_user_behavior_full;
CREATE external TABLE IF NOT EXISTS ods_user_behavior_full (
    id INT,
    user_id STRING,
    category_id STRING,
    category_name STRING,
    behavior_type STRING,
    behavior_time STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/ods/ods_user_behavior_full'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );


drop table if exists ods_user_info;
CREATE external TABLE IF NOT EXISTS ods_user_info (
    user_id string,
    age INT,
    gender STRING,
    region STRING,
    consumption_level STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/work_ord/ods/ods_user_info'
    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );