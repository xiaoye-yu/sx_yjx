-- 清洗后流量统一明细表
DROP TABLE IF EXISTS test_yi.dwd_traffic_unified_detail;
CREATE TABLE IF NOT EXISTS dwd_traffic_unified_detail (
    data_type VARCHAR(20) NOT NULL COMMENT '数据类型：wireless_instore/page_visit/instore_flow/pc_entry',
    visitor_id VARCHAR(100) COMMENT '访客唯一标识',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    page_type VARCHAR(50) COMMENT '入店页面类型（仅wireless_instore有值）',
    page_category VARCHAR(50) COMMENT '页面大类（仅page_visit有值）',
    page_subtype VARCHAR(50) COMMENT '页面细分类型（仅page_visit有值）',
    source_page VARCHAR(100) COMMENT '来源页面（instore_flow/pc_entry有值）',
    target_page VARCHAR(100) COMMENT '去向页面（仅instore_flow有值）',
    visit_time DATETIME NOT NULL COMMENT '访问/跳转时间',
    is_buy TINYINT COMMENT '是否下单（仅wireless_instore有值，0=未下单，1=下单）',
    stay_time INT COMMENT '停留时长（秒，仅page_visit有值，过滤<0的异常值）',
    dt DATE NOT NULL COMMENT '日期分区'
) ENGINE=OLAP
DUPLICATE KEY(data_type, visitor_id, store_id)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD"
);

-- 数据导入：从ODS层同步并清洗
INSERT INTO dwd_traffic_unified_detail
-- 1. 无线端入店数据
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
WHERE visit_time IS NOT NULL  -- 过滤无效时间
UNION ALL
-- 2. 页面访问数据
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
WHERE stay_time > 0  -- 过滤异常停留时长
UNION ALL
-- 3. 店内流转数据
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
WHERE source_page IS NOT NULL AND target_page IS NOT NULL  -- 过滤无效页面
UNION ALL
-- 4. PC端入口数据
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
FROM ods_pc_entry_log;