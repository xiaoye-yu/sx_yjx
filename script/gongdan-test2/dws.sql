# 1. 无线端 + PC 端核心指标汇总表
-- 工单编号：大数据-电商数仓-09-流量主题店内路径看板
DROP TABLE IF EXISTS dws_wireless_pc_summary;
CREATE TABLE IF NOT EXISTS dws_wireless_pc_summary (
    dt DATE NOT NULL COMMENT '日期',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    page_type VARCHAR(50) COMMENT '无线端入店页面类型',
    source_page VARCHAR(100) COMMENT 'PC端来源页面',
    wireless_uv BIGINT COMMENT '无线端访客数（去重）',
    wireless_buyer_num BIGINT COMMENT '无线端下单买家数（去重）',
    pc_uv BIGINT COMMENT 'PC端访客数（去重）'
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `store_id`)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 16;

-- 数据导入
INSERT INTO dws_wireless_pc_summary
-- 无线端指标
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
-- PC端指标
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
GROUP BY dt, store_id, source_page;

# 2. 页面访问指标汇总表
DROP TABLE IF EXISTS dws_page_visit_summary;
CREATE TABLE IF NOT EXISTS dws_page_visit_summary (
    dt DATE NOT NULL COMMENT '日期',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    page_category VARCHAR(50) NOT NULL COMMENT '页面大类',
    page_subtype VARCHAR(50) COMMENT '页面细分类型',
    page_uv BIGINT COMMENT '页面访客数（去重）',
    page_pv BIGINT COMMENT '页面浏览量（不去重）',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长（秒）'
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `store_id`, `page_category`)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 16;

-- 数据导入
INSERT INTO dws_page_visit_summary
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
GROUP BY dt, store_id, page_category, page_subtype;

# 3. 店内流转指标汇总表
DROP TABLE IF EXISTS dws_instore_flow_summary;
CREATE TABLE IF NOT EXISTS dws_instore_flow_summary (
    dt DATE NOT NULL COMMENT '日期',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    source_page VARCHAR(100) NOT NULL COMMENT '来源页面',
    target_page VARCHAR(100) NOT NULL COMMENT '去向页面',
    flow_uv BIGINT COMMENT '流转访客数（不去重，同一访客可多次跳转）',
    total_store_flow BIGINT COMMENT '店铺总流转访客数（用于计算占比）'
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `store_id`, `source_page`, `target_page`)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 16;

-- 数据导入
INSERT INTO dws_instore_flow_summary
SELECT
    dt,
    store_id,
    source_page,
    target_page,
    COUNT(visitor_id) AS flow_uv,
    SUM(COUNT(visitor_id)) OVER (PARTITION BY dt, store_id) AS total_store_flow
FROM dwd_traffic_unified_detail
WHERE data_type = 'instore_flow'
GROUP BY dt, store_id, source_page, target_page;