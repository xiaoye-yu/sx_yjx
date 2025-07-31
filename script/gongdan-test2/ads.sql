# 1. 无线端入店与承接分析表
-- 工单编号：大数据-电商数仓-09-流量主题店内路径看板
DROP TABLE IF EXISTS ads_wireless_instore_analysis;
CREATE TABLE IF NOT EXISTS ads_wireless_instore_analysis (
    dt DATE NOT NULL COMMENT '日期',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    page_type VARCHAR(50) COMMENT '入店页面类型',
    total_uv BIGINT COMMENT '总访客数',
    total_buyer_num BIGINT COMMENT '总下单买家数',
    conversion_rate DECIMAL(10,4) COMMENT '转化率（total_buyer_num/total_uv）'
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `store_id`, `page_type`)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 8;

-- 数据导入
INSERT INTO ads_wireless_instore_analysis
SELECT
    dt,
    store_id,
    page_type,
    wireless_uv AS total_uv,
    wireless_buyer_num AS total_buyer_num,
    ROUND(wireless_buyer_num / NULLIF(wireless_uv, 0), 4) AS conversion_rate
FROM dws_wireless_pc_summary
WHERE page_type IS NOT NULL;

# 2. 页面访问排行表
-- 工单编号：大数据-电商数仓-09-流量主题店内路径看板
DROP TABLE IF EXISTS ads_page_visit_rank;
CREATE TABLE IF NOT EXISTS ads_page_visit_rank (
    dt DATE NOT NULL COMMENT '日期',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    page_category VARCHAR(50) NOT NULL COMMENT '页面大类',
    page_subtype VARCHAR(50) COMMENT '页面细分类型',
    page_uv BIGINT COMMENT '访客数（排序依据）',
    page_pv BIGINT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长（秒）',
    rank_num INT COMMENT '按访客数排序的名次'
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `store_id`, `page_category`)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 8;

-- 数据导入
INSERT INTO ads_page_visit_rank
SELECT
    dt,
    store_id,
    page_category,
    page_subtype,
    page_uv,
    page_pv,
    avg_stay_time,
    ROW_NUMBER() OVER (PARTITION BY dt, store_id ORDER BY page_uv DESC) AS rank_num
FROM dws_page_visit_summary;

# 3. 店内流转路径分析表
-- 工单编号：大数据-电商数仓-09-流量主题店内路径看板
DROP TABLE IF EXISTS ads_instore_flow_analysis;
CREATE TABLE IF NOT EXISTS ads_instore_flow_analysis (
    dt DATE NOT NULL COMMENT '日期',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    source_page VARCHAR(100) NOT NULL COMMENT '来源页面',
    target_page VARCHAR(100) NOT NULL COMMENT '去向页面',
    flow_uv BIGINT COMMENT '流转访客数',
    source_ratio DECIMAL(10,4) COMMENT '来源页面流转占比（flow_uv/total_store_flow）'
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `store_id`, `source_page`, `target_page`)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 8;

-- 数据导入
INSERT INTO ads_instore_flow_analysis
SELECT
    dt,
    store_id,
    source_page,
    target_page,
    flow_uv,
    ROUND(flow_uv / NULLIF(total_store_flow, 0), 4) AS source_ratio
FROM dws_instore_flow_summary;

# 4. PC 端流量入口分析表
-- 工单编号：大数据-电商数仓-09-流量主题店内路径看板
DROP TABLE IF EXISTS ads_pc_entry_analysis;
CREATE TABLE IF NOT EXISTS ads_pc_entry_analysis (
    dt DATE NOT NULL COMMENT '日期',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    source_page VARCHAR(100) NOT NULL COMMENT '来源页面',
    source_uv BIGINT COMMENT '来源访客数',
    source_ratio DECIMAL(10,4) COMMENT '来源占比（source_uv/总PC访客数）',
    top_rank INT COMMENT '按访客数排序的TOP名次（取前20）'
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `store_id`, `source_page`)
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 8;

-- 数据导入
INSERT INTO ads_pc_entry_analysis
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
WHERE top_rank <= 20;