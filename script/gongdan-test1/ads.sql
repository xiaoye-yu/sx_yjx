-- 1.页面概览指标
-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
CREATE EXTERNAL TABLE IF NOT EXISTS test.ads_page_overview_report (
  page_type STRING COMMENT '页面类型',
  stat_date STRING COMMENT '统计日期',
  click_count BIGINT COMMENT '页面总点击量（所有点击行为）',
  click_user_count BIGINT COMMENT '去重点击人数',
  visitor_count BIGINT COMMENT '访客数（排除直播间、短视频、图文、微详情渠道）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/test/ads/ads_page_overview_report';

-- 数据导入（源自DWS层页面维度表）
INSERT OVERWRITE TABLE test.ads_page_overview_report PARTITION (ds = ${pt})
SELECT
  page_type,
  stat_date,
  click_count,
  click_user_count,
  visitor_count
FROM test.dws_page_stats
WHERE ds = ${pt};


-- 2.点击分布指标
-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
CREATE EXTERNAL TABLE IF NOT EXISTS test.ads_block_click_report (
  page_type STRING COMMENT '页面类型',
  block_id STRING COMMENT '板块ID',
  block_name STRING COMMENT '板块名称',
  block_click_count BIGINT COMMENT '板块总点击量',
  block_click_user_count BIGINT COMMENT '板块去重点击人数',
  block_guide_pay_amount DECIMAL(10,2) COMMENT '板块引导支付金额'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/test/ads/ads_block_click_report';

-- 数据导入（源自DWS层板块维度表）
INSERT OVERWRITE TABLE test.ads_block_click_report PARTITION (ds = ${pt})
SELECT
  page_type,
  block_id,
  block_name,
  block_click_count,
  block_click_user_count,
  guide_pay_amount AS block_guide_pay_amount
FROM test.dws_block_stats
WHERE ds = ${pt};

-- 3.数据趋势指标
-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
CREATE EXTERNAL TABLE IF NOT EXISTS test.ads_page_trend_report (
  page_type STRING COMMENT '页面类型',
  stat_date STRING COMMENT '统计日期',
  daily_visitor_count BIGINT COMMENT '每日访客数（排除直播间等渠道）',
  daily_click_user_count BIGINT COMMENT '每日点击人数（所有渠道）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/test/ads/ads_page_trend_report';

-- 数据导入（源自DWS层页面维度表的近30天数据）
INSERT OVERWRITE TABLE test.ads_page_trend_report PARTITION (ds = ${pt})
SELECT
  page_type,
  stat_date,
  visitor_count,
  click_user_count
FROM test.dws_page_stats
WHERE ds >= date_format(date_add(to_date(from_unixtime(unix_timestamp(${pt}, 'yyyyMMdd'))), -29), 'yyyyMMdd')
  AND ds <= ${pt};

-- 4、引导详情报表

-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
CREATE EXTERNAL TABLE IF NOT EXISTS test.ads_guide_detail_report (
  page_type STRING COMMENT '页面类型',
  guide_product_id STRING COMMENT '引导商品ID（板块ID）',
  guide_click_count BIGINT COMMENT '引导至商品的总点击量',
  guide_visitor_count BIGINT COMMENT '引导至商品的去重访客数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/test/ads/ads_guide_detail_report';

-- 数据导入（源自DWS层板块维度表的商品相关板块）
INSERT OVERWRITE TABLE test.ads_guide_detail_report PARTITION (ds = ${pt})
SELECT
  page_type,
  block_id AS guide_product_id,
  block_click_count,
  block_click_user_count
FROM test.dws_block_stats
WHERE ds = ${pt}
  AND block_name LIKE '%商品%';


-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
CREATE EXTERNAL TABLE IF NOT EXISTS test.ads_module_detail_report (
  page_type STRING COMMENT '页面类型',
  module_id STRING COMMENT '模块ID（板块ID）',
  module_name STRING COMMENT '模块名称（板块名称）',
  module_click_count BIGINT COMMENT '模块点击量',
  module_click_ratio_percent DECIMAL(5,2) COMMENT '模块点击占比（%）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/test/ads/ads_module_detail_report';

-- 数据导入（修正占比计算逻辑及分区参数格式）
INSERT OVERWRITE TABLE test.ads_module_detail_report PARTITION (ds = ${pt})
SELECT
  page_type,
  block_id AS module_id,
  block_name AS module_name,
  block_click_count AS module_click_count,
  ROUND((block_click_count / page_total_click) * 100, 2) AS module_click_ratio_percent
FROM test.dws_block_stats
WHERE ds = ${pt};