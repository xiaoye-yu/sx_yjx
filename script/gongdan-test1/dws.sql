-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
CREATE EXTERNAL TABLE IF NOT EXISTS test.dws_page_stats (
  page_type STRING COMMENT '页面类型（首页/自定义承接页/商品详情页）',
  stat_date STRING COMMENT '统计日期',
  visitor_count BIGINT COMMENT '访客数（排除直播间等渠道）',
  click_count BIGINT COMMENT '总点击量',
  click_user_count BIGINT COMMENT '点击人数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/test/dws/dws_page_stats';

-- 数据导入
INSERT OVERWRITE TABLE test.dws_page_stats PARTITION (ds = ${pt})
SELECT
  page_type,
  DATE(click_time) AS stat_date,
  COUNT(DISTINCT CASE WHEN is_exclude_channel = 0 THEN user_id END) AS visitor_count,  -- 🔶1-16🔶
  COUNT(CASE WHEN is_click = 1 THEN 1 END) AS click_count,
  COUNT(DISTINCT CASE WHEN is_click = 1 THEN user_id END) AS click_user_count
FROM test.dwd_user_page_behavior_detail
WHERE ds = ${pt}
GROUP BY page_type, DATE(click_time);

-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
CREATE EXTERNAL TABLE IF NOT EXISTS test.dws_block_stats (
  page_type STRING,
  block_id STRING,
  block_name STRING,
  block_click_count BIGINT COMMENT '板块点击量',
  block_click_user_count BIGINT COMMENT '板块点击人数',
  guide_pay_amount DECIMAL(12,2) COMMENT '引导支付金额',
  page_total_click BIGINT COMMENT '页面总点击量（用于计算占比）'
)
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION '/warehouse/test/dws/dws_block_stats';

-- 数据导入
INSERT OVERWRITE TABLE test.dws_block_stats PARTITION (ds = ${pt})
SELECT
  d.page_type,
  d.block_id,
  d.block_name,
  COUNT(CASE WHEN d.is_click = 1 THEN 1 END) AS block_click_count,
  COUNT(DISTINCT CASE WHEN d.is_click = 1 THEN d.user_id END) AS block_click_user_count,
  SUM(d.pay_amount) AS guide_pay_amount,
  t.total_click AS page_total_click
FROM test.dwd_user_page_behavior_detail d
JOIN (
  SELECT page_type, COUNT(CASE WHEN is_click = 1 THEN 1 END) AS total_click
  FROM test.dwd_user_page_behavior_detail
  WHERE ds = ${pt}
  GROUP BY page_type
) t ON d.page_type = t.page_type
WHERE d.ds = ${pt}
GROUP BY d.page_type, d.block_id, d.block_name, t.total_click;


