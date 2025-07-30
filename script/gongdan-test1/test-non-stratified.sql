-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 页面概览报表：按页面类型+日期统计核心指标
SELECT
  u.page_type,
  DATE(u.click_time) AS stat_date,
  COUNT(u.block_id) AS click_count, -- 页面总点击量（所有点击行为）
  COUNT(DISTINCT u.user_id) AS click_user_count, -- 去重点击人数
  -- 访客数：排除直播间、短视频、图文、微详情渠道
  COUNT(DISTINCT CASE WHEN u.channel NOT IN ('直播间','短视频','图文','微详情') THEN u.user_id END) AS visitor_count
FROM ods_user_behavior u
-- 仅统计文档要求的页面类型
WHERE u.page_type IN ('首页','自定义承接页','商品详情页')
  AND u.ds = ${pt} -- 添加分区条件（当前日期分区）
GROUP BY u.page_type, DATE(u.click_time)
ORDER BY stat_date, u.page_type;


-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 点击分布报表：按页面类型+板块统计点击与转化指标
SELECT
  u.page_type,
  u.block_id,
  s.block_name, -- 关联页面结构表获取板块名称
  COUNT(*) AS block_click_count, -- 板块总点击量
  COUNT(DISTINCT u.user_id) AS block_click_user_count, -- 板块去重点击人数
  -- 汇总该板块引导的支付金额（关联交易数据表）
  SUM(nullif(t.pay_amount, 0)) AS block_guide_pay_amount
FROM ods_user_behavior u
LEFT JOIN ods_page_structure s
  ON u.page_type = s.page_type AND u.block_id = s.block_id
  AND s.ds = ${pt} -- 页面结构表分区
LEFT JOIN ods_trade_data t
  ON u.block_id = t.guide_block_id AND u.user_id = t.user_id
   AND DATE(u.click_time) = DATE(t.pay_time) -- 时间匹配（点击与支付同一天）
   AND t.ds = ${pt} -- 交易数据表分区
WHERE u.page_type IN ('首页','自定义承接页','商品详情页')
  AND u.ds = ${pt} -- 用户行为表分区
GROUP BY u.page_type, u.block_id, s.block_name
ORDER BY u.page_type, block_click_count DESC;


-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 数据趋势报表：近30天页面每日访客与点击人数趋势
SELECT
  u.page_type,
  DATE(u.click_time) AS stat_date,
  -- 每日访客数（排除直播间等渠道）
  COUNT(DISTINCT CASE WHEN u.channel NOT IN ('直播间','短视频','图文','微详情') THEN u.user_id END) AS daily_visitor_count,
  -- 每日点击人数（所有渠道）
  COUNT(DISTINCT u.user_id) AS daily_click_user_count
FROM ods_user_behavior u
WHERE u.page_type IN ('首页','自定义承接页','商品详情页')
  AND u.ds >= date_format(date_add(to_date(from_unixtime(unix_timestamp(${pt}, 'yyyyMMdd'))), -29), 'yyyyMMdd')
  AND u.ds <= ${pt}
GROUP BY u.page_type, DATE(u.click_time)
ORDER BY stat_date, u.page_type;


-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 引导详情报表：页面引导至商品的点击与访客统计
SELECT
  u.page_type,
  u.block_id AS guide_product_id, -- 假设板块ID对应被引导商品ID（业务场景适配）
  COUNT(*) AS guide_click_count, -- 引导至商品的总点击量
  COUNT(DISTINCT u.user_id) AS guide_visitor_count -- 引导至商品的去重访客数
FROM ods_user_behavior u
-- 筛选有点击行为的记录（仅统计有效引导）
WHERE u.is_click = 1
  AND u.page_type IN ('首页','自定义承接页','商品详情页')
  AND u.ds = ${pt} -- 用户行为表分区
  -- 关联页面结构表，确保是商品相关板块（排除非商品板块）
  AND EXISTS (
    SELECT 1 FROM ods_page_structure s
    WHERE s.block_id = u.block_id
      AND s.block_name LIKE '%商品%'
      AND s.ds = ${pt} -- 页面结构表分区
  )
GROUP BY u.page_type, u.block_id
ORDER BY u.page_type, guide_click_count DESC;


-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 分布明细报表：模块点击量及占页面总点击量的比例
WITH page_total_click AS (
  -- 子查询：计算各页面的总点击量
  SELECT
    page_type,
    COUNT(*) AS total_click_count
  FROM ods_user_behavior
  WHERE page_type IN ('首页','自定义承接页','商品详情页')
    AND ds = ${pt} -- 子查询添加分区
  GROUP BY page_type
)
SELECT
  u.page_type,
  u.block_id AS module_id, -- 板块ID即模块ID
  s.block_name AS module_name, -- 模块名称（取自页面结构表）
  COUNT(*) AS module_click_count, -- 模块点击量
  ROUND(COUNT(*)/t.total_click_count * 100, 2) AS module_click_ratio_percent
FROM ods_user_behavior u
-- 关联页面结构表获取模块名称
LEFT JOIN ods_page_structure s
  ON u.block_id = s.block_id AND u.page_type = s.page_type
  AND s.ds = ${pt} -- 页面结构表分区
-- 关联总点击量子查询获取页面总点击量
JOIN page_total_click t
  ON u.page_type = t.page_type
WHERE u.page_type IN ('首页','自定义承接页','商品详情页')
  AND u.ds = ${pt} -- 用户行为表分区
GROUP BY u.page_type, u.block_id, s.block_name, t.total_click_count
ORDER BY u.page_type, module_click_count DESC;