# 1. 无线端入店与承接数据查询
-- 维度：店铺、入店页面类型、时间；指标：访客数、下单买家数
SELECT
    store_id,
    page_type,
    COUNT(DISTINCT visitor_id) AS uv,  -- 去重访客数
    COUNT(DISTINCT CASE WHEN is_buy = 1 THEN visitor_id END) AS buyer_num  -- 下单买家数（去重）
FROM ods_wireless_instore_log
WHERE dt BETWEEN ${start_date} AND ${end_date}
GROUP BY store_id, page_type
ORDER BY store_id, uv DESC;

# 2. 页面访问排行分析
-- 维度：店铺、页面大类/细分类型；指标：访客数、浏览量、平均停留时长
SELECT
    store_id,
    page_category,
    page_subtype,
    COUNT(DISTINCT visitor_id) AS page_uv,  -- 页面访客数（去重）
    COUNT(*) AS page_pv,  -- 页面浏览量（每条记录即1次浏览）
    round(AVG(stay_time),2) AS avg_stay_time  -- 平均停留时长
FROM ods_page_visit_log
WHERE dt BETWEEN ${start_date} AND ${end_date}
GROUP BY store_id, page_category, page_subtype
ORDER BY page_uv DESC;


# 3. 店内流转路径分析
-- 维度：店铺、来源页面、去向页面；指标：流转访客数、来源占比
SELECT
    store_id,
    source_page,
    target_page,
    COUNT(DISTINCT visitor_id) AS flow_uv,  -- 流转访客数（不查重，按文档需求保留重复跳转）
    COUNT(DISTINCT visitor_id) / SUM(COUNT(DISTINCT visitor_id)) OVER (PARTITION BY store_id) AS source_ratio
FROM ods_instore_flow_log
WHERE dt BETWEEN ${start_date} AND ${end_date}
GROUP BY store_id, source_page, target_page
ORDER BY store_id, flow_uv DESC;


# 4. PC 端流量入口分析
-- 维度：店铺、来源页面（TOP20）；指标：访客数、来源占比
SELECT
    store_id,
    source_page,
    COUNT(DISTINCT visitor_id) AS source_uv,  -- 来源页面访客数
    COUNT(DISTINCT visitor_id) / SUM(COUNT(DISTINCT visitor_id)) OVER (PARTITION BY store_id) AS source_uv_ratio
FROM ods_pc_entry_log
WHERE dt BETWEEN ${start_date} AND ${end_date}
GROUP BY store_id, source_page
ORDER BY source_uv DESC
LIMIT 20;