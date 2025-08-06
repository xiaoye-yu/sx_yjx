-- 1. 全品类商品销售排行（ads_product_sales_ranking）
drop table if exists ads_product_sales_ranking;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_product_sales_ranking (
  product_name STRING COMMENT '商品名称',
  product_category STRING COMMENT '商品分类',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  sales_amount DECIMAL(12,2) COMMENT '销售额',
  sales_quantity INT COMMENT '销量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ads/ads_product_sales_ranking';


INSERT OVERWRITE TABLE gmall_ins.ads_product_sales_ranking PARTITION (ds = ${pt})
SELECT
  product_name,
  product_category,
  time_type,
  sum(total_sales_amount) AS sales_amount,
  sum(total_sales_quantity) AS sales_quantity
FROM gmall_ins.dws_product_sales
WHERE ds = ${pt}
group by product_name,product_category,time_type;

-- 2. 流量来源 TOP10（ads_traffic_source_top10）
drop table if exists ads_traffic_source_top10;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_traffic_source_top10 (
  source_name STRING COMMENT '流量来源渠道',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  visitor_count INT COMMENT '访客数',
  pay_conversion_rate DECIMAL(6,4) COMMENT '支付转化率'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ads/ads_traffic_source_top10';

INSERT OVERWRITE TABLE gmall_ins.ads_traffic_source_top10 PARTITION (ds = ${pt})
SELECT
  source_name,
  time_type,
  visitor_count,
  pay_conversion_rate
FROM (
  SELECT
    source_name,
    time_type,
    total_visitor_count AS visitor_count,
    total_pay_conversion_rate AS pay_conversion_rate,
    ROW_NUMBER() OVER (PARTITION BY time_type ORDER BY total_visitor_count DESC) AS rn
  FROM gmall_ins.dws_traffic_source
  WHERE ds = ${pt}
) t
WHERE rn <= 10;

-- 3. 热销 SKU TOP5（ads_sku_sales_top5）
drop table if exists ads_sku_sales_top5;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_sku_sales_top5 (
  sku_info STRING COMMENT 'SKU信息',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  pay_quantity INT COMMENT '支付件数',
  pay_quantity_ratio DECIMAL(6,4) COMMENT '支付件数占比',
  current_stock INT COMMENT '当前库存',
  stock_available_days DECIMAL(6,2) COMMENT '库存可售天数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ads/ads_sku_sales_top5';

INSERT OVERWRITE TABLE gmall_ins.ads_sku_sales_top5 PARTITION (ds = ${pt})
SELECT
  sku_info,
  time_type,
  pay_quantity,
  pay_quantity_ratio,
  current_stock,
  stock_available_days
FROM (
  SELECT
    sku_info,
    time_type,
    total_pay_quantity AS pay_quantity,
    total_pay_quantity / SUM(total_pay_quantity) OVER (PARTITION BY time_type) AS pay_quantity_ratio,
    current_stock,
    stock_available_days,
    ROW_NUMBER() OVER (PARTITION BY time_type ORDER BY total_pay_quantity DESC) AS rn
  FROM gmall_ins.dws_sku_sales
  WHERE ds = ${pt}
) t
WHERE rn <= 5;

-- 4. 热门搜索词 TOP10 报表（ads_search_word_top10）
drop table if exists ads_search_word_top10;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_search_word_top10 (
  search_word STRING COMMENT '搜索词',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m/自定义）',
  visitor_count INT COMMENT '访客数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ads/ads_search_word_top10';

INSERT OVERWRITE TABLE gmall_ins.ads_search_word_top10 PARTITION (ds = ${pt})
SELECT
  search_word,
  time_type,
  visitor_count
FROM (
  SELECT
    search_word,
    time_type,
    total_visitor_count AS visitor_count,
    ROW_NUMBER() OVER (PARTITION BY time_type ORDER BY total_visitor_count DESC) AS rn
  FROM gmall_ins.dws_search_word
  WHERE ds = ${pt}
) t
WHERE rn <= 10;

-- 5. 价格力商品概览（ads_price_strength_overview）
drop table if exists ads_price_strength_overview;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_price_strength_overview (
  price_strength_level STRING COMMENT '价格力等级',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m）',
  product_count INT COMMENT '商品数量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ads/ads_price_strength_overview';

INSERT OVERWRITE TABLE gmall_ins.ads_price_strength_overview PARTITION (ds = ${pt})
SELECT
  price_strength_level,
  time_type,
  SUM(product_count) AS product_count
FROM gmall_ins.dws_price_strength
WHERE ds = ${pt}
GROUP BY price_strength_level, time_type;

-- 6. 商品预警清单（ads_product_warning_list）
drop table if exists ads_product_warning_list;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ads_product_warning_list (
  product_name STRING COMMENT '商品名称',
  warning_type STRING COMMENT '预警类型',
  time_type STRING COMMENT '时间粒度（7d/30d/d/w/m）',
  warning_reason STRING COMMENT '预警原因'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ads/ads_product_warning_list';

INSERT OVERWRITE TABLE gmall_ins.ads_product_warning_list PARTITION (ds = ${pt})
SELECT
  product_name,
  warning_type,
  time_type,
  warning_reason
FROM gmall_ins.dws_product_warning
WHERE ds = ${pt};