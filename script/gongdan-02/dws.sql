-- 1. 商品销售汇总表（dws_product_sales）
drop table if exists dws_product_sales;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_product_sales (
  product_name STRING COMMENT '商品名称',
  product_category STRING COMMENT '商品分类',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_sales_amount DECIMAL(12,2) COMMENT '总销售额',
  total_sales_quantity INT COMMENT '总销量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dws/dws_product_sales'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '商品销售多粒度汇总');

INSERT OVERWRITE TABLE gmall_ins.dws_product_sales PARTITION (ds = ${pt})
SELECT
   product_name, product_category, 'd' AS time_type,
  stat_date AS start_date, stat_date AS end_date,
  SUM(sales_amount) AS total_sales_amount, SUM(sales_quantity) AS total_sales_quantity
FROM gmall_ins.dwd_product_sales_daily
GROUP BY  product_name, product_category, stat_date
-- 7天
UNION ALL
SELECT
   product_name, product_category, '7d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 6) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(sales_amount) AS total_sales_amount, SUM(sales_quantity) AS total_sales_quantity
FROM gmall_ins.dwd_product_sales_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 6) AND TO_DATE(${bizdate})
GROUP BY  product_name, product_category
-- 30天
UNION ALL
SELECT
  product_name, product_category, '30d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 29) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(sales_amount) AS total_sales_amount, SUM(sales_quantity) AS total_sales_quantity
FROM gmall_ins.dwd_product_sales_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 29) AND TO_DATE(${bizdate})
GROUP BY product_name, product_category;


-- 2. 流量来源汇总表（dws_traffic_source）
drop table if exists dws_traffic_source;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_traffic_source (
  source_name STRING COMMENT '流量来源渠道',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_visitor_count INT COMMENT '总访客数',
  total_pay_conversion_rate DECIMAL(6,4) COMMENT '总支付转化率'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dws/dws_traffic_source'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '流量来源多粒度汇总');

INSERT OVERWRITE TABLE gmall_ins.dws_traffic_source PARTITION (ds = ${pt})
SELECT
  source_name, 'd' AS time_type,
  stat_date AS start_date, stat_date AS end_date,
  SUM(visitor_count) AS total_visitor_count,
  SUM(pay_buyer_count)/SUM(visitor_count) AS total_pay_conversion_rate
FROM gmall_ins.dwd_traffic_source_daily
GROUP BY source_name,stat_date
union
SELECT
  source_name, '7d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 6) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(visitor_count) AS total_visitor_count,
  SUM(pay_buyer_count)/SUM(visitor_count) AS total_pay_conversion_rate
FROM gmall_ins.dwd_traffic_source_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 6) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 6), 'yyyyMMdd') AND ${pt}
GROUP BY source_name
union
SELECT
  source_name, '30d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 29) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(visitor_count) AS total_visitor_count,
  SUM(pay_buyer_count)/SUM(visitor_count) AS total_pay_conversion_rate
FROM gmall_ins.dwd_traffic_source_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 29) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 29), 'yyyyMMdd') AND ${pt}
GROUP BY source_name;

-- 3. SKU 销售汇总表（dws_sku_sales）
drop table if exists dws_sku_sales;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_sku_sales (
  sku_id STRING COMMENT 'SKU ID',
  product_name STRING COMMENT '商品名称',
  sku_info STRING COMMENT 'SKU信息',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_pay_quantity INT COMMENT '总支付件数',
  current_stock INT COMMENT '当前库存',
  stock_available_days DECIMAL(6,2) COMMENT '库存可售天数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dws/dws_sku_sales'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = 'SKU销售多粒度汇总');

INSERT OVERWRITE TABLE gmall_ins.dws_sku_sales PARTITION (ds = ${pt})
SELECT
  sku_id, product_name, sku_info, 'd' AS time_type,
  stat_date   AS start_date, stat_date AS end_date,
  SUM(pay_quantity) AS total_pay_quantity,
  MAX(current_stock) AS current_stock,  -- 取结束日库存
  AVG(stock_available_days) AS stock_available_days
FROM gmall_ins.dwd_sku_sales_inventory_daily
GROUP BY sku_id, product_name, sku_info,stat_date
union
SELECT
  sku_id, product_name, sku_info, '7d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 6) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(pay_quantity) AS total_pay_quantity,
  MAX(current_stock) AS current_stock,  -- 取结束日库存
  AVG(stock_available_days) AS stock_available_days
FROM gmall_ins.dwd_sku_sales_inventory_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 6) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 6), 'yyyyMMdd') AND ${pt}
GROUP BY sku_id, product_name, sku_info
union
SELECT
  sku_id, product_name, sku_info, '30d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 29) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(pay_quantity) AS total_pay_quantity,
  MAX(current_stock) AS current_stock,  -- 取结束日库存
  AVG(stock_available_days) AS stock_available_days
FROM gmall_ins.dwd_sku_sales_inventory_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 29) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 29), 'yyyyMMdd') AND ${pt}
GROUP BY sku_id, product_name, sku_info;

-- 4. 搜索词汇总表（dws_search_word）
drop table if exists dws_search_word;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_search_word (
  search_word STRING COMMENT '搜索词',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  total_visitor_count INT COMMENT '总访客数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dws/dws_search_word'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '搜索词多粒度汇总');


INSERT OVERWRITE TABLE gmall_ins.dws_search_word PARTITION (ds = ${pt})
SELECT
  search_word, 'd' AS time_type,
 stat_date AS start_date, stat_date AS end_date,
 SUM(visitor_count) AS total_visitor_count
FROM gmall_ins.dwd_search_word_daily
GROUP BY search_word,stat_date
union
SELECT
  search_word, '7d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 6) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(visitor_count) AS total_visitor_count
FROM gmall_ins.dwd_search_word_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 6) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 6), 'yyyyMMdd') AND ${pt}
GROUP BY search_word
union
SELECT
  search_word, '30d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 29) AS start_date, TO_DATE(${bizdate}) AS end_date,
  SUM(visitor_count) AS total_visitor_count
FROM gmall_ins.dwd_search_word_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 29) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 29), 'yyyyMMdd') AND ${pt}
GROUP BY search_word;

-- 5. 价格力商品汇总表（dws_price_strength）
drop table if exists dws_price_strength;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_price_strength (
  price_strength_level STRING COMMENT '价格力等级（优秀/良好/较差）',
  product_category STRING COMMENT '商品分类',
  time_type STRING COMMENT '时间粒度（d/7d/30d）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日',
  product_count INT COMMENT '商品数量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dws/dws_price_strength'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '价格力商品多粒度汇总');

INSERT OVERWRITE TABLE gmall_ins.dws_price_strength PARTITION (ds = ${pt})
SELECT
  price_strength_level, product_category, 'd' AS time_type,
  TO_DATE(${bizdate}) AS start_date, TO_DATE(${bizdate}) AS end_date,
  COUNT(DISTINCT product_id) AS product_count
FROM gmall_ins.dwd_price_strength_daily
GROUP BY price_strength_level, product_category
union
SELECT
  price_strength_level, product_category, '7d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 6) AS start_date, TO_DATE(${bizdate}) AS end_date,
  COUNT(DISTINCT product_id) AS product_count
FROM gmall_ins.dwd_price_strength_daily
WHERE ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 6), 'yyyyMMdd') AND ${pt}
GROUP BY price_strength_level, product_category
union
SELECT
  price_strength_level, product_category, '30d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 29) AS start_date, TO_DATE(${bizdate}) AS end_date,
  COUNT(DISTINCT product_id) AS product_count
FROM gmall_ins.dwd_price_strength_daily
WHERE ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 29), 'yyyyMMdd') AND ${pt}
GROUP BY price_strength_level, product_category;

-- 6. 商品预警汇总表（dws_product_warning）
drop table if exists dws_product_warning;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dws_product_warning (
  product_name STRING COMMENT '商品名称',
  warning_type STRING COMMENT '预警类型（价格力/商品力）',
  warning_reason STRING COMMENT '预警原因',
  time_type STRING COMMENT '时间粒度（d/7d/30d/w/m）',
  start_date DATE COMMENT '时间范围起始日',
  end_date DATE COMMENT '时间范围结束日'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dws/dws_product_warning'
TBLPROPERTIES ('orc.compress' = 'SNAPPY', 'comment' = '商品预警多粒度汇总');

INSERT OVERWRITE TABLE gmall_ins.dws_product_warning PARTITION (ds = ${pt})
SELECT
  product_name, warning_type, warning_reason, 'd' AS time_type,
  TO_DATE(${bizdate}) AS start_date, TO_DATE(${bizdate}) AS end_date
FROM gmall_ins.dwd_product_warning_daily
union
SELECT
  product_name, warning_type, warning_reason, '7d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 6) AS start_date, TO_DATE(${bizdate}) AS end_date
FROM gmall_ins.dwd_product_warning_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 6) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 6), 'yyyyMMdd') AND ${pt}
union
SELECT
  product_name, warning_type, warning_reason, '30d' AS time_type,
  DATE_SUB(TO_DATE(${bizdate}), 29) AS start_date, TO_DATE(${bizdate}) AS end_date
FROM gmall_ins.dwd_product_warning_daily
WHERE stat_date BETWEEN DATE_SUB(TO_DATE(${bizdate}), 29) AND TO_DATE(${bizdate})
  AND ds BETWEEN DATE_FORMAT(DATE_SUB(TO_DATE(${bizdate}), 29), 'yyyyMMdd') AND ${pt};