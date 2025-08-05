-- 1. 商品销售明细事实表（dwd_product_sales_daily）
drop table dwd_product_sales_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dwd_product_sales_daily (
  product_id STRING COMMENT '商品ID',
  product_name STRING COMMENT '商品名称（关联ods_product_base）',
  product_category STRING COMMENT '商品分类（关联ods_product_base）',
  brand STRING COMMENT '品牌（如轩妈家）',
  shop_id STRING COMMENT '所属店铺ID',
  stat_date DATE COMMENT '统计日期',
  sales_amount DECIMAL(12,2) COMMENT '当日销售额（清洗后）',
  sales_quantity INT COMMENT '当日销量（清洗后）',
  visitor_count INT COMMENT '商品访客数（详情页访问）',
  pay_buyer_count INT COMMENT '支付买家数',
  pay_conversion_rate DECIMAL(6,4) COMMENT '支付转化率=支付买家数/访客数',
  add_cart_count INT COMMENT '加购次数',
  collect_count INT COMMENT '收藏次数',
  refund_count INT COMMENT '退款件数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dwd/dwd_product_sales_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '商品每日销售明细，补充商品名称、分类等信息，支撑销售排行分析'
);


INSERT OVERWRITE TABLE gmall_ins.dwd_product_sales_daily PARTITION (ds = ${bizdate})
SELECT
  s.product_id,
  p.product_name,
  p.product_category,
  p.brand,
  p.shop_id,
  s.stat_date,
  CASE WHEN s.sales_amount < 0 THEN 0 ELSE s.sales_amount END AS sales_amount,
  CASE WHEN s.sales_quantity < 0 THEN 0 ELSE s.sales_quantity END AS sales_quantity,
  s.visitor_count,
  s.pay_buyer_count,
  CASE WHEN s.visitor_count = 0 THEN 0 ELSE s.pay_buyer_count / s.visitor_count END AS pay_conversion_rate,
  s.add_cart_count,
  s.collect_count,
  s.refund_count
FROM gmall_ins.ods_product_sales_daily s
-- 关联商品基础表补充名称、分类等维度
LEFT JOIN gmall_ins.ods_product_base p
  ON s.product_id = p.product_id
WHERE s.ds = ${bizdate};


-- 2. 流量来源明细事实表（dwd_traffic_source_daily）
drop table if exists dwd_traffic_source_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dwd_traffic_source_daily (
  product_id STRING COMMENT '商品ID',
  product_name STRING COMMENT '商品名称（关联ods_product_base）',
  source_name STRING COMMENT '流量来源（如效果广告、手淘搜索）',
  stat_date DATE COMMENT '统计日期',
  visitor_count INT COMMENT '该来源访客数',
  pay_buyer_count INT COMMENT '该来源支付买家数',
  pay_conversion_rate DECIMAL(6,4) COMMENT '该来源支付转化率',
  click_count INT COMMENT '该来源点击数',
  stay_duration INT COMMENT '平均停留时长（秒）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dwd/dwd_traffic_source_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '流量来源每日明细，补充商品名称，支撑流量来源TOP10分析'
);

INSERT OVERWRITE TABLE gmall_ins.dwd_traffic_source_daily PARTITION (ds = ${bizdate})
SELECT
  t.product_id,
  p.product_name,
  t.source_name,
  t.stat_date,
  t.visitor_count,
  t.pay_buyer_count,
  CASE WHEN t.visitor_count = 0 THEN 0 ELSE t.pay_buyer_count / t.visitor_count END AS pay_conversion_rate,
  t.click_count,
  t.stay_duration
FROM gmall_ins.ods_traffic_source_daily t
LEFT JOIN gmall_ins.ods_product_base p
  ON t.product_id = p.product_id AND p.ds = ${bizdate}
WHERE t.ds = ${bizdate};


-- 3. SKU 销售及库存明细事实表（dwd_sku_sales_inventory_daily）
drop table if exists dwd_sku_sales_inventory_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dwd_sku_sales_inventory_daily (
  sku_id STRING COMMENT 'SKU ID',
  product_id STRING COMMENT '所属商品ID',
  product_name STRING COMMENT '商品名称（关联ods_product_base）',
  sku_info STRING COMMENT 'SKU信息（如颜色、规格）',
  stat_date DATE COMMENT '统计日期',
  pay_quantity INT COMMENT '当日支付件数',
  sales_amount DECIMAL(10,2) COMMENT '当日销售额',
  current_stock INT COMMENT '当日库存（件，清洗后）',
  stock_in INT COMMENT '当日入库（件）',
  stock_out INT COMMENT '当日出库（件）',
  avg_daily_sales INT COMMENT '近7天日均销量（用于计算可售天数）',
  stock_available_days DECIMAL(6,2) COMMENT '库存可售天数=当前库存/日均销量'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dwd/dwd_sku_sales_inventory_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = 'SKU销售及库存明细，补充商品名称和库存可售天数，支撑SKU TOP5分析'
);

INSERT OVERWRITE TABLE gmall_ins.dwd_sku_sales_inventory_daily PARTITION (ds = ${bizdate})
SELECT
  s.sku_id,
  s.product_id,
  p.product_name,
  s.sku_info,
  s.stat_date,
  s.pay_quantity,
  s.sales_amount,
  CASE WHEN s.current_stock < 0 THEN 0 ELSE s.current_stock END AS current_stock,  -- 清洗负库存
  s.stock_in,
  s.stock_out,
  AVG(ss.pay_quantity) OVER (PARTITION BY s.sku_id ORDER BY s.stat_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_daily_sales,
  CASE
    WHEN AVG(ss.pay_quantity) OVER (PARTITION BY s.sku_id ORDER BY s.stat_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) = 0 THEN 0
    ELSE CASE WHEN s.current_stock < 0 THEN 0 ELSE s.current_stock END / AVG(ss.pay_quantity) OVER (PARTITION BY s.sku_id ORDER BY s.stat_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
  END AS stock_available_days
FROM gmall_ins.ods_sku_sales_inventory_daily s
LEFT JOIN gmall_ins.ods_product_base p
  ON s.product_id = p.product_id AND p.ds = ${bizdate}
LEFT JOIN gmall_ins.ods_sku_sales_inventory_daily ss
  ON s.sku_id = ss.sku_id AND ss.stat_date BETWEEN DATE_SUB(s.stat_date, 6) AND s.stat_date
WHERE s.ds = ${bizdate};

-- 4. 搜索词明细事实表（dwd_search_word_daily）
drop table if exists dwd_search_word_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dwd_search_word_daily (
  search_word STRING COMMENT '搜索词（如轩妈家、蛋黄酥）',
  product_id STRING COMMENT '被搜索的商品ID',
  product_name STRING COMMENT '商品名称（关联ods_product_base）',
  stat_date DATE COMMENT '统计日期',
  search_count INT COMMENT '搜索次数',
  click_count INT COMMENT '点击次数',
  visitor_count INT COMMENT '搜索带来的访客数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dwd/dwd_search_word_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '搜索词每日明细，补充商品名称，支撑搜索词TOP10分析'
);

INSERT OVERWRITE TABLE gmall_ins.dwd_search_word_daily PARTITION (ds = ${bizdate})
SELECT
  s.search_word,
  s.product_id,
  p.product_name,
  s.stat_date,
  s.search_count,
  s.click_count,
  s.visitor_count
FROM gmall_ins.ods_search_word_daily s
LEFT JOIN gmall_ins.ods_product_base p
  ON s.product_id = p.product_id AND p.ds = ${bizdate}
WHERE s.ds = ${bizdate};


-- 5. 价格力商品明细事实表（dwd_price_strength_daily）
drop table if exists dwd_price_strength_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dwd_price_strength_daily (
  product_id STRING COMMENT '商品ID',
  product_name STRING COMMENT '商品名称（关联ods_product_base）',
  product_category STRING COMMENT '商品分类（关联ods_product_base）',
  price_strength_level STRING COMMENT '价格力等级（优秀/良好/较差）',
  coupon_price DECIMAL(10,2) COMMENT '普惠券后价',
  price_band STRING COMMENT '价格带（如0-50元）',
  same_category_avg_price DECIMAL(10,2) COMMENT '同类目均价',
  update_time TIMESTAMP COMMENT '等级更新时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dwd/dwd_price_strength_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '价格力商品每日明细，补充商品名称和分类，支撑价格力分析'
);

INSERT OVERWRITE TABLE gmall_ins.dwd_price_strength_daily PARTITION (ds = ${bizdate})
SELECT
  p.product_id,
  b.product_name,
  b.product_category,
  p.price_strength_level,
  p.coupon_price,
  p.price_band,
  p.same_category_avg_price,
  p.update_time
FROM gmall_ins.ods_price_strength_product p
LEFT JOIN gmall_ins.ods_product_base b
  ON p.product_id = b.product_id AND b.ds = ${bizdate}
WHERE p.ds = ${bizdate};

-- 6. 商品预警明细事实表（dwd_product_warning_daily）
drop table if exists dwd_product_warning_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.dwd_product_warning_daily (
  product_id STRING COMMENT '商品ID',
  product_name STRING COMMENT '商品名称（关联ods_product_base）',
  stat_date DATE COMMENT '预警日期',
  warning_type STRING COMMENT '预警类型（价格力预警/商品力预警）',
  warning_reason STRING COMMENT '预警原因（如持续低星、转化率低于市场平均）',
  warning_level STRING COMMENT '预警等级（一般/严重）',
  handle_status STRING COMMENT '处理状态（未处理/已处理）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/dwd/dwd_product_warning_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '商品预警每日明细，补充商品名称，支撑预警清单分析'
);

INSERT OVERWRITE TABLE gmall_ins.dwd_product_warning_daily PARTITION (ds = ${bizdate})
SELECT
  w.product_id,
  b.product_name,
  TO_DATE(w.stat_date) AS stat_date,
  w.warning_type,
  w.warning_reason,
  w.warning_level,
  w.handle_status
FROM gmall_ins.ods_product_warning_daily w
LEFT JOIN gmall_ins.ods_product_base b
  ON w.product_id = b.product_id AND b.ds = ${bizdate}
WHERE w.ds = ${bizdate};