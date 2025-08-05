use gmall_ins;
-- 1. 商品基础信息表（gmall_ins.ods_product_base）
drop table if exists ods_product_base;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_product_base (
  product_id STRING COMMENT '商品ID',
  product_name STRING COMMENT '商品名称',
  product_category STRING COMMENT '商品分类（如零食、手机）',
  brand STRING COMMENT '品牌（如轩妈家）',
  shop_id STRING COMMENT '所属店铺ID',
  launch_date STRING COMMENT '上架日期',
  original_price DECIMAL(10,2) COMMENT '原价',
  status STRING COMMENT '商品状态（在售/下架）',
  create_time TIMESTAMP COMMENT '记录创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_product_base'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '商品基础信息ODS层（存储商品固有属性，支撑全量商品分析）'
);

-- 2. 商品每日销售快照表（gmall_ins.ods_product_sales_daily）
drop table if exists ods_product_sales_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_product_sales_daily (
  product_id STRING COMMENT '商品ID',
  stat_date DATE COMMENT '统计日期',
  sales_amount DECIMAL(12,2) COMMENT '当日销售额',
  sales_quantity INT COMMENT '当日销量',
  visitor_count INT COMMENT '商品访客数（详情页访问）',
  pay_buyer_count INT COMMENT '支付买家数',
  pay_conversion_rate DECIMAL(6,4) COMMENT '支付转化率=支付买家数/访客数🔶2-25🔶',
  add_cart_count INT COMMENT '加购次数',
  collect_count INT COMMENT '收藏次数',
  refund_count INT COMMENT '退款件数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_product_sales_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '商品每日销售快照ODS层（支撑销售额、销量排行分析）'
);

-- 3. 流量来源明细数据表（gmall_ins.ods_traffic_source_daily）
drop table if exists ods_traffic_source_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_traffic_source_daily (
  product_id STRING COMMENT '商品ID',
  stat_date DATE COMMENT '统计日期',
  source_name STRING COMMENT '流量来源（如效果广告、手淘搜索等🔶2-41至51🔶）',
  visitor_count INT COMMENT '该来源访客数',
  pay_buyer_count INT COMMENT '该来源支付买家数',
  click_count INT COMMENT '该来源点击数',
  stay_duration INT COMMENT '平均停留时长（秒）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_traffic_source_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '流量来源明细ODS层（支撑TOP10流量来源分析）'
);

-- 4. SKU 销售及库存表（gmall_ins.ods_sku_sales_inventory_daily）
drop table if exists ods_sku_sales_inventory_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_sku_sales_inventory_daily (
  sku_id STRING COMMENT 'SKU ID',
  product_id STRING COMMENT '所属商品ID',
  sku_info STRING COMMENT 'SKU信息（如颜色、规格）',
  stat_date DATE COMMENT '统计日期',
  pay_quantity INT COMMENT '当日支付件数',
  sales_amount DECIMAL(10,2) COMMENT '当日销售额',
  current_stock INT COMMENT '当日库存（件）',
  stock_in INT COMMENT '当日入库（件）',
  stock_out INT COMMENT '当日出库（件）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_sku_sales_inventory_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = 'SKU销售及库存ODS层（支撑TOP5 SKU分析🔶2-56🔶）'
);

-- 5. 搜索词数据表（gmall_ins.ods_search_word_daily）
drop table if exists ods_search_word_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_search_word_daily (
  search_word STRING COMMENT '搜索词（如轩妈家、蛋黄酥）',
  stat_date DATE COMMENT '统计日期',
  product_id STRING COMMENT '被搜索的商品ID',
  search_count INT COMMENT '搜索次数',
  click_count INT COMMENT '点击次数',
  visitor_count INT COMMENT '搜索带来的访客数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_search_word_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '搜索词数据ODS层（支撑TOP10搜索词分析🔶2-64🔶）'
);

-- 6. 价格力商品基础表（gmall_ins.ods_price_strength_product）
drop table if exists ods_price_strength_product;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_price_strength_product (
  product_id STRING COMMENT '商品ID',
  price_strength_level STRING COMMENT '价格力等级（优秀/良好/较差🔶2-93🔶）',
  coupon_price DECIMAL(10,2) COMMENT '普惠券后价',
  price_band STRING COMMENT '价格带（如0-50元）',
  same_category_avg_price DECIMAL(10,2) COMMENT '同类目均价',
  update_time TIMESTAMP COMMENT '等级更新时间'  -- 已修改为TIMESTAMP类型
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_price_strength_product'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '价格力商品基础ODS层（支撑价格力商品排行🔶2-89🔶）'
);

-- 7. 商品预警明细表（gmall_ins.ods_product_warning_daily）
drop table if exists ods_product_warning_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_product_warning_daily (
  product_id STRING COMMENT '商品ID',
  stat_date STRING COMMENT '预警日期',
  warning_type STRING COMMENT '预警类型（价格力预警/商品力预警🔶2-94🔶）',
  warning_reason STRING COMMENT '预警原因（如持续低星、转化率低于市场平均🔶2-95🔶🔶2-97🔶）',
  warning_level STRING COMMENT '预警等级（一般/严重）',
  handle_status STRING COMMENT '处理状态（未处理/已处理）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_product_warning_daily'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '商品预警明细ODS层（支撑预警商品监控）'
);

-- 8. 商品分类维度表（gmall_ins.ods_product_category）
drop table if exists ods_product_category;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_product_category (
  category_id STRING COMMENT '分类ID',
  category_name STRING COMMENT '分类名称（如零食）',
  parent_category_id STRING COMMENT '父分类ID（如食品）',
  level INT COMMENT '分类层级（1/2/3）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_product_category'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '商品分类维度ODS层（支撑按分类展示商品排行🔶2-20🔶）'
);

-- 9. 店铺基础信息表（gmall_ins.ods_shop_base）
drop table if exists ods_shop_base;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_shop_base (
  shop_id STRING COMMENT '店铺ID',
  shop_name STRING COMMENT '店铺名称',
  business_scope STRING COMMENT '经营范围',
  opening_date DATE COMMENT '开店日期'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_shop_base'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '店铺基础信息ODS层（关联商品所属店铺）'
);

-- 10. 商品趋势指标表（gmall_ins.ods_product_trend_hourly）
drop table if exists ods_product_trend_hourly;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_ins.ods_product_trend_hourly (
  product_id STRING COMMENT '商品ID',
  stat_date DATE COMMENT '统计日期',
  stat_hour INT COMMENT '统计小时（0-23）',
  visitor_count INT COMMENT '小时访客数',
  sales_quantity INT COMMENT '小时销量',
  pay_amount DECIMAL(10,2) COMMENT '小时销售额'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/gmall_ins/ods/ods_product_trend_hourly'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '商品趋势指标ODS层（支撑商品数据趋势查看🔶2-83🔶）'
);