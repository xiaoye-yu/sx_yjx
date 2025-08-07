-- 一、销售分析 ADS 表（ads_sales_analysis）
drop table if exists ads_sales_analysis;
create external table if not exists ads_sales_analysis (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    item_name string comment '商品名称',
    time_type string comment '时间粒度（1d/7d/30d）',
    total_sales decimal(12,2) comment '总销售额',
    total_quantity int comment '总销量',
    item_sales_rank int comment '品类内商品销售排名',
    monthly_pay_progress decimal(5,2) comment '月支付金额进度（%）',
    monthly_pay_contribution decimal(5,2) comment '月支付金额贡献（%）',
    last_month_rank int comment '上月类目销售排名'
)
partitioned by (dt string comment '分区日期（与time_value结束日期一致）')
stored as orc
location '/bigdata_warehouse/ads/ads_sales_analysis'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '销售分析ADS表，支撑销售核心概况报表'
);
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads_sales_analysis partition (dt)
select
    category_id,
    category_name,
    item_name,
    time_type,
    total_sales,
    total_quantity,
    item_sales_rank,
    monthly_pay_progress,
    monthly_pay_contribution,
    last_month_rank,
    ${pt}
from dws_sales_analysis
where dt = '2025-01-30';

-- 二、属性分析 ADS 表（ads_attribute_analysis）

drop table if exists ads_attribute_analysis;
create external table if not exists ads_attribute_analysis (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    attr_id string comment '属性ID',
    attr_value string comment '属性值（如红色、XL）',
    time_type string comment '时间粒度（1d/7d/30d）',
    attr_visitor_count int comment '属性流量（独立访客数）',
    attr_pay_conversion decimal(5,2) comment '属性支付转化率（%）',
    active_item_count int comment '动销商品数'
)
partitioned by (dt string comment '分区日期（与time_value结束日期一致）')
stored as orc
location '/bigdata_warehouse/ads/ads_attribute_analysis'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '属性分析ADS表，支撑属性流量及转化报表'
);


insert overwrite table ads_attribute_analysis partition (dt)
select
    category_id,
    category_name,
    attr_id,
    attr_value,
    time_type,
    attr_visitor_count,
    attr_pay_conversion,
    active_item_count,
    ${pt}
from dws_attribute_analysis
where dt = '2025-01-30';


-- 三、流量分析 ADS 表（ads_traffic_analysis）
drop table if exists ads_traffic_analysis;
create external table if not exists ads_traffic_analysis (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    channel string comment '流量渠道（如搜索、直通车）',
    search_word string comment '热搜词（非搜索渠道为null）',
    channel_visitor_count int comment '渠道访客数',
    channel_pay_conversion decimal(5,2) comment '渠道支付转化率（%）',
    hot_word_visitor_count int comment '热搜词访客量',
    hot_word_rank int comment '热搜词排名'
)
partitioned by (dt string comment '分区日期（月份最后一天，如2025-01-31）')
stored as orc
location '/bigdata_warehouse/ads/ads_traffic_analysis'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '流量分析ADS表，支撑渠道及热搜词报表'
);

insert overwrite table ads_traffic_analysis partition (dt)
select
    category_id,
    category_name,
    channel,
    search_word,
    channel_visitor_count,
    channel_pay_conversion,
    hot_word_visitor_count,
    hot_word_rank,
    ${pt}
from dws_traffic_analysis
where dt = '2025-01-30';


-- 四、客群洞察 ADS 表（ads_customer_insight）
drop table if exists ads_customer_insight;
create external table if not exists ads_customer_insight (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    behavior_type string comment '用户行为类型（search/visit/pay）',
    crowd_count int comment '对应行为人群的总数量' -- 核心指标：各行为人群的总人数
)
partitioned by (dt string )
stored as orc
location '/bigdata_warehouse/ads/ads_customer_insight'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '客群洞察ADS表，聚焦搜索/访问/支付人群数量统计'
);

insert overwrite table ads_customer_insight partition (dt)
select
    category_id,
    category_name,
    behavior_type,
    sum(crowd_count),
    ${pt}
from dws_customer_insight
group by category_id, category_name, behavior_type;