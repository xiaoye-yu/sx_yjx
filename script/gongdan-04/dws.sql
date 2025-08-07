-- 一、销售分析 DWS 表（dws_sales_analysis）
drop table if exists dws_sales_analysis;
create external table if not exists dws_sales_analysis (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    item_name string comment '商品名称',
    time_type string comment '时间粒度（1d/7d/30d）', -- 对应1天/7天/30天
    time_value string comment '时间值（如2025-01-01/2025-01-01至2025-01-07/2025-01）',
    total_sales decimal(12,2) comment '总销售额',
    total_quantity int comment '总销量',
    item_sales_rank int comment '品类内商品销售排名（按销售额降序）',
    monthly_pay_progress decimal(5,2) comment '月支付金额进度（当前周期销售额/目标GMV，仅30d维度有值）',
    monthly_pay_contribution decimal(5,2) comment '月支付金额贡献（类目销售额/全店销售额，仅30d维度有值）',
    last_month_rank int comment '上月类目在本店同级类目的销售排名'
)
partitioned by (dt string comment '分区日期（与time_value的结束日期一致）')
stored as orc
location '/bigdata_warehouse/dws/dws_sales_analysis'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '销售分析DWS表，聚合品类及商品的销售核心指标，支撑销售分析看板'
);
 set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_sales_analysis partition (dt)
-- 1. 1d维度聚合（单天）
select
    category_id,
    category_name,
    item_name,
    '1d' as time_type,
    create_dt as time_value,
    sum(pay_amount) as total_sales,
    sum(pay_quantity) as total_quantity,
    rank() over(partition by category_id, create_dt order by sum(pay_amount) desc) as item_sales_rank,
    null as monthly_pay_progress, -- 1d维度无月进度
    null as monthly_pay_contribution, -- 1d维度无月贡献
    last_month_rank,
    create_dt as dt
from dwd_trade_sales_detail
group by category_id, category_name, item_name, create_dt, last_month_rank
union all
-- 2. 7d维度聚合（近7天，按结束日期分区）
select
    category_id,
    category_name,
    item_name,
    '7d' as time_type,
    concat(date_sub(create_dt, 6), '至', create_dt) as time_value, -- 时间范围描述
    sum(pay_amount) as total_sales,
    sum(pay_quantity) as total_quantity,
    rank() over(partition by category_id, create_dt order by sum(pay_amount) desc) as item_sales_rank, -- 按结束日期分区排序
    null as monthly_pay_progress, -- 7d维度无月进度
    null as monthly_pay_contribution, -- 7d维度无月贡献
    last_month_rank,
    create_dt as dt -- 以结束日期为分区
from dwd_trade_sales_detail
group by category_id, category_name, item_name, create_dt, last_month_rank
union all
-- 3. 30d维度聚合（近30天，按结束日期分区）
select
    t.category_id,
    t.category_name,
    t.item_name,
    '30d' as time_type,
    concat(date_sub(t.create_dt, 29), '至', t.create_dt) as time_value,
    sum(t.pay_amount) as total_sales,
    sum(t.pay_quantity) as total_quantity,
    rank() over(partition by t.category_id, t.create_dt order by sum(t.pay_amount) desc) as item_sales_rank,
    (sum(t.pay_amount)/t.target_gmv)*100 as monthly_pay_progress, -- 30d维度计算月进度
    (sum(t.pay_amount)/d.total_30d_sales)*100 as monthly_pay_contribution, -- 使用JOIN后的30天总额
    t.last_month_rank,
    t.create_dt as dt
from dwd_trade_sales_detail t
-- 提前计算每个结束日期对应的30天总销售额
join (
    select
        create_dt as end_dt,
        sum(pay_amount) as total_30d_sales
    from dwd_trade_sales_detail
    group by create_dt
) d on t.create_dt = d.end_dt
group by t.category_id, t.category_name, t.item_name, t.create_dt, t.target_gmv, t.last_month_rank, d.total_30d_sales;


-- 二、属性分析 DWS 表（dws_attribute_analysis）
drop table if exists dws_attribute_analysis;
create external table if not exists dws_attribute_analysis (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    attr_id string comment '属性ID',
    attr_value string comment '属性值（如红色、XL）',
    time_type string comment '时间粒度（1d/7d/30d）',
    time_value string comment '时间值（如2025-01-01/2025-01-01至2025-01-07/2025-01-01至2025-01-30）',
    attr_visitor_count int comment '属性流量（独立访客数）',
    attr_pay_conversion decimal(5,2) comment '属性支付转化率（支付用户数/访问用户数，%）',
    active_item_count int comment '动销商品数'
)
partitioned by (dt string comment '分区日期（与time_value的结束日期一致）')
stored as orc
location '/bigdata_warehouse/dws/dws_attribute_analysis'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '属性分析DWS表，聚合品类属性的流量及转化指标'
);

insert overwrite table dws_attribute_analysis partition (dt)
-- 1. 1d维度聚合
select
    category_id,
    category_name,
    attr_id,
    attr_value,
    '1d' as time_type,
    visit_dt as time_value,
    count(distinct visitor_id) as attr_visitor_count,
    (count(distinct case when is_pay=1 then visitor_id end)/count(distinct visitor_id))*100 as attr_pay_conversion,
    count(distinct item_id) as active_item_count,
    visit_dt as dt
from dwd_item_attribute_detail
group by category_id, category_name, attr_id, attr_value, visit_dt
union all
-- 2. 7d维度聚合
select
    category_id,
    category_name,
    attr_id,
    attr_value,
    '7d' as time_type,
    concat(date_sub(visit_dt, 6), '至', visit_dt) as time_value,
    count(distinct visitor_id) as attr_visitor_count,
    (count(distinct case when is_pay=1 then visitor_id end)/count(distinct visitor_id))*100 as attr_pay_conversion,
    count(distinct item_id) as active_item_count,
    visit_dt as dt
from dwd_item_attribute_detail
group by category_id, category_name, attr_id, attr_value, visit_dt
union all
-- 3. 30d维度聚合
select
    category_id,
    category_name,
    attr_id,
    attr_value,
    '30d' as time_type,
    concat(date_sub(visit_dt, 29), '至', visit_dt) as time_value,
    count(distinct visitor_id) as attr_visitor_count,
    (count(distinct case when is_pay=1 then visitor_id end)/count(distinct visitor_id))*100 as attr_pay_conversion,
    count(distinct item_id) as active_item_count,
    visit_dt as dt
from dwd_item_attribute_detail
group by category_id, category_name, attr_id, attr_value, visit_dt;


-- 三、流量分析 DWS 表（dws_traffic_analysis）

drop table if exists dws_traffic_analysis;
create external table if not exists dws_traffic_analysis (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    channel string comment '流量渠道（如搜索、直通车、推荐）',
    search_word string comment '热搜词（非搜索渠道为null）',
    time_value string comment '时间值（如2025-01-01至2025-01-30）',
    channel_visitor_count int comment '渠道访客数',
    channel_pay_conversion decimal(5,2) comment '渠道支付转化率（%）',
    hot_word_visitor_count int comment '热搜词访客量',
    hot_word_rank int comment '热搜词排名'
)
partitioned by (dt string comment '分区日期（与time_value的结束日期一致）')
stored as orc
location '/bigdata_warehouse/dws/dws_traffic_analysis'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '流量分析DWS表，聚合品类流量渠道及热搜词指标'
);

insert overwrite table dws_traffic_analysis partition (dt)
select
    category_id,
    category_name,
    channel,
    search_word,
    concat(date_sub(visit_dt, 29), '至', visit_dt) as time_value,
    count(distinct visitor_id) as channel_visitor_count,
    (count(distinct case when is_pay=1 then visitor_id end)/count(distinct visitor_id))*100 as channel_pay_conversion,
    count(distinct case when search_word is not null then visitor_id end) as hot_word_visitor_count,
    rank() over(partition by category_id, visit_dt order by count(distinct case when search_word is not null then visitor_id end) desc) as hot_word_rank,
    visit_dt as dt
from dwd_traffic_source_detail
group by category_id, category_name, channel, search_word, visit_dt;

-- 四、客群洞察 DWS 表（dws_customer_insight）

drop table if exists dws_customer_insight;
create external table if not exists dws_customer_insight (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    behavior_type string comment '用户行为类型（search/visit/pay）',
    age int comment '用户年龄',
    gender string comment '用户性别',
    region string comment '用户地域',
    consumption_level string comment '用户消费层级',
    time_value string comment '时间值（如2025-01-01至2025-01-30）',
    crowd_count int comment '人群数量',
    crowd_ratio decimal(5,2) comment '人群占比（%）'
)
partitioned by (dt string comment '分区日期（与time_value的结束日期一致）')
stored as orc
location '/bigdata_warehouse/dws/dws_customer_insight'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'comment' = '客群洞察DWS表，聚合品类用户行为及人群属性指标'
);

insert overwrite table dws_customer_insight partition (dt)
select
    t.category_id,
    t.category_name,
    t.behavior_type,
    t.age,
    t.gender,
    t.region,
    t.consumption_level,
    concat(date_sub(t.behavior_dt, 29), '至', t.behavior_dt) as time_value,
    count(distinct t.user_id) as crowd_count,
    (count(distinct t.user_id)/total.total_users)*100 as crowd_ratio,
    t.behavior_dt as dt
from dwd_user_behavior_detail t
-- 提前计算每个维度的总用户数
join (
    select
        category_id,
        behavior_type,
        behavior_dt,
        count(distinct user_id) as total_users
    from dwd_user_behavior_detail
    group by category_id, behavior_type, behavior_dt
) total on t.category_id = total.category_id
    and t.behavior_type = total.behavior_type
    and t.behavior_dt = total.behavior_dt
group by
    t.category_id,
    t.category_name,
    t.behavior_type,
    t.age,
    t.gender,
    t.region,
    t.consumption_level,
    t.behavior_dt,
    total.total_users;