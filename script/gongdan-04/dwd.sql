-- 一、销售明细 DWD 表（dwd_trade_sales_detail）

drop table if exists dwd_trade_sales_detail;
create external table if not exists dwd_trade_sales_detail (
    item_id string comment '商品ID',
    item_name string comment '商品名称',
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    pay_amount decimal(10,2) comment '单条记录支付金额',
    pay_quantity int comment '单条记录支付数量',
    target_gmv decimal(12,2) comment '类目目标GMV',
    last_month_rank int comment '上月该类目在本店同级类目的销售排名',
    create_dt string comment '销售日期（yyyy-MM-dd）'
)
partitioned by (dt string comment '分区日期（yyyy-MM-dd）')
stored as orc
LOCATION '/bigdata_warehouse/work_ord/dwd/dwd_trade_sales_detail'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true',
    'comment' = '销售明细DWD表，用于支撑销售分析的基础明细数据'
);

-- 从销售全量表同步数据，按日分区加载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_sales_detail partition (dt)
select
    item_id,
    item_name,
    category_id,
    category_name,
    pay_amount,
    pay_quantity,
    target_gmv,
    last_month_rank,
    date_format(create_time, 'yyyy-MM-dd') as create_dt,
    ds
from ods_sales_full;


-- 二、商品属性明细 DWD 表（dwd_item_attribute_detail）

drop table if exists dwd_item_attribute_detail;
create external table if not exists dwd_item_attribute_detail (
    item_id string comment '商品ID',
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    attr_id string comment '属性ID（如颜色、尺寸对应的ID）',
    attr_value string comment '属性值（如红色、XL）',
    visitor_id string comment '访客ID',
    is_pay tinyint comment '是否支付（1=支付，0=未支付）',
    visit_dt string comment '访问日期（yyyy-MM-dd）'
)
partitioned by (dt string comment '分区日期（yyyy-MM-dd）')
stored as orc
LOCATION '/bigdata_warehouse/work_ord/dwd/dwd_item_attribute_detail'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true',
    'comment' = '商品属性明细DWD表，用于支撑属性分析的基础明细数据'
);

-- 从属性信息表同步数据，按日分区加载
insert overwrite table dwd_item_attribute_detail partition (dt = ${biz_date})
select
    item_id,
    category_id,
    category_name,
    attr_id,
    attr_value,
    visitor_id,
    is_pay,
    date_format(visit_time, 'yyyy-MM-dd') as visit_dt  -- 标准化访问日期
from ods_attribute_info
where ds = ${biz_date};


-- 三、流量来源明细 DWD 表（dwd_traffic_source_detail）
drop table if exists dwd_traffic_source_detail;
create external table if not exists dwd_traffic_source_detail (
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    channel string comment '流量渠道（如搜索、直通车、推荐）',
    search_word string comment '热搜词（非搜索渠道为null）',
    visitor_id string comment '访客ID',
    is_pay tinyint comment '是否支付（1=支付，0=未支付）',
    visit_dt string comment '访问日期（yyyy-MM-dd）'
)
partitioned by (dt string comment '分区日期（yyyy-MM-dd）')
stored as orc
LOCATION '/bigdata_warehouse/work_ord/dwd/dwd_traffic_source_detail'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true',
    'comment' = '流量来源明细DWD表，用于支撑流量分析的基础明细数据'
);

-- 从流量明细表同步数据，按日分区加载
insert overwrite table dwd_traffic_source_detail partition (dt = ${biz_date})
select
    category_id,
    category_name,
    channel,
    search_word,
    visitor_id,
    is_pay,
    date_format(visit_time, 'yyyy-MM-dd') as visit_dt  -- 标准化访问日期
from ods_traffic_detail
where ds = ${biz_date};

-- 四、用户行为明细 DWD 表（dwd_user_behavior_detail）

drop table if exists dwd_user_behavior_detail;
create external table if not exists dwd_user_behavior_detail (
    user_id string comment '用户ID',
    category_id string comment '品类ID',
    category_name string comment '品类名称',
    behavior_type string comment '用户行为类型（search/visit/pay）',
    behavior_dt string comment '行为日期（yyyy-MM-dd）',
    age int comment '用户年龄',
    gender string comment '用户性别（男/女）',
    region string comment '用户地域（如省/市）',
    consumption_level string comment '用户消费层级（如高/中/低）'
)
partitioned by (dt string comment '分区日期（yyyy-MM-dd）')
stored as orc
LOCATION '/bigdata_warehouse/work_ord/dwd/dwd_user_behavior_detail'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true',
    'comment' = '用户行为明细DWD表，用于支撑客群洞察的基础明细数据'
);

-- 关联用户行为表与用户信息表，同步当日数据
insert overwrite table dwd_user_behavior_detail partition (dt = ${biz_date})
select
    b.user_id,
    b.category_id,
    b.category_name,
    b.behavior_type,
    date_format(b.behavior_time, 'yyyy-MM-dd') as behavior_dt,
    u.age,
    u.gender,
    u.region,
    u.consumption_level
from ods_user_behavior_full b
left join ods_user_info u
on b.user_id = u.user_id and u.ds = ${biz_date}  -- 关联当日用户信息
where b.ds = ${biz_date};