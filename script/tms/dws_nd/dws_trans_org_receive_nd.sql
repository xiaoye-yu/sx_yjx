drop table if exists dws_trans_org_receive_nd;
create external table dws_trans_org_receive_nd(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '揽收次数',
	`order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 n 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_receive_nd/'
	tblproperties ('orc.compress'='snappy');

insert overwrite table dws_trans_org_receive_nd
    partition (dt = '2025-07-11')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_org_receive_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-11', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
