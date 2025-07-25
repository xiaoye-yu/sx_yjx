drop table if exists dwd_bound_sort_inc;
create external table dwd_bound_sort_inc(
  `id` bigint COMMENT '中转记录ID',
  `order_id` bigint COMMENT '订单ID',
  `org_id` bigint COMMENT '机构ID',
  `sort_time` string COMMENT '分拣时间',
  `sorter_emp_id` bigint COMMENT '分拣人员'
) comment '中转域分拣事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_bound_sort_inc'
    tblproperties('orc.compress' = 'snappy');


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_sort_inc
    partition (dt)
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           cast(after.sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') sort_time,
       after.sorter_emp_id,
       date_format(from_utc_timestamp(
                           cast(after.create_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound after ;

insert overwrite table dwd_bound_sort_inc
    partition (dt='2025-07-11')
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           cast(after.sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') sort_time,
       after.sorter_emp_id
from ods_order_org_bound after ;