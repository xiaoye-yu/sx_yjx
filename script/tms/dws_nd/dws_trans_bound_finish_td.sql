drop table if exists dws_trans_bound_finish_td;
create external table dws_trans_bound_finish_td(
	`order_count` bigint comment '发单数',
	`order_amount` decimal(16,2) comment '发单金额'
) comment '物流域转运完成历史至今汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location 'warehouse/tms/dws/dws_trans_bound_finish_td'
	tblproperties('orc.compress'='snappy');


insert overwrite table dws_trans_bound_finish_td
    partition (dt = '2025-07-11')
select count(order_id)   order_count,
       sum(order_amount) order_amount
from (select order_id,
             max(amount) order_amount
      from dwd_trans_bound_finish_detail_inc
      group by order_id) distinct_info;