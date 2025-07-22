--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-22 18:56:10
--********************************************************************--


set odps.sql.hive.compatible=true;
insert overwrite table dws_gmall_trade_order_to_pay_interval_avg_1d  PARTITION (ds)
select
    cast(avg(unix_timestamp(payment_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(order_time,'yyyy-MM-dd HH:mm:ss')) as bigint),
    DATE_FORMAT(payment_time,'yyyyMMdd') ds
FROM dwd_gmall_trade_trade_flow_order_di
where (ds='99991231' or ds=${ds})
  and DATE_FORMAT(payment_time,'yyyyMMdd')=${ds}
GROUP by DATE_FORMAT(payment_time,'yyyyMMdd');

