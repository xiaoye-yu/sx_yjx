--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-22 21:47:28
--********************************************************************--

set odps.sql.hive.compatible=true;
INSERT OVERWRITE TABLE ads_gmall_gmall_user_topic_new_user_order_stats_nd PARTITION (ds=${ds})
SELECT
    from_unixtime(unix_timestamp(${ds}, 'yyyyMMdd'), 'yyyy-MM-dd') dt,
    SUM(IF(order_date_first=${ds},1,0)) new_order_user_count_1d,
    SUM(IF(order_date_first>=DATE_FORMAT(date_add(from_unixtime(unix_timestamp(${ds}, 'yyyyMMdd'), 'yyyy-MM-dd'),-6) ,'yyyyMMdd'),1,0)) new_order_user_count_7d,
    SUM(IF(order_date_first>=DATE_FORMAT(date_add(from_unixtime(unix_timestamp(${ds}, 'yyyyMMdd'), 'yyyy-MM-dd'),-29) ,'yyyyMMdd'),1,0)) new_order_user_count_30d
from dws_gmall_trade_user_order_std
WHERE ds=${ds};