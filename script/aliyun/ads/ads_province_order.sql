--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-22 21:54:47
--********************************************************************--

SET odps.sql.hive.compatible=TRUE;
INSERT OVERWRITE TABLE ads_gmall_gmall_trade_topic_order_by_province_nd PARTITION (ds)
SELECT
    FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd'),'yyyy-MM-dd') dt,
    nd.province_id,
    nd.province_name,
    nd.area_code,
    nd.iso_code,
    nd.iso_3166_2,
    od.order_user_num_1d,
    od.order_total_amount_1d,
    nd.order_user_num_1w,
    nd.order_total_amount_1w,
    nd.order_user_num_1m,
    nd.order_total_amount_1m,
    ${ds}
FROM dws_gmall_trade_province_order_nd nd
         LEFT  JOIN dws_gmall_trade_province_order_1d od
                    on od.province_id=nd.province_id
WHERE od.ds=${ds} and nd.ds=${ds};