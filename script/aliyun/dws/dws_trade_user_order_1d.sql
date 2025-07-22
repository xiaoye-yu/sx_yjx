--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-22 11:15:36
--********************************************************************--

INSERT OVERWRITE TABLE dws_gmall_trade_user_order_1d PARTITION (ds)
SELECT
    user_id,
    COUNT(DISTINCT order_id) order_count_1d,
    SUM(CAST(sku_num as BIGINT ) ) order_sku_num_1d,
    SUM(split_orginal_amount) order_original_amount_1d,
    SUM(NVL(split_activity_amount,0)) activity_reduce_amount_1d,
    SUM(NVL(split_coupon_amount,0)) coupon_reduce_amount_1d,
    SUM(split_total_amount) order_total_amount_1d,
    ds
FROM dwd_gmall_trade_order_od_di
WHERE ds=${ds}
GROUP BY user_id,ds;


SELECT * FROM dws_gmall_trade_user_order_1d WHERE ds<=${ds};