--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-22 11:23:38
--********************************************************************--

INSERT OVERWRITE TABLE dws_gmall_trade_user_payment_1d PARTITION (ds)
SELECT
    user_id,
    SUM(CAST(sku_num as BIGINT )) payment_suc_sku_num_1d,
    COUNT(*)  payment_suc_count_1d,
    SUM(split_payment_amount) payment_suc_amount_1d,
    ds
FROM dwd_gmall_trade_payment_suc_od_di
WHERE ds=${ds}
GROUP BY user_id,ds;