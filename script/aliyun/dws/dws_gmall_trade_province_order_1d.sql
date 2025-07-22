--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-22 11:31:46
--********************************************************************--

INSERT OVERWRITE TABLE dws_gmall_trade_province_order_1d PARTITION (ds)
SELECT
    province_id,
    name province_name,
    area_code,
    iso_code,
    iso_3166_2 ,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    order_original_amount_1d,
    order_count_1d,
    order_user_num_1d,
    ds
FROM (
         SELECT
             province_id,
             SUM(split_activity_amount) activity_reduce_amount_1d,
             SUM(split_coupon_amount) coupon_reduce_amount_1d,
             SUM(split_total_amount) order_total_amount_1d,
             SUM(split_orginal_amount) order_original_amount_1d,
             COUNT(*) order_count_1d,
             COUNT(DISTINCT user_id) order_user_num_1d ,
             ds
         FROM dwd_gmall_trade_order_od_di
         WHERE ds=${ds}
         GROUP BY province_id,ds
     )t1
         LEFT JOIN (
    SELECT
        id,
        name,
        area_code,
        iso_code,
        iso_3166_2
    from dim_province_di
    WHERE ds<=${ds}
)province
                   on t1.province_id=province.id;



