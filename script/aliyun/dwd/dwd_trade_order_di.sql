--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-21 10:44:31
--********************************************************************--


SET odps.sql.hive.compatible=TRUE;
INSERT OVERWRITE TABLE dwd_gmall_trade_order_od_di PARTITION (ds)
SELECT
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    create_time,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    date_format(create_time,'yyyyMMdd') ds
FROM (
         SELECT
             id,
             order_id,
             sku_id,
             create_time,
             sku_num,
             order_price * sku_num split_original_amount,
             split_activity_amount,
             split_coupon_amount,
             split_total_amount
         FROM ods_order_detail_ri
         WHERE pt = '2025'
     )od
         JOIN (
    SELECT
        id,
        user_id,
        province_id
    FROM ods_order_info_ri
    WHERE pt = '2025'
)oi
              on od.order_id=oi.id;

SELECT * FROM dwd_gmall_trade_order_od_di WHERE ds = '20250720';