--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-21 10:49:40
--********************************************************************--

SET odps.sql.hive.compatible=TRUE;
INSERT OVERWRITE TABLE dwd_gmall_trade_payment_suc_od_di PARTITION (ds)
SELECT
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    payment_type,
    dic_name payment_name,
    callback_time payment_time,
    sku_num,
    split_original_amount,
    split_total_amount,
    split_activity_amount,
    split_coupon_amount,
    DATE_FORMAT(CAST(callback_time AS TIMESTAMP),'yyyyMMdd') ds
FROM (
         SELECT
             order_id,
             user_id,
             payment_type,
             callback_time
         FROM ods_payment_info_di_init
         WHERE pt=${ds} and payment_status='1602'
     )payment
         JOIN (
    SELECT
        id,
        province_id
    from ods_order_info_di_init
    WHERE pt=${ds}
)oi
              on payment.order_id=oi.id
         JOIN (
    SELECT
        id,
        order_id,
        sku_id,
        sku_num,
        order_price * sku_num split_original_amount,
        split_total_amount,
        split_activity_amount,
        split_coupon_amount
    from ods_order_detail_di_init
    WHERE pt=${ds}
)od
              on payment.order_id=od.order_id
         LEFT JOIN (
    SELECT
        dic_code,
        dic_name
    from ods_dic_di
    WHERE pt=${ds} and parent_code='11'
)dic
                   on payment_type=dic_code;


SELECT * FROM dwd_gmall_trade_payment_suc_od_di WHERE ds = '20220611';


-- 每日增量

SET odps.sql.hive.compatible=TRUE;
INSERT OVERWRITE TABLE dwd_gmall_trade_payment_suc_od_di PARTITION (ds)
SELECT
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    payment_type,
    dic_name payment_name,
    callback_time payment_time,
    sku_num,
    split_original_amount,
    split_total_amount,
    split_activity_amount,
    split_coupon_amount,
    DATE_FORMAT(callback_time,'yyyyMMdd') ds
FROM (
         SELECT
             order_id,
             user_id,
             payment_type,
             callback_time
         FROM ods_payment_info_ri
         WHERE pt = ${y}
           and payment_status='1602'
     )payment
         JOIN (
    SELECT
        id,
        province_id
    from ods_order_info_ri
    WHERE pt = ${y}
      AND order_status='1002'
)oi
              on payment.order_id=oi.id
         JOIN (
    SELECT
        id,
        order_id,
        sku_id,
        sku_num,
        order_price * sku_num split_original_amount,
        split_total_amount,
        split_activity_amount,
        split_coupon_amount
    from ods_order_detail_ri
    WHERE pt = ${y}
    UNION
    SELECT
        id,
        order_id,
        sku_id,
        sku_num,
        order_price * sku_num split_original_amount,
        split_total_amount,
        split_activity_amount,
        split_coupon_amount
    FROM ods_order_detail_di_init
    WHERE pt=${ds_1}
)od
              on payment.order_id=od.order_id
         LEFT JOIN (
    SELECT
        dic_code,
        dic_name
    from ods_dic_di
    WHERE pt=${ds} and parent_code='11'
)dic
                   on payment_type=dic_code;



SELECT * FROM dwd_gmall_trade_payment_suc_od_di WHERE ds = '20250720';