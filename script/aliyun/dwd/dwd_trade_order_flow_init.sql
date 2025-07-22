--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-21 11:00:55
--********************************************************************--

SET odps.sql.hive.compatible=TRUE;
INSERT OVERWRITE TABLE dwd_gmall_trade_trade_flow_order_di PARTITION (ds)
SELECT
    oi.order_id,
    user_id,
    province_id,
    order_time,
    payment_time,
    delivery_time,
    closed_time,
    finished_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    IF(payment_time is null,0,total_amount) payment_amount,
    IF(NVL(closed_time,finished_time) is not null,DATE_FORMAT(CAST(NVL(closed_time,finished_time) AS TIMESTAMP),'yyyyMMdd'),'99991231') ds
FROM (
         SELECT
             id order_id,
             user_id,
             province_id,
             create_time order_time,
             original_total_amount,
             activity_reduce_amount,
             coupon_reduce_amount,
             total_amount
         FROM ods_order_info_di_init
         WHERE pt=${ds}
     )oi
         LEFT JOIN (
    SELECT
        order_id,
        create_time payment_time
    FROM ods_order_status_log_di_init
    WHERE pt=${ds}
      AND order_status='1002'
)payment
                   on oi.order_id=payment.order_id
         LEFT JOIN (
    SELECT
        order_id,
        create_time delivery_time
    FROM ods_order_status_log_di_init
    WHERE pt=${ds}
      AND order_status='1004'
)delivery
                   on oi.order_id=delivery.order_id
         LEFT JOIN (
    SELECT
        order_id,
        create_time closed_time
    FROM ods_order_status_log_di_init
    WHERE pt=${ds}
      AND order_status='1005'
)closed
                   on oi.order_id=closed.order_id
         LEFT JOIN (
    SELECT
        order_id,
        create_time finished_time
    FROM ods_order_status_log_di_init
    WHERE pt=${ds}
      AND order_status='1006'
)finished
                   on oi.order_id=finished.order_id;


SELECT * FROM dwd_gmall_trade_trade_flow_order_di WHERE ds = '99991231' ;





SET odps.sql.hive.compatible=TRUE;
INSERT OVERWRITE TABLE dwd_gmall_trade_trade_flow_order_di PARTITION (ds)
SELECT
    oi.order_id,
    user_id,
    province_id,
    order_time,
    NVL(payment.payment_time,oi.payment_time) payment_time,
    NVL(delivery.delivery_time,oi.delivery_time) delivery_time,
    NVL(closed.closed_time,oi.closed_time) closed_time,
    NVL(finished.finished_time,oi.finished_time) finished_time,
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    IF(NVL(payment.payment_time,oi.payment_time) is null,0,oi.payment_amount)  payment_amount,
    IF(NVL(NVL(closed.closed_time,oi.closed_time),NVL(finished.finished_time,oi.finished_time)) is not null,
       DATE_FORMAT(NVL(NVL(closed.closed_time,oi.closed_time),NVL(finished.finished_time,oi.finished_time)),'yyyyMMdd'),
       '99991231') ds
FROM (
         SELECT
             order_id,
             user_id,
             province_id,
             order_time,
             payment_time,
             delivery_time,
             closed_time,
             finished_time,
             order_original_amount,
             order_activity_amount,
             order_coupon_amount,
             payment_amount
         FROM dwd_gmall_trade_trade_flow_order_di
         WHERE ds='99991231'
         union
         SELECT
             CAST(id AS STRING) order_id,
             CAST(user_id AS STRING) user_id,
             CAST(province_id AS STRING) province_id,
             TO_CHAR(create_time, 'yyyy-MM-dd HH:mm:ss') order_time,
             null payment_time,
             null delivery_time,
             null closed_time,
             null finished_time,
             original_total_amount order_original_amount,
             activity_reduce_amount order_activity_amount,
             coupon_reduce_amount order_coupon_amount,
             total_amount payment_amount
         FROM ods_order_info_ri
         WHERE pt = '2025'
     )oi
         LEFT JOIN (
    SELECT
        order_id,
        create_time payment_time
    FROM ods_order_status_log_ri
    WHERE pt = '2025'
      AND order_status='1002'
)payment
                   on oi.order_id=payment.order_id
         LEFT JOIN (
    SELECT
        order_id,
        create_time delivery_time
    FROM ods_order_status_log_ri
    WHERE pt = '2025'
      AND order_status='1004'
)delivery
                   on oi.order_id=delivery.order_id
         LEFT JOIN (
    SELECT
        order_id,
        create_time closed_time
    FROM ods_order_status_log_ri
    WHERE pt = '2025'
      AND order_status='1005'
)closed
                   on oi.order_id=closed.order_id
         LEFT JOIN (
    SELECT
        order_id,
        create_time finished_time
    FROM ods_order_status_log_ri
    WHERE pt = '2025'
      AND order_status='1006'
)finished
                   on oi.order_id=finished.order_id;


SELECT * FROM dwd_gmall_trade_trade_flow_order_di WHERE ds = '20250720';





