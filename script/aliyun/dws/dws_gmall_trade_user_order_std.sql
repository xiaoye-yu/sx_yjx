--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-22 19:43:49
--********************************************************************--

set odps.sql.hive.compatible=true ;
INSERT OVERWRITE TABLE dws_gmall_trade_province_order_nd PARTITION (ds)
SELECT
    t1.province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_7d,
    user_count_7d,
    coupon_reduce_amount_7d,
    order_total_amount_7d,
    order_original_amount_7d,
    activity_reduce_amount_7d,
    user_count_7d,
    coupon_reduce_amount_30d,
    order_total_amount_30d,
    order_original_amount_30d,
    activity_reduce_amount_30d,
    order_count_30d,
    ${ds}
FROM (
         SELECT
             province_id,
             province_name,
             area_code,
             iso_code,
             iso_3166_2,
             SUM(IF(ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-6),'yyyyMMdd'),activity_reduce_amount_1d,0) ) activity_reduce_amount_7d,
             SUM(IF(ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-6),'yyyyMMdd'),coupon_reduce_amount_1d,0) ) coupon_reduce_amount_7d,
             SUM(IF(ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-6),'yyyyMMdd'),order_total_amount_1d,0) ) order_total_amount_7d,
             SUM(IF(ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-6),'yyyyMMdd'),order_original_amount_1d,0) ) order_original_amount_7d,
             SUM(IF(ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-6),'yyyyMMdd'),order_count_1d,0) ) order_count_7d,
             SUM(activity_reduce_amount_1d) activity_reduce_amount_30d,
             sum(coupon_reduce_amount_1d)coupon_reduce_amount_30d,
             SUM(order_total_amount_1d) order_total_amount_30d,
             SUM(order_original_amount_1d) order_original_amount_30d,
             SUM(order_count_1d) order_count_30d
         FROM dws_gmall_trade_province_order_1d
         WHERE ds<=${ds} and ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-29),'yyyyMMdd')
         GROUP BY province_id,
                  province_name,
                  area_code,
                  iso_code,
                  iso_3166_2
     )t1
         JOIN (
    SELECT
        province_id,
        COUNT(DISTINCT user_id) user_count_30d
    FROM dwd_gmall_trade_order_od_di
    WHERE ds<=${ds} and ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-29),'yyyyMMdd')
    GROUP BY province_id
)t2
              on t1.province_id=t2.province_id
         join (
    SELECT
        province_id,
        COUNT(DISTINCT user_id) user_count_7d
    FROM dwd_gmall_trade_order_od_di
    WHERE ds<=${ds} and ds>= DATE_FORMAT(DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(${ds},'yyyyMMdd')),'yyyy-MM-dd'),-6),'yyyyMMdd')
    GROUP BY province_id
)t3
              on t1.province_id=t3.province_id;