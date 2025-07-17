import pyspark2hive.sparktohive  as ts


sql = """
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    date_format(data.create_time, 'yyyy-MM-dd')
from ods_cart_info data
    where ds = '20250627';
    """
time = '2025-06-27'
tableName = 'dwd_trade_cart_add_inc'

ts.execute_hive_insert(sql,time,tableName)

sql1 = """
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    cast(sku_num as BIGINT),
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.create_time,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail data
    where ds = '20250627'
) od
left join
(
    select
        data.id,
        data.user_id,
        data.province_id
    from ods_order_info data
    where ds = '2025-06-27'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity data
    where ds = '20250627'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon data
    where ds = '20250627'
) cou
on od.id = cou.order_detail_id;
"""
time1 = "2025-06-07"
tableName1 = "dwd_trade_order_detail_inc"
ts.execute_hive_insert(sql1,time1,tableName1)



sql2 = """
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    cast(sku_num as BIGINT),
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail data
    where ds = '20250627'
) od
join
(
    select
        data.user_id,
        data.order_id,
        data.payment_type,
        data.callback_time
    from ods_payment_info data
    where ds='20250627'
) pi
on od.order_id=pi.order_id
left join
(
    select
        data.id,
        data.province_id
    from ods_order_info data
    where ds = '2025-06-27'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity data
    where ds = '20250627'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon data
    where ds = '20250627'
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic
    where ds='20250627'
    and parent_code='11'
) pay_dic
on pi.payment_type=pay_dic.dic_code;
"""
time2 = "2025-06-27"
tableName2 = "dwd_trade_pay_detail_suc_inc"
ts.execute_hive_insert(sql2,time2,tableName2)


sql3 = """
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0)
from
(
    select
        data.id,
        data.user_id,
        data.province_id,
        data.create_time,
        data.original_total_amount,
        data.activity_reduce_amount,
        data.coupon_reduce_amount,
        data.total_amount
    from ods_order_info data
    where ds='2025-06-27'
)oi
left join
(
    select
        data.order_id,
        data.callback_time,
        data.total_amount payment_amount
    from ods_payment_info data
    where ds='20250627'
)pi
on oi.id=pi.order_id
left join
(
    select
        data.order_id,
        data.operate_time finish_time
    from ods_order_status_log data
    where ds='20250627'
    and data.order_status='1004'
)log
on oi.id=log.order_id;
"""
time3 = "2025-06-27"
tableName3 = "dwd_trade_trade_flow_acc"
ts.execute_hive_insert(sql3,time3,tableName3)

sql4 = """
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select user_id,
                channel,
                province_id,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts
         from (
             SELECT
    -- 从 JSON 中提取 common 结构体字段
    get_json_object(json, '$.common.uid') AS user_id,
    get_json_object(json, '$.common.ch') AS channel,
    get_json_object(json, '$.common.ar') AS province_id,
    get_json_object(json, '$.common.vc') AS version_code,
    get_json_object(json, '$.common.mid') AS mid_id,
    get_json_object(json, '$.common.ba') AS brand,
    get_json_object(json, '$.common.md') AS model,
    get_json_object(json, '$.common.os') AS operate_system,

    -- 提取时间戳字段
    get_json_object(json, '$.ts') AS ts,

    -- 窗口函数（假设 common.sid 存在）
    row_number() OVER (
        PARTITION BY get_json_object(json, '$.common.sid')
        ORDER BY cast(get_json_object(json, '$.ts') AS bigint)
    ) AS rn

FROM tmp_json_test

WHERE
     get_json_object(json, '$.page') IS NOT NULL
    AND get_json_object(json, '$.common.uid') IS NOT NULL
              ) t1
     ) t2;

"""
time4 = "2025-06-28"
tableName4 = "dwd_user_login_inc"
ts.execute_hive_insert(sql4,time4,tableName4)

sql5 = """
SELECT
    ui.user_id,
    date_format(create_time, 'yyyy-MM-dd') AS date_id,
    create_time,
    log.channel,
    log.province_id,
    log.version_code,
    log.mid_id,
    log.brand,
    log.model,
    log.operate_system,
    date_format(create_time, 'yyyy-MM-dd') AS ds  -- 分区字段
FROM (
    select
        data.id user_id,
        data.create_time
    from ods_user_info data
    where ds='20250627'
) ui
LEFT JOIN (
    SELECT
    -- 从 json 字段中提取 common 结构体的各个字段
    get_json_object(json, '$.common.ar') AS province_id,
    get_json_object(json, '$.common.ba') AS brand,
    get_json_object(json, '$.common.ch') AS channel,
    get_json_object(json, '$.common.md') AS model,
    get_json_object(json, '$.common.mid') AS mid_id,
    get_json_object(json, '$.common.os') AS operate_system,
    get_json_object(json, '$.common.uid') AS user_id,
    get_json_object(json, '$.common.vc') AS version_code
FROM tmp_json_test
WHERE get_json_object(json, '$.common.uid') IS NOT NULL
) log ON ui.user_id = log.user_id;
"""
time5 = "2025-06-28"
tableName5 = "dwd_user_register_inc"
ts.execute_hive_insert(sql5,time5,tableName5)