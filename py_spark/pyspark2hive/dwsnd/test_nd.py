from pyspark2hive import orcmonth as ts
job_list = [
    {
        "sql": """
            SELECT
                province_id,
                province_name,
                area_code,
                iso_code,
                iso_3166_2,
                sum(if(ds>=date_add('2025-06-27',-6),order_count_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),order_original_amount_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),activity_reduce_amount_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),coupon_reduce_amount_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),order_total_amount_1d,0)),
                sum(order_count_1d),
                sum(order_original_amount_1d),
                sum(activity_reduce_amount_1d),
                sum(coupon_reduce_amount_1d),
                sum(order_total_amount_1d)
            FROM dws_trade_province_order_1d
            WHERE ds>=date_add('2025-06-27',-29)
            AND ds<='2025-06-27'
            GROUP BY province_id,province_name,area_code,iso_code,iso_3166_2
        """,
        "partition_date": "2025-06-27",
        "table_name": "dws_trade_province_order_nd"
    },
    {
        "sql": """
            SELECT
                user_id,
                min(ds) order_date_first,
                max(ds) order_date_last,
                sum(order_count_1d) order_count,
                sum(order_num_1d) order_num,
                sum(order_original_amount_1d) original_amount,
                sum(activity_reduce_amount_1d) activity_reduce_amount,
                sum(coupon_reduce_amount_1d) coupon_reduce_amount,
                sum(order_total_amount_1d) total_amount
            FROM dws_trade_user_order_1d
            GROUP BY user_id
        """,
        "partition_date": "2025-06-27",
        "table_name": "dws_trade_user_order_td"
    },
    {
        "sql": """
            SELECT nvl(old.user_id, new.user_id),
                   if(old.user_id is not null, old.order_date_first, '2025-06-27'),
                   if(new.user_id is not null, '2022-06-27', old.order_date_last),
                   nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
                   nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
                   nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
                   nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
                   nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
                   nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
            FROM (
                     SELECT user_id,
                            order_date_first,
                            order_date_last,
                            order_count_td,
                            order_num_td,
                            original_amount_td,
                            activity_reduce_amount_td,
                            coupon_reduce_amount_td,
                            total_amount_td
                     FROM dws_trade_user_order_td
                     WHERE ds = date_add('2025-06-27', -1)
                 ) old
                 FULL OUTER JOIN
             (
                 SELECT user_id,
                        order_count_1d,
                        order_num_1d,
                        order_original_amount_1d,
                        activity_reduce_amount_1d,
                        coupon_reduce_amount_1d,
                        order_total_amount_1d
                 FROM dws_trade_user_order_1d
                 WHERE ds = '2025-06-27'
             ) new
             ON old.user_id = new.user_id
        """,
        "partition_date": "2025-06-27",
        "table_name": "dws_trade_user_order_td"
    },
    {
        "sql": """
            SELECT
                user_id,
                sku_id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                tm_id,
                tm_name,
                sum(if(ds>=date_add('2025-06-27',-6),order_count_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),order_num_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),order_original_amount_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),activity_reduce_amount_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),coupon_reduce_amount_1d,0)),
                sum(if(ds>=date_add('2025-06-27',-6),order_total_amount_1d,0)),
                sum(order_count_1d),
                sum(order_num_1d),
                sum(order_original_amount_1d),
                sum(activity_reduce_amount_1d),
                sum(coupon_reduce_amount_1d),
                sum(order_total_amount_1d)
            FROM dws_trade_user_sku_order_1d
            WHERE ds>=date_add('2025-06-27',-29)
            GROUP BY user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name
        """,
        "partition_date": "2025-06-27",
        "table_name": "dws_trade_user_sku_order_nd"
    },
    {
        "sql": """
            SELECT u.id user_id,
                   nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
                   date_format(create_time, 'yyyy-MM-dd') login_date_first,
                   nvl(login_count_td, 1) login_count_td
            FROM (
                     SELECT id,
                            create_time
                     FROM dim_user_zip
                     WHERE ds = '9999-12-31'
                 ) u
                 LEFT JOIN
             (
                 SELECT user_id,
                        max(ds) login_date_last,
                        count(*) login_count_td
                 FROM dwd_user_login_inc
                 GROUP BY user_id
             ) l
             ON u.id = l.user_id
        """,
        "partition_date": "2025-06-27",
        "table_name": "dws_user_user_login_td"
    },
    {
        "sql": """
            SELECT nvl(old.user_id, new.user_id) user_id,
                   if(new.user_id is null, old.login_date_last, '2025-06-27') login_date_last,
                   if(old.login_date_first is null, '2022-06-27', old.login_date_first) login_date_first,
                   nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0) login_count_td
            FROM (
                     SELECT user_id,
                            login_date_last,
                            login_date_first,
                            login_count_td
                     FROM dws_user_user_login_td
                     WHERE ds = date_add('2025-06-27', -1)
                 ) old
                 FULL OUTER JOIN
             (
                 SELECT user_id,
                        count(*) login_count_1d
                 FROM dwd_user_login_inc
                 WHERE ds = '2025-06-27'
                 GROUP BY user_id
             ) new
             ON old.user_id = new.user_id
        """,
        "partition_date": "2025-06-27",
        "table_name": "dws_user_user_login_td"
    }
]

for job in job_list:
    print(f"[INFO] 正在处理表：{job['table_name']}")
    ts.execute_hive_insert(job['sql'], job['partition_date'], job['table_name'])


