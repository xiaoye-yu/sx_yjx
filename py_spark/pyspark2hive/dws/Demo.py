from pyspark2hive import orcmonth as ts
import time

job_list = [
    {
        "sql": """
            SELECT
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
                favor_add_count,
                ds
            FROM
            (
                SELECT
                    sku_id,
                    count(*) favor_add_count
                FROM dwd_interaction_favor_add_inc
                GROUP BY sku_id
            ) favor
            LEFT JOIN
            (
                SELECT
                    id,
                    sku_name,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    tm_id,
                    tm_name,
                    ds
                FROM dim_sku_full
                WHERE ds = '20250627'
            ) sku
            ON favor.sku_id = sku.id
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_interaction_sku_favor_add_1d"
    },
    {
        "sql": """
            SELECT
                user_id,
                coupon_id,
                coupon_name,
                coupon_type_code,
                coupon_type_name,
                benefit_rule,
                used_count,
                ds
            FROM
            (
                SELECT
                    ds,
                    user_id,
                    coupon_id,
                    count(*) used_count
                FROM dwd_tool_coupon_used_inc
                GROUP BY ds, user_id, coupon_id
            ) t1
            LEFT JOIN
            (
                SELECT
                    id,
                    coupon_name,
                    coupon_type_code,
                    coupon_type_name,
                    benefit_rule
                FROM dim_coupon_full
                WHERE ds = '20250627'
            ) t2
            ON t1.coupon_id = t2.id
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_tool_user_coupon_coupon_used_1d"
    },
    {
        "sql": """
            SELECT
                province_id,
                province_name,
                area_code,
                iso_code,
                iso_3166_2,
                order_count_1d,
                order_original_amount_1d,
                activity_reduce_amount_1d,
                coupon_reduce_amount_1d,
                order_total_amount_1d,
                ds
            FROM
            (
                SELECT
                    province_id,
                    count(distinct(order_id)) order_count_1d,
                    sum(split_original_amount) order_original_amount_1d,
                    sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
                    sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
                    sum(split_total_amount) order_total_amount_1d,
                    ds
                FROM dwd_trade_order_detail_inc
                GROUP BY province_id, ds
            ) o
            LEFT JOIN
            (
                SELECT
                    id,
                    province_name,
                    area_code,
                    iso_code,
                    iso_3166_2
                FROM dim_province_full
                WHERE ds = '20250627'
            ) p
            ON o.province_id = p.id
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_trade_province_order_1d"
    },
    {
        "sql": """
            SELECT
                user_id,
                count(*),
                sum(sku_num),
                ds
            FROM dwd_trade_cart_add_inc
            GROUP BY user_id, ds
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_trade_user_cart_add_1d"
    },
    {
        "sql": """
            SELECT
                user_id,
                count(distinct(order_id)),
                sum(sku_num),
                sum(split_original_amount),
                sum(nvl(split_activity_amount,0)),
                sum(nvl(split_coupon_amount,0)),
                sum(split_total_amount),
                ds
            FROM dwd_trade_order_detail_inc
            GROUP BY user_id, ds
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_trade_user_order_1d"
    },
    {
        "sql": """
            SELECT
                user_id,
                count(distinct(order_id)),
                sum(sku_num),
                sum(split_payment_amount),
                ds
            FROM dwd_trade_pay_detail_suc_inc
            GROUP BY user_id, ds
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_trade_user_payment_1d"
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
                order_count_1d,
                order_num_1d,
                order_original_amount_1d,
                activity_reduce_amount_1d,
                coupon_reduce_amount_1d,
                order_total_amount_1d,
                ds
            FROM
            (
                SELECT
                    ds,
                    user_id,
                    sku_id,
                    count(*) order_count_1d,
                    sum(sku_num) order_num_1d,
                    sum(split_original_amount) order_original_amount_1d,
                    sum(nvl(split_activity_amount,0.0)) activity_reduce_amount_1d,
                    sum(nvl(split_coupon_amount,0.0)) coupon_reduce_amount_1d,
                    sum(split_total_amount) order_total_amount_1d
                FROM dwd_trade_order_detail_inc
                GROUP BY ds, user_id, sku_id
            ) od
            LEFT JOIN
            (
                SELECT
                    id,
                    sku_name,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    tm_id,
                    tm_name
                FROM dim_sku_full
                WHERE ds = '20250627'
            ) sku
            ON od.sku_id = sku.id
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_trade_user_sku_order_1d"
    },
    {
        "sql": """
            SELECT
                mid_id,
                brand,
                model,
                operate_system,
                page_id,
                sum(during_time),
                count(*)
            FROM dwd_traffic_page_view_inc
            WHERE ds = '2025-06-28'
            GROUP BY mid_id, brand, model, operate_system, page_id
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_traffic_page_visitor_page_view_1d"
    },
    {
        "sql": """
            SELECT
                session_id,
                mid_id,
                brand,
                model,
                operate_system,
                version_code,
                channel,
                sum(during_time),
                count(*)
            FROM dwd_traffic_page_view_inc
            WHERE ds = '2025-06-28'
            GROUP BY session_id, mid_id, brand, model, operate_system, version_code, channel
        """,
        "partition_date": "2025-06-28",
        "table_name": "dws_traffic_session_page_view_1d"
    }
]


for job in job_list:
    print(f"[INFO] 正在处理表：{job['table_name']}")
    ts.execute_hive_insert(job['sql'], job['partition_date'], job['table_name'])
    time.sleep(2)

