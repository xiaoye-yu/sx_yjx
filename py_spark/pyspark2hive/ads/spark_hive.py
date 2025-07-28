import pyspark2hive.ads_spark_hive as ts

job_list = [
    {
        "sql": """
            select * from ads_coupon_stats
            union
            select
                '2025-06-27' dt,
                coupon_id,
                coupon_name,
                cast(sum(used_count_1d) as bigint),
                cast(count(*) as bigint)
            from dws_tool_user_coupon_coupon_used_1d
            where ds='2025-06-25'
            group by coupon_id,coupon_name
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_coupon_stats"
    },
    {
        "sql": """
            select * from ads_order_by_province
            union
            select
                '2025-06-27' dt,
                1 recent_days,
                province_id,
                province_name,
                area_code,
                iso_code,
                iso_3166_2,
                order_count_1d,
                order_total_amount_1d
            from dws_trade_province_order_1d
            where ds='2025-06-27'
            union
            select
                '2025-06-27' as ds,
                recent_days,
                province_id,
                province_name,
                area_code,
                iso_code,
                iso_3166_2,
                case recent_days
                    when 7 then order_count_7d
                    when 30 then order_count_30d
                end order_count,
                case recent_days
                    when 7 then order_total_amount_7d
                    when 30 then order_total_amount_30d
                end order_total_amount
            from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
            where ds='2025-06-27'
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_order_by_province"
    },
    {
        "sql": """
            select * from ads_order_continuously_user_count
            union
            select
                '2025-06-27',
                7,
                count(distinct(user_id))
            from
            (
                select
                    user_id,
                    datediff(lead(ds,2,'9999-12-31') over(partition by user_id order by ds),ds) diff
                from dws_trade_user_order_1d
                where ds>=date_add('2025-06-27',-6)
            )t1
            where diff=2
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_order_continuously_user_count"
    },
    {
        "sql": """
            select * from ads_order_stats_by_cate
            union
            select
                '2025-06-27' dt,
                recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                order_count,
                order_user_count
            from
            (
                select
                    1 recent_days,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    sum(order_count_1d) order_count,
                    count(distinct(user_id)) order_user_count
                from dws_trade_user_sku_order_1d
                where ds='2025-06-27'
                group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
                union all
                select
                    recent_days,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    sum(order_count),
                    count(distinct(if(order_count>0,user_id,null)))
                from
                (
                    select
                        recent_days,
                        user_id,
                        category1_id,
                        category1_name,
                        category2_id,
                        category2_name,
                        category3_id,
                        category3_name,
                        case recent_days
                            when 7 then order_count_7d
                            when 30 then order_count_30d
                        end order_count
                    from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                    where ds='2025-06-27'
                )t1
                group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
            )odr
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_order_stats_by_cate"
    },
    {
        "sql": """
            select * from ads_order_stats_by_tm
            union
            select
                '2025-06-27' ds,
                recent_days,
                tm_id,
                tm_name,
                order_count,
                order_user_count
            from
            (
                select
                    1 recent_days,
                    tm_id,
                    tm_name,
                    sum(order_count_1d) order_count,
                    count(distinct(user_id)) order_user_count
                from dws_trade_user_sku_order_1d
                where ds='2025-06-27'
                group by tm_id,tm_name
                union all
                select
                    recent_days,
                    tm_id,
                    tm_name,
                    sum(order_count),
                    count(distinct(if(order_count>0,user_id,null)))
                from
                (
                    select
                        recent_days,
                        user_id,
                        tm_id,
                        tm_name,
                        case recent_days
                            when 7 then order_count_7d
                            when 30 then order_count_30d
                        end order_count
                    from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                    where ds='2025-06-27'
                )t1
                group by recent_days,tm_id,tm_name
            )odr
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_order_stats_by_tm"
    },
    {
        "sql": """
            select * from ads_order_to_pay_interval_avg
            union
            select
                '2025-06-27',
                cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
            from dwd_trade_trade_flow_acc
            where ds in ('9999-12-31','2025-06-27')
            and payment_date_id='2025-06-27'
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_order_to_pay_interval_avg"
    },
    {
        "sql": """
            select * from ads_page_path
            union
            select
                '2025-06-27' ds,
                source,
                nvl(target,'null'),
                count(*) path_count
            from
            (
                select
                    concat('step-',rn,':',page_id) source,
                    concat('step-',rn+1,':',next_page_id) target
                from
                (
                    select
                        page_id,
                        lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
                        row_number() over (partition by session_id order by view_time) rn
                    from dwd_traffic_page_view_inc
                    where ds='2025-06-27'
                )t1
            )t2
            group by source,target
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_page_path"
    },
    {
        "sql": """
            select * from ads_repeat_purchase_by_tm
            union
            select
                '2025-06-27',
                30,
                tm_id,
                tm_name,
                cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
            from
            (
                select
                    user_id,
                    tm_id,
                    tm_name,
                    sum(order_count_30d) order_count
                from dws_trade_user_sku_order_nd
                where ds='2025-06-27'
                group by user_id, tm_id,tm_name
            )t1
            group by tm_id,tm_name
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_repeat_purchase_by_tm"
    },
    {
        "sql": """
            select * from ads_sku_cart_num_top3_by_cate
            union
            select
                '2025-06-27' ds,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sku_id,
                sku_name,
                cart_num,
                rk
            from
            (
                select
                    sku_id,
                    sku_name,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    cart_num,
                    rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
                from
                (
                    select
                        sku_id,
                        sum(sku_num) cart_num
                    from dwd_trade_cart_full
                    where ds='2025-06-27'
                    group by sku_id
                )cart
                left join
                (
                    select
                        id,
                        sku_name,
                        category1_id,
                        category1_name,
                        category2_id,
                        category2_name,
                        category3_id,
                        category3_name
                    from dim_sku_full
                    where ds='20250627'
                )sku
                on cart.sku_id=sku.id
            )t1
            where rk<=3
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_sku_cart_num_top3_by_cate"
    },
    {
        "sql": """
            select * from ads_sku_favor_count_top3_by_tm
            union
            select
                '2025-06-27' ds,
                tm_id,
                tm_name,
                sku_id,
                sku_name,
                favor_count,
                rk
            from
            (
                select
                    tm_id,
                    tm_name,
                    sku_id,
                    sku_name,
                    favor_add_count_1d,
                    rank() over (partition by tm_id order by favor_add_count_1d desc) rk
                from dws_interaction_sku_favor_add_1d
                where ds='20250627'
            )t1
            where rk<=3
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_sku_favor_count_top3_by_tm"
    },
    {
        "sql": """
            select * from ads_traffic_stats_by_channel
            union
            select
                '2025-06-27' as ds,
                recent_days,
                channel,
                cast(count(distinct(mid_id)) as bigint) uv_count,
                cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
                cast(avg(page_count_1d) as bigint) avg_page_count,
                cast(count(*) as bigint) sv_count,
                cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
            from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
            where ds>=date_add('2025-06-27',-recent_days+1)
            group by recent_days,channel
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_traffic_stats_by_channel"
    },
    {
        "sql": """
            select * from ads_user_action
            union
            select
                '2025-06-27' ds,
                home_count,
                good_detail_count,
                cart_count,
                order_count,
                payment_count
            from
            (
                select
                    1 recent_days,
                    sum(if(page_id='home',1,0)) home_count,
                    sum(if(page_id='good_detail',1,0)) good_detail_count
                from dws_traffic_page_visitor_page_view_1d
                where ds='2025-06-27'
                and page_id in ('home','good_detail')
            )page
            join
            (
                select
                    1 recent_days,
                    count(*) cart_count
                from dws_trade_user_cart_add_1d
                where ds='2025-06-27'
            )cart
            on page.recent_days=cart.recent_days
            join
            (
                select
                    1 recent_days,
                    count(*) order_count
                from dws_trade_user_order_1d
                where ds='2025-06-27'
            )ord
            on page.recent_days=ord.recent_days
            join
            (
                select
                    1 recent_days,
                    count(*) payment_count
                from dws_trade_user_payment_1d
                where ds='2025-06-27'
            )pay
            on page.recent_days=pay.recent_days
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_user_action"
    },
    {
        "sql": """
            select * from ads_user_change
            union
            select
                churn.ds,
                user_churn_count,
                user_back_count
            from
            (
                select
                    '2025-06-27' ds,
                    count(*) user_churn_count
                from dws_user_user_login_td
                where ds='2025-06-27'
                and login_date_last=date_add('2025-06-27',-7)
            )churn
            join
            (
                select
                    '2025-06-27' dt,
                    count(*) user_back_count
                from
                (
                    select
                        user_id,
                        login_date_last
                    from dws_user_user_login_td
                    where ds='2025-06-27'
                    and login_date_last = '2025-06-27'
                )t1
                join
                (
                    select
                        user_id,
                        login_date_last login_date_previous
                    from dws_user_user_login_td
                    where ds=date_add('2025-06-27',-1)
                )t2
                on t1.user_id=t2.user_id
                where datediff(login_date_last,login_date_previous)>=8
            )back
            on churn.ds=back.dt
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_user_change"
    },
    {
        "sql": """
            select * from ads_user_retention
            union
            select '2025-06-27' ds,
                   login_date_first create_date,
                   datediff('2025-06-27', login_date_first) retention_day,
                   sum(if(login_date_last = '2025-06-27', 1, 0)) retention_count,
                   count(*) new_user_count,
                   cast(sum(if(login_date_last = '2025-06-27', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
            from (
                     select user_id,
                            login_date_last,
                            login_date_first
                     from dws_user_user_login_td
                     where ds = '2025-06-27'
                       and login_date_first >= date_add('2025-06-27', -7)
                       and login_date_first < '2025-06-27'
                 ) t1
            group by login_date_first
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_user_retention"
    },
    {
        "sql": """
            select * from ads_user_stats
            union
            select '2025-06-27' as ds,
                   recent_days,
                   sum(if(login_date_first >= date_add('2025-06-27', -recent_days + 1), 1, 0)) new_user_count,
                   count(*) active_user_count
            from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
            where ds = '2025-06-27'
              and login_date_last >= date_add('2025-06-27', -recent_days + 1)
            group by recent_days
        """,
        "partition_date": "2025-06-27",
        "table_name": "ads_user_stats"
    }
]

for job in job_list:
    print(f"[INFO] 正在处理表：{job['table_name']}")
    ts.execute_hive_insert(job['sql'], job['partition_date'], job['table_name'])
