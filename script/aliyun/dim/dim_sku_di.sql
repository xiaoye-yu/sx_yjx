--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-19 11:54:55
--********************************************************************--

insert OVERWRITE TABLE dim_sku_df  PARTITION (ds)
select
    id,
    spu_id,
    CAST(price AS DOUBLE),
    sku_name,
    sku_desc,
    CAST(weight AS DOUBLE),
    tm_id,
    category3_id,
    is_sale,
    create_time,
    operate_time,
    pt
from ods_sku_di
where
 pt = ${ds}
;



SELECT * FROM dim_sku_df  WHERE ds = '20220611';