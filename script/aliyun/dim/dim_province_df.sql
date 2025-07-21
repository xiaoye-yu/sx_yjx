--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-20 19:16:46
--********************************************************************--

set odps.sql.select.output.showcolumntype=true;
set odps.task.wlm.quota=os_PayAsYouGoQuota;
INSERT OVERWRITE TABLE gmall_space_maxcom.dim_province_di  PARTITION(ds)
select
    id,
    name,
    region_id,
    area_code,
    iso_code,
    iso_3166_2,
    pt
from ods_province_di
where
        pt = ${ds};


SELECT * FROM dim_province_di WHERE ds = '20220611';

