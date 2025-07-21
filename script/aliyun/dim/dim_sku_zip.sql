--MaxCompute SQL
--********************************************************************--
--author: 尉菁薪
--create time: 2025-07-20 19:36:42
--********************************************************************--

INSERT OVERWRITE TABLE dim_user_zip  partition(ds)
select
    id,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    head_img,
    user_level,
    cast(birthday as timestamp) birthday,
    gender,
    cast(create_time as timestamp) create_time,
    cast(operate_time as timestamp) operate_time,
    status,
    date_format(nvl(cast(operate_time as timestamp), cast(create_time as timestamp)),'yyyyMMdd') start_date,
    '99991231' end_date,
    '99991231' ds
from ods_user_info_di_init
where
        pt = '20220611';



SELECT * FROM dim_user_zip where ds = '99991231';




set odps.sql.hive.compatible=true;
INSERT OVERWRITE TABLE dim_user_zip  PARTITION (ds)
SELECT
    id,
    login_name,
    nick_name,
    `name`,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    `status`,
    `start_date`,
    IF(rn1=1,end_date,${ds}) end_date,
    IF(rn1=1,end_date,${ds}) ds
from (
         SELECT
             id,
             login_name,
             nick_name,
             `name`,
             phone_num,
             email,
             head_img,
             user_level,
             birthday,
             gender,
             create_time,
             operate_time,
             `status`,
             `start_date`,
             end_date,
             row_number() over (partition by id order by start_date desc ) rn1
         from (
                  SELECT
                      id,
                      login_name,
                      nick_name,
                      `name`,
                      phone_num,
                      email,
                      head_img,
                      user_level,
                      birthday,
                      gender,
                      create_time,
                      operate_time,
                      `status` ,
                      `start_date`,
                      end_date
                  from dim_user_zip
                  WHERE ds='99991231'
                  union
                  (SELECT
                       cast(id as string) id,
                       login_name,
                       nick_name,
                       `name`,
                       phone_num,
                       email,
                       head_img,
                       user_level,
                       birthday,
                       gender,
                       create_time,
                       operate_time,
                       `status` ,
                       `start_date` ,
                       end_date
                   from (
                            select
                                id,
                                login_name,
                                nick_name,
                                `name`,
                                phone_num,
                                email,
                                head_img,
                                user_level,
                                birthday,
                                gender,
                                create_time,
                                operate_time,
                                `status`,
                                row_number() over (partition by id order by create_time  desc ) rn,
                                    DATE_FORMAT(create_time,'yyyyMMdd') start_date,
                                '99991231' end_date
                            from ods_user_info_ri
                            where pt='2025'
                        )t1
                   WHERE rn=1)
              )t2
     )t3;






SELECT * FROM dim_user_zip WHERE ds = '20250720' ;
