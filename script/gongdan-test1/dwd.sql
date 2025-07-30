-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
drop table dwd_user_page_behavior_detail;

CREATE EXTERNAL TABLE IF NOT EXISTS test.dwd_user_page_behavior_detail (
  user_id STRING COMMENT '用户ID（来自ods_user_behavior）',
  click_time TIMESTAMP COMMENT '点击时间（来自ods_user_behavior）',
  page_type STRING COMMENT '页面类型（首页/自定义承接页/商品详情页，来自ods_user_behavior）',
  block_id STRING COMMENT '板块ID（来自ods_user_behavior）',
  block_name STRING COMMENT '板块名称（来自ods_page_structure）',
  block_position STRING COMMENT '板块位置（来自ods_page_structure）',
  channel STRING COMMENT '访问渠道（来自ods_user_behavior）',
  is_click TINYINT COMMENT '是否点击（1=是，0=否，来自ods_user_behavior）',
  is_exclude_channel TINYINT COMMENT '是否为需排除的渠道（1=是，对应文档中直播间等）',
  trade_id STRING COMMENT '交易ID（来自ods_trade_data，无交易则为NULL）',
  pay_amount DECIMAL(10,2) COMMENT '支付金额（来自ods_trade_data，无交易则为0）',
  pay_time TIMESTAMP COMMENT '支付时间（来自ods_trade_data，无交易则为NULL）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，与ODS层一致（yyyyMMdd）')
STORED AS ORC
LOCATION '/warehouse/test/dwd/dwd_user_page_behavior_detail'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',
  'comment' = '用户页面行为明细宽表，父表为ODS层3张表'
);
-- 数据导入
INSERT OVERWRITE TABLE test.dwd_user_page_behavior_detail PARTITION (ds = ${pt})
SELECT
  u.user_id,
  u.click_time,
  u.page_type,
  u.block_id,
  s.block_name,
  s.block_position,
  u.channel,
  u.is_click,
  CASE WHEN u.channel IN ('直播间','短视频','图文','微详情') THEN 1 ELSE 0 END AS is_exclude_channel,
  t.trade_id,
  COALESCE(t.pay_amount, 0) AS pay_amount,
  t.pay_time
FROM test.ods_user_behavior u
-- 关联页面结构表获取板块信息
LEFT JOIN test.ods_page_structure s
  ON u.page_type = s.page_type
  AND u.block_id = s.block_id
  AND u.ds = s.ds
-- 关联交易表获取支付信息
LEFT JOIN test.ods_trade_data t
  ON u.user_id = t.user_id
  AND u.block_id = t.guide_block_id
  AND DATE(u.click_time) = DATE(t.pay_time)  -- 点击与支付同一天
  AND u.ds = t.ds
WHERE u.ds = ${pt}
  AND u.page_type IN ('首页','自定义承接页','商品详情页');


-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE test.dwd_user_page_behavior_detail PARTITION (ds)
SELECT
  u.user_id,
  u.click_time,
  u.page_type,
  u.block_id,
  s.block_name,
  s.block_position,
  u.channel,
  u.is_click,
  CASE WHEN u.channel IN ('直播间','短视频','图文','微详情') THEN 1 ELSE 0 END AS is_exclude_channel,
  t.trade_id,
  COALESCE(t.pay_amount, 0),
  t.pay_time,
    u.ds
FROM test.ods_user_behavior u
LEFT JOIN test.ods_page_structure s
  ON u.page_type = s.page_type AND u.block_id = s.block_id AND u.ds = s.ds
LEFT JOIN test.ods_trade_data t
  ON u.user_id = t.user_id AND u.block_id = t.guide_block_id AND u.ds = t.ds
WHERE u.page_type IN ('首页','自定义承接页','商品详情页');



