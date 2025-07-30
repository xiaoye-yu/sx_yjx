CREATE EXTERNAL TABLE IF NOT EXISTS test.ods_user_behavior (
  user_id STRING COMMENT '用户ID',  -- Hive中常用STRING类型存储字符串
  click_time TIMESTAMP COMMENT '点击时间',  -- 对应MySQL的DATETIME
  page_type STRING COMMENT '页面类型（首页/自定义承接页/商品详情页）',
  block_id STRING COMMENT '板块ID',
  channel STRING COMMENT '访问渠道（含直播间等需排除的渠道）',
  is_click TINYINT COMMENT '是否点击（1=是，0=否）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/test/ods_user_behavior'  -- 指定HDFS存储路径
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',  -- 压缩配置
  'comment' = '用户行为数据ODS层（同步自MySQL）'
);

CREATE EXTERNAL TABLE IF NOT EXISTS test.ods_page_structure (
  page_type STRING COMMENT '页面类型（如首页、商品详情页）',
  block_id STRING COMMENT '板块ID',
  block_name STRING COMMENT '板块名称（如轮播图、推荐商品区）',
  block_position STRING COMMENT '板块位置（如顶部、中部）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/test/ods_page_structure'  -- HDFS存储路径
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',  -- 启用SNAPPY压缩
  'comment' = '页面结构数据ODS层（存储页面各板块的基础信息）'
);

CREATE EXTERNAL TABLE IF NOT EXISTS test.ods_trade_data (
  trade_id STRING COMMENT '交易ID',
  user_id STRING COMMENT '用户ID（关联用户行为表ods_user_behavior的user_id）',
  pay_amount DECIMAL(10,2) COMMENT '支付金额',
  guide_block_id STRING COMMENT '引导支付的板块ID（关联页面结构表ods_page_structure的block_id）',
  pay_time TIMESTAMP COMMENT '支付时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION '/warehouse/test/ods_trade_data'
TBLPROPERTIES (
  'orc.compress' = 'SNAPPY',  -- 启用SNAPPY压缩
  'comment' = '交易数据ODS层（存储支付相关原始数据，关联用户行为和页面结构）'
);