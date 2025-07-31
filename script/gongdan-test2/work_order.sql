DROP TABLE IF EXISTS ods_wireless_instore_log;
CREATE TABLE IF NOT EXISTS ods_wireless_instore_log (
    visitor_id VARCHAR(100) COMMENT '访客唯一标识（如CookieID/用户ID）',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    page_type VARCHAR(50) COMMENT '入店页面类型（如首页、活动页、分类页、商品详情页等）',
    visit_time DATETIME NOT NULL COMMENT '入店时间（精确到秒）',
    is_buy TINYINT COMMENT '是否下单（0=未下单，1=下单，用于计算下单买家数）',
    dt DATE NOT NULL COMMENT '日期分区，格式yyyy-MM-dd'
) ENGINE=OLAP
DUPLICATE KEY(`visitor_id`, `store_id`)
COMMENT '无线端入店原始日志表，记录无线端访客入店页面及下单行为'
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD"
);

# 2. 页面访问明细日志表
DROP TABLE IF EXISTS ods_page_visit_log;
CREATE TABLE IF NOT EXISTS ods_page_visit_log (
    visitor_id VARCHAR(100) COMMENT '访客唯一标识',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    page_category VARCHAR(50) COMMENT '页面大类（店铺页/商品详情页/店铺其他页）',
    page_subtype VARCHAR(50) COMMENT '页面细分类型（如店铺页下的首页、活动页、分类页等）',
    visit_time DATETIME NOT NULL COMMENT '访问开始时间',
    leave_time DATETIME COMMENT '访问结束时间',
    stay_time INT COMMENT '停留时长（秒，leave_time - visit_time计算所得，冗余存储）',
    dt DATE NOT NULL COMMENT '日期分区，格式yyyy-MM-dd'
) ENGINE=OLAP
DUPLICATE KEY(`visitor_id`, `store_id`)
COMMENT '页面访问原始日志表，记录所有页面的访问明细及停留信息'
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD"
);

# 3. 店内流转路径日志表
DROP TABLE IF EXISTS ods_instore_flow_log;
CREATE TABLE IF NOT EXISTS ods_instore_flow_log (
    visitor_id VARCHAR(100) COMMENT '访客唯一标识',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    source_page VARCHAR(100) COMMENT '来源页面（跳转前的页面）',
    target_page VARCHAR(100) COMMENT '去向页面（跳转后的页面）',
    jump_time DATETIME NOT NULL COMMENT '跳转时间（精确到秒）',
    dt DATE NOT NULL COMMENT '日期分区，格式yyyy-MM-dd'
) ENGINE=OLAP
DUPLICATE KEY(`visitor_id`, `store_id`)
COMMENT '店内页面流转原始日志表，记录访客在店内的页面跳转路径'
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD"
);


# 4. PC 端流量入口日志表
DROP TABLE IF EXISTS ods_pc_entry_log;
CREATE TABLE IF NOT EXISTS ods_pc_entry_log (
    visitor_id VARCHAR(100) COMMENT '访客唯一标识',
    store_id VARCHAR(50) NOT NULL COMMENT '店铺ID',
    source_page VARCHAR(100) COMMENT 'PC端访客的来源页面（如搜索引擎、外部链接、站内其他页面等）',
    visit_time DATETIME NOT NULL COMMENT '访问时间',
    dt DATE NOT NULL COMMENT '日期分区，格式yyyy-MM-dd'
) ENGINE=OLAP
DUPLICATE KEY(`visitor_id`, `store_id`)
COMMENT 'PC端流量入口原始日志表，记录PC端访客的来源页面信息'
PARTITION BY RANGE(`dt`) (
    PARTITION p202507 VALUES LESS THAN ('2025-08-01')
)
DISTRIBUTED BY HASH(`store_id`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "storage_medium" = "HDD"
);
