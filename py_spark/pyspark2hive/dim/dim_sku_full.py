from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gmall")
    return spark


def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"gmall.{tableName}")


# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    select_sql = f"""
with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ods_sku_info
    where ds='20250627'
),
spu as
(
    select
        id,
        spu_name
    from ods_spu_info
    where ds='20250627'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ods_base_category3
    where ds='20250627'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ods_base_category2
    where ds='20250627'
),
c1 as
(
    select
        id,
        name
    from ods_base_category1
    where ds='20250627'
),
tm as
(
    select
        id,
        tm_name
    from ods_base_trademark
    where ds='20250627'
),
attr as (
    select
        sku_id,
        collect_set(
            named_struct(
                'attr_id', CAST(attr_id AS STRING),
                'value_id', CAST(value_id AS STRING),
                'attr_name', attr_name,
                'value_name', value_name
            )
        ) as attrs
    from ods_sku_attr_value
    where ds='20250627'
    group by sku_id
),
sale_attr as (
    select
        sku_id,
        collect_set(
            named_struct(
                'sale_attr_id', CAST(sale_attr_id AS STRING),  -- 若目标表是 string 就转
                'sale_attr_value_id', CAST(sale_attr_value_id AS STRING),
                'sale_attr_name', sale_attr_name,
                'sale_attr_value_name', sale_attr_value_name
            )
        ) as sale_attrs
    from ods_sku_sale_attr_value
    where ds='20250627'
    group by sku_id
)
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 4. 主函数（示例调用）
if __name__ == "__main__":
    # 设置目标分区日期
    target_date = '2025-06-27'

    # 执行插入操作
    execute_hive_insert(target_date, 'dim_sku_full')