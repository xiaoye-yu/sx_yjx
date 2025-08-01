from pyspark.sql import SparkSession

def read_doris_with_jdbc_fixed():
    # 驱动路径：必须是你实际存放jar包的位置，无中文无空格


    # 初始化SparkSession，强制指定驱动路径
    spark = SparkSession.builder \
        .appName("Doris-JDBC-Fixed") \
        .getOrCreate()

    # JDBC连接参数（使用Doris的MySQL协议端口9030）
    jdbc_params = {
        "url": "jdbc:mysql://cdh01:9030/test_yi?useSSL=false&serverTimezone=Asia/Shanghai",
        "dbtable": "(SELECT * FROM ods_wireless_instore_log LIMIT 5) t",  # 只查前5条
        "user": "root",
        "password": "123456",
        "driver": "com.mysql.cj.jdbc.Driver"  # 驱动类名
    }

    try:
        # 读取数据
        df = spark.read \
            .format("jdbc") \
            .options(** jdbc_params) \
            .load()

        # 显示结果
        print("✅ 成功读取前5条数据：")
        df.show(truncate=False)
        print("\n表结构：")
        df.printSchema()

    except Exception as e:
        print(f"❌ 错误：{str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    read_doris_with_jdbc_fixed()