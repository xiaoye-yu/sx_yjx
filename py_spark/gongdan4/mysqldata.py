import random
import pymysql
from faker import Faker
from datetime import date
import sys
import time
import uuid

# 数据库配置（根据实际环境修改）
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': '123456',
    'db': 'work_ord',
    'port': 3306,
    'charset': 'utf8mb4',
    'init_command': 'SET NAMES utf8mb4'
}

# 初始化 Faker
fake = Faker('zh_CN')

# 数据量分配
table_data = {
    'sales_full': 300000,
    'attribute_info': 300000,
    'traffic_detail': 200000,
    'user_behavior_full': 150000,
    'user_info': 50000  # 先生成5万用户，供行为表关联
}

# 公共数据配置（保持不变）
categories = [
    {'id': '1001', 'name': '女装', 'target_gmv': 5000000},
    {'id': '1002', 'name': '男装', 'target_gmv': 3000000},
    {'id': '1003', 'name': '数码', 'target_gmv': 10000000},
    {'id': '1004', 'name': '家居', 'target_gmv': 2000000},
    {'id': '1005', 'name': '美妆', 'target_gmv': 4000000}
]
category_name_templates = {
    '女装': ['连衣裙', 'T恤', '卫衣', '牛仔裤', '风衣'],
    '男装': ['衬衫', '夹克', '休闲裤', 'POLO衫', '羽绒服'],
    '数码': ['手机', '耳机', '充电宝', '键盘', '鼠标'],
    '家居': ['抱枕', '地毯', '收纳箱', '窗帘', '毛巾'],
    '美妆': ['口红', '面膜', '粉底液', '眼影', '防晒霜']
}
category_attributes = {
    '女装': {'颜色': ['红', '粉', '白', '黑', '紫'], '尺寸': ['S', 'M', 'L', 'XL'], '材质': ['棉', '雪纺', '丝绸']},
    '男装': {'颜色': ['黑', '灰', '藏青', '白', '蓝'], '尺寸': ['M', 'L', 'XL', 'XXL'], '材质': ['棉', '涤纶', '羊毛']},
    '数码': {'品牌': ['华为', '苹果', '小米', '罗技', '飞利浦']},
    '家居': {'颜色': ['米白', '深灰', '墨绿', '酒红', '驼色'], '材质': ['棉麻', '毛绒', '涤纶', '亚麻']},
    '美妆': {'色号': ['正红', '豆沙', '自然白', '大地色', '奶茶色']}
}
channels = ['搜索', '直通车', '推荐', '活动', '站外']
behaviors = ['搜索', '访问', '支付']
regions = ['北京', '上海', '广州', '深圳', '杭州', '成都', '武汉']
consumption_levels = ['低', '中', '高']

# 全局变量：存储生成的用户ID，供行为表使用
generated_user_ids = []


# 进度条函数（保持不变）
def show_progress(current, total, table):
    bar_length = 50
    percent = current / total
    filled = int(bar_length * percent)
    bar = '=' * filled + '-' * (bar_length - filled)
    sys.stdout.write(f'\r{table}: [{bar}] {int(percent * 100)}% ({current}/{total})')
    sys.stdout.flush()


# 数据库连接函数（保持不变）
def get_conn():
    return pymysql.connect(**db_config)


# 1. 写入 sales_full 表（保持不变）
def insert_sales_full():
    conn = get_conn()
    cursor = conn.cursor()
    start = time.time()
    batch_size = 1000
    sql = """
    INSERT INTO sales_full (item_id, item_name, category_id, category_name, pay_amount, 
                          pay_quantity, target_gmv, last_month_rank, create_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = []
    start_date = date(2025, 1, 1)
    end_date = date(2025, 1, 31)

    for i in range(table_data['sales_full']):
        cat = random.choice(categories)
        cat_name = cat['name']
        item_name = f'{cat_name}_{random.choice(category_name_templates[cat_name])}'

        data.append((
            f'item_{random.randint(10000, 99999)}',
            item_name,
            cat['id'],
            cat_name,
            round(random.uniform(10, 10000), 2),
            random.randint(1, 10),
            cat['target_gmv'],
            random.randint(1, 20),
            fake.date_between(start_date=start_date, end_date=end_date)
        ))
        if (i + 1) % batch_size == 0:
            cursor.executemany(sql, data)
            conn.commit()
            data = []
            show_progress(i + 1, table_data['sales_full'], 'sales_full')
    if data:
        cursor.executemany(sql, data)
        conn.commit()
    show_progress(table_data['sales_full'], table_data['sales_full'], 'sales_full')
    conn.close()
    print(f'\n耗时: {round(time.time() - start, 2)}秒')


# 2. 写入 attribute_info 表（保持不变）
def insert_attribute_info():
    conn = get_conn()
    cursor = conn.cursor()
    start = time.time()
    batch_size = 1000
    sql = """
    INSERT INTO attribute_info (item_id, category_id, category_name, attr_id, attr_value,
                              visitor_id, is_pay, visit_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = []
    start_date = date(2025, 1, 1)
    end_date = date(2025, 1, 31)

    for i in range(table_data['attribute_info']):
        cat = random.choice(categories)
        cat_name = cat['name']
        attrs = category_attributes[cat_name]
        attr_id, attr_values = random.choice(list(attrs.items()))
        attr_val = random.choice(attr_values)

        data.append((
            f'item_{random.randint(10000, 99999)}',
            cat['id'],
            cat_name,
            attr_id,
            attr_val,
            f'visitor_{random.randint(100000, 999999)}',
            random.randint(0, 1),
            fake.date_between(start_date=start_date, end_date=end_date)
        ))
        if (i + 1) % batch_size == 0:
            cursor.executemany(sql, data)
            conn.commit()
            data = []
            show_progress(i + 1, table_data['attribute_info'], 'attribute_info')
    if data:
        cursor.executemany(sql, data)
        conn.commit()
    show_progress(table_data['attribute_info'], table_data['attribute_info'], 'attribute_info')
    conn.close()
    print(f'\n耗时: {round(time.time() - start, 2)}秒')


# 3. 写入 traffic_detail 表（保持不变）
def insert_traffic_detail():
    conn = get_conn()
    cursor = conn.cursor()
    start = time.time()
    batch_size = 1000
    sql = """
    INSERT INTO traffic_detail (category_id, category_name, channel, search_word,
                              visitor_id, is_pay, visit_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    data = []
    start_date = date(2025, 1, 1)
    end_date = date(2025, 1, 31)

    for i in range(table_data['traffic_detail']):
        cat = random.choice(categories)
        cat_id = cat['id']
        cat_name = cat['name']
        channel = random.choice(channels)
        if channel == '搜索':
            base_keyword = random.choice(category_name_templates[cat_name])
            xs = random.choice(['新款', '推荐', '测评', '购买', '怎么样', '价格'])
            search_word = f'{cat_name}_{base_keyword}_{xs}'
        else:
            search_word = None

        data.append((
            cat_id,
            cat_name,
            channel,
            search_word,
            f'visitor_{random.randint(100000, 999999)}',
            random.randint(0, 1),
            fake.date_between(start_date=start_date, end_date=end_date)
        ))
        if (i + 1) % batch_size == 0:
            cursor.executemany(sql, data)
            conn.commit()
            data = []
            show_progress(i + 1, table_data['traffic_detail'], 'traffic_detail')
    if data:
        cursor.executemany(sql, data)
        conn.commit()
    show_progress(table_data['traffic_detail'], table_data['traffic_detail'], 'traffic_detail')
    conn.close()
    print(f'\n耗时: {round(time.time() - start, 2)}秒')


# 4. 写入 user_behavior_full 表（修改：使用已生成的user_id）
def insert_user_behavior_full():
    # 确保先执行user_info生成，否则会报错
    if not generated_user_ids:
        print("错误：请先执行insert_user_info生成用户数据")
        return

    conn = get_conn()
    cursor = conn.cursor()
    start = time.time()
    batch_size = 1000
    sql = """
    INSERT INTO user_behavior_full (user_id, category_id, category_name, behavior_type, behavior_time)
    VALUES (%s, %s, %s, %s, %s)
    """
    data = []
    start_date = date(2025, 1, 1)
    end_date = date(2025, 1, 31)

    for i in range(table_data['user_behavior_full']):
        cat = random.choice(categories)
        # 从已生成的用户ID中随机选择一个
        user_id = random.choice(generated_user_ids)

        data.append((
            user_id,  # 使用关联的user_id
            cat['id'],
            cat['name'],
            random.choice(behaviors),
            fake.date_between(start_date=start_date, end_date=end_date)
        ))
        if (i + 1) % batch_size == 0:
            cursor.executemany(sql, data)
            conn.commit()
            data = []
            show_progress(i + 1, table_data['user_behavior_full'], 'user_behavior_full')
    if data:
        cursor.executemany(sql, data)
        conn.commit()
    show_progress(table_data['user_behavior_full'], table_data['user_behavior_full'], 'user_behavior_full')
    conn.close()
    print(f'\n耗时: {round(time.time() - start, 2)}秒')


# 5. 写入 user_info 表（修改：记录生成的user_id）
def insert_user_info():
    global generated_user_ids
    generated_user_ids = []  # 清空历史数据
    conn = get_conn()
    cursor = conn.cursor()
    start = time.time()
    batch_size = 1000
    # 新增user_id字段（如果表结构没有的话，需要先在数据库中添加）
    sql = """
    INSERT INTO user_info (user_id, age, gender, region, consumption_level)
    VALUES (%s, %s, %s, %s, %s)
    """
    data = []

    for i in range(table_data['user_info']):
        # 生成唯一user_id并记录
        user_id = f'user_{uuid.uuid4().hex[:8]}'
        generated_user_ids.append(user_id)

        data.append((
            user_id,  # 存储user_id
            random.randint(18, 60),
            random.choice(['男', '女']),
            random.choice(regions),
            random.choice(consumption_levels)
        ))
        if (i + 1) % batch_size == 0:
            cursor.executemany(sql, data)
            conn.commit()
            data = []
            show_progress(i + 1, table_data['user_info'], 'user_info')
    if data:
        cursor.executemany(sql, data)
        conn.commit()
    show_progress(table_data['user_info'], table_data['user_info'], 'user_info')
    conn.close()
    print(f'\n已生成 {len(generated_user_ids)} 个用户ID，耗时: {round(time.time() - start, 2)}秒')


# 执行写入（调整顺序：先写user_info，再写user_behavior_full）
if __name__ == '__main__':
    total_start = time.time()
    print("开始写入数据...")
    # 调整执行顺序：确保user_info先执行
    insert_sales_full()
    insert_attribute_info()
    insert_traffic_detail()
    insert_user_info()  # 先生成用户
    insert_user_behavior_full()  # 再生成关联的行为数据
    print(
        f'\n所有表数据写入完成！总数据量: {sum(table_data.values())}条，总耗时: {round(time.time() - total_start, 2)}秒')