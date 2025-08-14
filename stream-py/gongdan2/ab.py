import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import time

# 初始化Faker
fake = Faker('zh_CN')

# 数据库连接配置
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': '123456',
    'database': 'gmall_ins',
    'port': 3306
}


# 连接数据库
def get_db_connection():
    return mysql.connector.connect(**db_config)


# 生成随机日期
def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)


# 生成商品分类数据
def generate_product_categories(num=100):
    categories = []
    parent_ids = ['0']  # 顶级分类的父ID

    # 生成一级分类
    first_level_cats = ['电子产品', '服装鞋帽', '食品饮料', '家居用品', '美妆个护', '母婴用品', '图书音像', '运动户外']
    for i, cat_name in enumerate(first_level_cats):
        cat_id = f"cat{i + 1:03d}"
        categories.append({
            'category_id': cat_id,
            'category_name': cat_name,
            'parent_category_id': '0',
            'level': 1
        })
        parent_ids.append(cat_id)

    # 生成二级和三级分类
    for i in range(len(first_level_cats), num):
        cat_id = f"cat{i + 1:03d}"
        parent_id = random.choice(parent_ids)
        level = 2 if parent_id == '0' else 3
        categories.append({
            'category_id': cat_id,
            'category_name': fake.word() + '类',
            'parent_category_id': parent_id,
            'level': level
        })
        if level == 2:
            parent_ids.append(cat_id)

    return categories[:num]


# 生成店铺数据
def generate_shops(num=200):
    shops = []
    business_scopes = ['综合零售', '电子产品专卖', '服装销售', '食品销售', '家居用品', '美妆护肤', '母婴用品',
                       '图书文具']

    for i in range(num):
        shop_id = f"shop{i + 1:05d}"
        shops.append({
            'shop_id': shop_id,
            'shop_name': fake.company() + '店',
            'business_scope': random.choice(business_scopes),
            'opening_date': random_date(datetime(2018, 1, 1), datetime(2023, 1, 1))
        })

    return shops


# 生成商品基础数据
def generate_products(num=3000, categories=None, shops=None):
    products = []
    product_categories = [cat['category_name'] for cat in categories]
    brands = ['轩妈家', '三只松鼠', '华为', '小米', '苹果', '耐克', '阿迪达斯', '美的', '海尔', '格力',
              '欧莱雅', '兰蔻', '好孩子', '乐高', '九阳', '苏泊尔', '恒源祥', '南极人']

    for i in range(num):
        product_id = f"prod{i + 1:06d}"
        category = random.choice(categories)
        shop = random.choice(shops)

        products.append({
            'product_id': product_id,
            'product_name': fake.word() + random.choice(['', '新款', '升级版', '特惠装']),
            'product_category': category['category_name'],
            'brand': random.choice(brands),
            'shop_id': shop['shop_id'],
            'launch_date': random_date(datetime(2020, 1, 1), datetime(2023, 12, 31)).strftime('%Y-%m-%d'),
            'original_price': round(random.uniform(9.9, 9999.99), 2),
            'status': random.choice(['在售', '下架']),
            'create_time': datetime.now()
        })

    return products


# 生成SKU数据
def generate_skus(num=8000, products=None):
    skus = []
    colors = ['红色', '蓝色', '黑色', '白色', '绿色', '黄色', '粉色', '灰色']
    sizes = ['S', 'M', 'L', 'XL', 'XXL', '均码', '160', '165', '170', '175', '180']

    for i in range(num):
        sku_id = f"sku{i + 1:07d}"
        product = random.choice(products)

        # 随机生成SKU信息（颜色+尺寸组合）
        has_color = random.choice([True, False])
        has_size = random.choice([True, False])

        sku_info_parts = []
        if has_color:
            sku_info_parts.append(random.choice(colors))
        if has_size:
            sku_info_parts.append(random.choice(sizes))

        if not sku_info_parts:
            sku_info_parts.append(f"规格{i % 5 + 1}")

        sku_info = '-'.join(sku_info_parts)

        skus.append({
            'sku_id': sku_id,
            'product_id': product['product_id'],
            'sku_info': sku_info
        })

    return skus


# 生成价格力数据
def generate_price_strengths(products=None):
    strengths = []
    levels = ['优秀', '良好', '较差']
    price_bands = ['0-50元', '50-100元', '100-300元', '300-500元', '500-1000元', '1000元以上']

    for product in products:
        # 随机决定是否生成该商品的价格力数据（约80%的商品有价格力数据）
        if random.random() < 0.8:
            # 券后价略低于原价
            coupon_price = round(product['original_price'] * random.uniform(0.7, 0.98), 2)

            # 同类目均价基于原价上下浮动
            same_category_avg = round(product['original_price'] * random.uniform(0.8, 1.2), 2)

            strengths.append({
                'product_id': product['product_id'],
                'price_strength_level': random.choice(levels),
                'coupon_price': coupon_price,
                'price_band': random.choice(price_bands),
                'same_category_avg_price': same_category_avg,
                'update_time': random_date(datetime(2023, 1, 1), datetime.now())
            })

    return strengths


# 生成搜索词数据
def generate_search_words(num=10000, products=None):
    search_words = []
    base_words = ['新款', '特价', '优惠', '正品', '官方', '爆款', '热卖', '推荐', '好用', '耐用']

    for i in range(num):
        product = random.choice(products)

        # 搜索词可以是品牌、商品名或组合
        word_type = random.choice(['brand', 'product', 'combined', 'base'])

        if word_type == 'brand':
            search_word = product['brand']
        elif word_type == 'product':
            search_word = product['product_name']
        elif word_type == 'combined':
            search_word = f"{product['brand']} {product['product_name']}"
        else:
            search_word = f"{random.choice(base_words)} {random.choice([product['brand'], product['product_name']])}"

        search_count = random.randint(10, 500)
        click_count = random.randint(1, search_count // 2)
        visitor_count = random.randint(1, click_count)

        search_words.append({
            'search_word': search_word,
            'product_id': product['product_id'],
            'search_count': search_count,
            'click_count': click_count,
            'visitor_count': visitor_count
        })

    return search_words


# 生成流量来源数据
def generate_traffic_sources(num=8000, products=None):
    traffic_sources = []
    sources = ['效果广告', '手淘搜索', '京东搜索', '拼多多搜索', '微信小程序', '直播带货', '短视频推荐',
               '朋友推荐', '百度搜索', 'APP首页', '分类导航', '活动页面', '购物车']

    for i in range(num):
        product = random.choice(products)
        source = random.choice(sources)

        visitor_count = random.randint(10, 500)
        pay_buyer_count = random.randint(0, visitor_count // 2)
        click_count = random.randint(visitor_count, visitor_count * 3)
        stay_duration = random.randint(10, 300)

        traffic_sources.append({
            'product_id': product['product_id'],
            'source_name': source,
            'visitor_count': visitor_count,
            'pay_buyer_count': pay_buyer_count,
            'click_count': click_count,
            'stay_duration': stay_duration
        })

    return traffic_sources


# 生成商品预警数据
def generate_product_warnings(num=2000, products=None):
    warnings = []
    warning_types = ['价格力预警', '商品力预警', '库存预警', '质量预警']
    reasons = {
        '价格力预警': ['价格高于同类目均价30%以上', '持续低星评价', '价格波动过大', '优惠券力度不足'],
        '商品力预警': ['转化率低于市场平均', '评价星级下降', '退货率过高', '投诉量增加'],
        '库存预警': ['库存不足', '库存积压', '库存周转率低', '临期商品过多'],
        '质量预警': ['质量投诉增加', '抽检不合格', '存在安全隐患', '用料问题']
    }
    levels = ['一般', '严重']
    handle_statuses = ['未处理', '已处理']

    for i in range(num):
        product = random.choice(products)
        warning_type = random.choice(warning_types)

        warnings.append({
            'product_id': product['product_id'],
            'warning_type': warning_type,
            'warning_reason': random.choice(reasons[warning_type]),
            'warning_level': random.choice(levels),
            'handle_status': random.choice(handle_statuses)
        })

    return warnings


# 批量插入数据
def batch_insert(table, data, batch_size=1000):
    start_time = time.time()
    conn = get_db_connection()
    cursor = conn.cursor()

    total = len(data)
    inserted = 0

    while inserted < total:
        batch = data[inserted:inserted + batch_size]
        if not batch:
            break

        # 获取字段名
        fields = batch[0].keys()
        placeholders = ', '.join(['%s'] * len(fields))
        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"

        # 准备数据
        values = []
        for item in batch:
            row = [item[field] for field in fields]
            values.append(tuple(row))

        # 执行插入
        cursor.executemany(sql, values)
        conn.commit()

        inserted += len(batch)
        print(f"插入 {table}: {inserted}/{total} 条数据，耗时: {time.time() - start_time:.2f}秒")

    cursor.close()
    conn.close()
    print(f"完成 {table} 数据插入，总耗时: {time.time() - start_time:.2f}秒\n")


# 生成带日期的数据并插入（如销售数据、趋势数据等）
def generate_and_insert_time_series_data(table, products, skus=None, days=30, daily_avg=100):
    start_time = time.time()
    conn = get_db_connection()
    cursor = conn.cursor()

    start_date = datetime.now() - timedelta(days=days)
    total = 0

    for day in range(days):
        stat_date = start_date + timedelta(days=day)
        daily_count = random.randint(int(daily_avg * 0.7), int(daily_avg * 1.3))

        # 为随机选择的商品生成当日数据
        sampled_products = random.sample(products, min(daily_count, len(products)))

        for product in sampled_products:
            if table == 'product_sales_daily':
                # 生成商品每日销售数据
                visitor_count = random.randint(50, 2000)
                pay_buyer_count = random.randint(1, visitor_count // 5)
                pay_conversion_rate = round(pay_buyer_count / visitor_count, 4) if visitor_count > 0 else 0

                sales_quantity = random.randint(pay_buyer_count, pay_buyer_count * 5)
                sales_amount = round(sales_quantity * product['original_price'] * random.uniform(0.8, 1.0), 2)

                data = {
                    'product_id': product['product_id'],
                    'stat_date': stat_date,
                    'sales_amount': sales_amount,
                    'sales_quantity': sales_quantity,
                    'visitor_count': visitor_count,
                    'pay_buyer_count': pay_buyer_count,
                    'pay_conversion_rate': pay_conversion_rate,
                    'add_cart_count': random.randint(pay_buyer_count, visitor_count),
                    'collect_count': random.randint(0, visitor_count // 10),
                    'refund_count': random.randint(0, sales_quantity // 20)
                }

            elif table == 'sku_sales_inventory_daily' and skus:
                # 为商品选择一个SKU
                product_skus = [s for s in skus if s['product_id'] == product['product_id']]
                if not product_skus:
                    continue
                sku = random.choice(product_skus)

                pay_quantity = random.randint(1, 50)
                sales_amount = round(pay_quantity * product['original_price'] * random.uniform(0.8, 1.0), 2)

                # 库存逻辑：基于前一天库存变化
                current_stock = random.randint(50, 500)
                stock_in = random.randint(0, 100)
                stock_out = pay_quantity + random.randint(0, 10)

                data = {
                    'sku_id': sku['sku_id'],
                    'product_id': product['product_id'],
                    'sku_info': sku['sku_info'],
                    'stat_date': stat_date,
                    'pay_quantity': pay_quantity,
                    'sales_amount': sales_amount,
                    'current_stock': current_stock,
                    'stock_in': stock_in,
                    'stock_out': stock_out
                }

            elif table == 'product_trend_hourly':
                # 为每个小时生成数据
                for hour in range(24):
                    # 模拟小时数据分布，晚上和周末流量更高
                    hour_factor = 1.0
                    if 0 <= hour < 6:
                        hour_factor = 0.2  # 凌晨流量低
                    elif 18 <= hour < 23:
                        hour_factor = 1.8  # 晚上流量高

                    # 周末流量更高
                    if stat_date.weekday() >= 5:
                        hour_factor *= 1.5

                    visitor_count = max(1, int(random.randint(5, 200) * hour_factor))
                    sales_quantity = random.randint(0, visitor_count // 10)
                    pay_amount = round(sales_quantity * product['original_price'] * random.uniform(0.8, 1.0), 2)

                    data = {
                        'product_id': product['product_id'],
                        'stat_date': stat_date,
                        'stat_hour': hour,
                        'visitor_count': visitor_count,
                        'sales_quantity': sales_quantity,
                        'pay_amount': pay_amount
                    }

                    # 插入单条数据
                    fields = data.keys()
                    placeholders = ', '.join(['%s'] * len(fields))
                    sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"

                    values = [data[field] for field in fields]
                    cursor.execute(sql, tuple(values))

                total += 24  # 每小时一条数据
                continue

            elif table == 'search_word_daily':
                # 搜索词每日数据
                search_word = random.choice(
                    [p['product_name'] for p in products[:100]] + [p['brand'] for p in products[:100]])
                search_count = random.randint(10, 500)
                click_count = random.randint(1, search_count // 2)
                # 修复空范围问题
                max_visitor = max(1, click_count // 2)
                visitor_count = random.randint(1, max_visitor)

                data = {
                    'search_word': search_word,
                    'stat_date': stat_date,
                    'product_id': product['product_id'],
                    'search_count': search_count,
                    'click_count': click_count,
                    'visitor_count': visitor_count
                }

            elif table == 'traffic_source_daily':
                # 流量来源每日数据
                source_name = random.choice(
                    ['效果广告', '手淘搜索', '京东搜索', '拼多多搜索', '微信小程序', '直播带货'])
                visitor_count = random.randint(10, 500)
                pay_buyer_count = random.randint(0, visitor_count // 5)
                click_count = random.randint(visitor_count, visitor_count * 3)
                stay_duration = random.randint(10, 300)

                data = {
                    'product_id': product['product_id'],
                    'stat_date': stat_date,
                    'source_name': source_name,
                    'visitor_count': visitor_count,
                    'pay_buyer_count': pay_buyer_count,
                    'click_count': click_count,
                    'stay_duration': stay_duration
                }

            # 插入单条数据
            fields = data.keys()
            placeholders = ', '.join(['%s'] * len(fields))
            sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"

            values = [data[field] for field in fields]
            cursor.execute(sql, tuple(values))
            total += 1

        # 每天提交一次
        conn.commit()
        print(
            f"插入 {table}: 日期 {stat_date.strftime('%Y-%m-%d')}, 累计 {total} 条数据, 耗时: {time.time() - start_time:.2f}秒")

    cursor.close()
    conn.close()
    print(f"完成 {table} 数据插入，总数据量: {total} 条，总耗时: {time.time() - start_time:.2f}秒\n")


# 主函数
def main():
    print("开始生成GMALL测试数据...\n")

    # 1. 生成基础数据
    print("生成商品分类数据...")
    categories = generate_product_categories(100)
    batch_insert('product_category', categories)

    print("生成店铺数据...")
    shops = generate_shops(200)
    batch_insert('shop_base', shops)

    print("生成商品基础数据...")
    products = generate_products(3000, categories, shops)
    batch_insert('product_base', products)

    print("生成SKU数据...")
    skus = generate_skus(8000, products)

    # 2. 生成其他关联数据
    print("生成价格力数据...")
    price_strengths = generate_price_strengths(products)
    batch_insert('price_strength_product', price_strengths)

    print("生成商品预警数据...")
    warnings = generate_product_warnings(2000, products)
    # 为预警数据添加日期
    for w in warnings:
        w['stat_date'] = random_date(datetime(2023, 1, 1), datetime.now()).strftime('%Y-%m-%d')
    batch_insert('product_warning_daily', warnings)

    # 3. 生成带日期的时序数据
    print("生成商品每日销售数据...")
    generate_and_insert_time_series_data('product_sales_daily', products, days=30, daily_avg=300)

    print("生成SKU销售及库存数据...")
    generate_and_insert_time_series_data('sku_sales_inventory_daily', products, skus, days=30, daily_avg=200)

    print("生成流量来源数据...")
    generate_and_insert_time_series_data('traffic_source_daily', products, days=30, daily_avg=200)

    print("生成搜索词数据...")
    generate_and_insert_time_series_data('search_word_daily', products, days=30, daily_avg=200)

    print("生成商品趋势每小时数据...")
    generate_and_insert_time_series_data('product_trend_hourly', products, days=15, daily_avg=100)

    print("所有数据生成完成！")


if __name__ == "__main__":
    main()
