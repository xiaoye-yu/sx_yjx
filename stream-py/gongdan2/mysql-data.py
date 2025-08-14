import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import time

# 初始化Faker（中文环境，生成更真实的中文名称）
fake = Faker('zh_CN')

# 数据库连接配置（根据实际环境修改）
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': '123456',
    'database': 'gmall_ins',
    'port': 3306
}


# --------------------------- 工具函数 ---------------------------
def get_db_connection():
    """获取数据库连接"""
    return mysql.connector.connect(**db_config)


def random_date(start_date, end_date):
    """生成两个日期之间的随机日期"""
    time_between = end_date - start_date
    random_days = random.randrange(time_between.days)
    return start_date + timedelta(days=random_days)


# --------------------------- 基础数据生成（修复分类生成错误） ---------------------------
def generate_product_categories(num=100):
    """生成商品分类（修复父分类查找错误）"""
    categories = []
    # 存储所有有效分类ID，用于后续选择父分类
    valid_parent_ids = ['0']  # 顶级分类父ID

    # 一级分类：真实电商大品类
    first_level = [
        '3C数码', '时尚服饰', '食品生鲜', '家居生活',
        '美妆个护', '母婴用品', '图书音像', '运动户外'
    ]

    # 生成一级分类
    for i, cat_name in enumerate(first_level):
        cat_id = f"cat{i + 1:03d}"
        categories.append({
            'category_id': cat_id,
            'category_name': cat_name,
            'parent_category_id': '0',
            'level': 1
        })
        valid_parent_ids.append(cat_id)  # 只有已生成的分类ID才能作为父ID

    # 二级分类模板（与一级分类强关联）
    second_level_map = {
        '3C数码': ['手机', '笔记本电脑', '平板电脑', '智能手表', '耳机音响', '数码配件'],
        '时尚服饰': ['男装', '女装', '童装', '鞋靴', '内衣', '配饰'],
        '食品生鲜': ['休闲零食', '水果', '肉类', '粮油', '乳制品', '速食'],
        '家居生活': ['家具', '家纺', '厨房用品', '清洁用品', '灯具', '装饰'],
        '美妆个护': ['面部护理', '彩妆', '香水', '洗发水', '沐浴露', '牙膏牙刷'],
        '母婴用品': ['婴儿奶粉', '纸尿裤', '童装', '玩具', '孕妇用品', '喂养工具'],
        '图书音像': ['小说', '教材', '儿童绘本', '杂志', '音乐CD', '电影DVD'],
        '运动户外': ['运动鞋', '运动服', '健身器材', '户外装备', '球类', '骑行用品']
    }

    # 三级分类后缀（让名称更具体）
    third_level_suffix = ['经典款', '新款', '高端系列', '入门款', '限量版']

    # 生成二级和三级分类
    for i in range(len(first_level), num):
        cat_id = f"cat{i + 1:03d}"
        # 只从已生成的分类ID中选择父ID，避免无效ID
        parent_id = random.choice(valid_parent_ids)
        level = 2 if parent_id == '0' else 3

        try:
            # 生成有意义的分类名称
            if level == 2:
                # 二级分类：从一级分类的模板中选择
                # 查找父分类信息
                parent_cats = [c for c in categories if c['category_id'] == parent_id]
                if not parent_cats:
                    raise ValueError(f"父分类ID {parent_id} 不存在")

                first_cat_name = parent_cats[0]['category_name']
                # 确保一级分类在映射表中存在
                if first_cat_name not in second_level_map:
                    cat_name = f"二级分类{i}"
                else:
                    cat_name = random.choice(second_level_map[first_cat_name])
            else:
                # 三级分类：基于二级分类+后缀
                parent_cats = [c for c in categories if c['category_id'] == parent_id]
                if not parent_cats:
                    raise ValueError(f"父分类ID {parent_id} 不存在")

                second_cat_name = parent_cats[0]['category_name']
                cat_name = f"{second_cat_name}-{random.choice(third_level_suffix)}"

            categories.append({
                'category_id': cat_id,
                'category_name': cat_name,
                'parent_category_id': parent_id,
                'level': level
            })

            # 只有二级分类可以作为父分类
            if level == 2:
                valid_parent_ids.append(cat_id)

        except Exception as e:
            print(f"生成分类时出错: {e}，使用默认分类名称")
            # 出错时使用默认名称，确保程序能继续运行
            categories.append({
                'category_id': cat_id,
                'category_name': f"分类{i + 1}",
                'parent_category_id': '0',  # 默认使用顶级分类作为父类
                'level': 2
            })
            valid_parent_ids.append(cat_id)

    return categories[:num]


def generate_shops(num=200):
    """生成店铺数据（名称和经营范围更贴近真实电商）"""
    shops = []
    # 真实店铺经营范围（与分类关联）
    business_scopes = [
        '3C数码全品类销售', '时尚服饰连锁', '食品生鲜超市',
        '家居生活卖场', '美妆个护专卖', '母婴用品体验店',
        '图书音像批发', '运动户外装备'
    ]

    for i in range(num):
        shop_id = f"shop{i + 1:05d}"
        # 店铺名称格式：品牌名/公司名 + 业态（如“XX旗舰店”“XX专营店”）
        shop_name = f"{fake.company()}{'旗舰店' if random.random() > 0.3 else '专营店'}"
        shops.append({
            'shop_id': shop_id,
            'shop_name': shop_name,
            'business_scope': random.choice(business_scopes),
            'opening_date': random_date(datetime(2018, 1, 1), datetime(2023, 1, 1))
        })

    return shops


def generate_products(num=3000, categories=None, shops=None):
    """生成商品数据（名称结合分类、属性，避免无意义词汇）"""
    products = []
    # 真实品牌列表（覆盖各品类）
    brands = [
        '华为', '小米', '苹果', '耐克', '阿迪达斯', '优衣库',
        '三只松鼠', '百草味', '美的', '海尔', '欧莱雅', '兰蔻',
        '好孩子', '乐高', '九阳', '苏泊尔', '恒源祥', '南极人'
    ]

    # 商品属性词（让名称更具体）
    attr_words = {
        '材质': ['纯棉', '真皮', '不锈钢', '玻璃', '有机', '蚕丝'],
        '功能': ['智能', '便携', '节能', '防水', '快充', '静音'],
        '风格': ['简约', '复古', '潮流', '可爱', '商务', '运动']
    }

    for i in range(num):
        product_id = f"prod{i + 1:06d}"
        category = random.choice(categories)
        shop = random.choice(shops)

        # 商品名称生成逻辑：分类名 + 属性 + 具体物品 + 后缀
        # 例："3C数码-手机-智能-华为P50新款"
        category_name = category['category_name']
        attr = random.choice(attr_words['材质'] + attr_words['功能'])
        item = fake.word(ext_word_list=[
            '手机', 'T恤', '零食', '沙发', '面霜', '奶粉', '书', '运动鞋'
        ])  # 具体物品词
        suffix = random.choice(['', '新款', '限量版', '超值装', 'Pro'])

        product_name = f"{category_name}-{attr}-{item}-{suffix}".strip('-')

        products.append({
            'product_id': product_id,
            'product_name': product_name,
            'product_category': category['category_name'],
            'brand': random.choice(brands),
            'shop_id': shop['shop_id'],
            'launch_date': random_date(datetime(2020, 1, 1), datetime(2023, 12, 31)).strftime('%Y-%m-%d'),
            'original_price': round(random.uniform(9.9, 9999.99), 2),
            'status': random.choice(['在售', '下架']),
            'create_time': datetime.now()
        })

    return products


def generate_skus(num=8000, products=None):
    """生成SKU数据（规格结合商品类型，如衣服有尺寸，手机有颜色）"""
    skus = []
    # 真实规格选项（按品类区分）
    sku_specs = {
        '颜色': ['黑色', '白色', '红色', '蓝色', '绿色', '粉色', '灰色'],
        '尺寸': ['S', 'M', 'L', 'XL', 'XXL', '均码', '160', '170', '180'],
        '容量': ['16G', '32G', '64G', '128G', '256G', '512G'],
        '重量': ['100g', '200g', '500g', '1kg', '2kg']
    }

    for i in range(num):
        sku_id = f"sku{i + 1:07d}"
        product = random.choice(products)
        product_name = product['product_name']

        # 根据商品类型选择规格（如“手机”用颜色+容量，“衣服”用颜色+尺寸）
        if '手机' in product_name or '电脑' in product_name:
            spec1 = random.choice(sku_specs['颜色'])
            spec2 = random.choice(sku_specs['容量'])
            sku_info = f"{spec1}-{spec2}"
        elif '衣服' in product_name or '鞋' in product_name:
            spec1 = random.choice(sku_specs['颜色'])
            spec2 = random.choice(sku_specs['尺寸'])
            sku_info = f"{spec1}-{spec2}"
        elif '零食' in product_name or '食品' in product_name:
            sku_info = random.choice(sku_specs['重量'])
        else:
            sku_info = random.choice(sku_specs['颜色'])

        skus.append({
            'sku_id': sku_id,
            'product_id': product['product_id'],
            'sku_info': sku_info
        })

    return skus


# --------------------------- 关联数据生成（优化业务逻辑） ---------------------------
def generate_price_strengths(products=None):
    """生成价格力数据（逻辑不变，确保字段关联）"""
    strengths = []
    levels = ['优秀', '良好', '较差']
    price_bands = ['0-50元', '50-100元', '100-300元', '300-500元', '500-1000元', '1000元以上']

    for product in products:
        if random.random() < 0.8:  # 80%商品有价格力数据
            coupon_price = round(product['original_price'] * random.uniform(0.7, 0.98), 2)
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


def generate_search_words(num=10000, products=None):
    """生成搜索词（基于用户真实搜索习惯，如“品牌+商品名”“属性+商品”）"""
    search_words = []
    # 搜索前缀（用户常用词）
    prefixes = ['正品', '新款', '特价', '好用吗', '推荐', '官方', '评价', '促销']

    for i in range(num):
        product = random.choice(products)
        # 搜索词组合方式（更贴近真实）
        word_type = random.choice(['brand', 'product', 'brand+product', 'prefix+product'])

        if word_type == 'brand':
            search_word = product['brand']
        elif word_type == 'product':
            search_word = product['product_name']
        elif word_type == 'brand+product':
            search_word = f"{product['brand']} {product['product_name']}"
        else:
            search_word = f"{random.choice(prefixes)} {product['product_name']}"

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


def generate_traffic_sources(num=8000, products=None):
    """生成流量来源（补充真实渠道，如“抖音直播”“小红书”）"""
    traffic_sources = []
    sources = [
        '淘宝搜索', '京东搜索', '拼多多搜索', '抖音直播',
        '小红书种草', '微信朋友圈', 'APP首页推荐', '直播带货',
        '朋友推荐', '百度搜索'
    ]

    for i in range(num):
        product = random.choice(products)
        source = random.choice(sources)

        visitor_count = random.randint(10, 500)
        pay_buyer_count = random.randint(0, visitor_count // 5)
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


def generate_product_warnings(num=2000, products=None):
    """生成商品预警（原因更具体，关联关业务场景）"""
    warnings = []
    warning_types = ['价格力预警', '商品力预警', '库存预警', '质量预警']
    # 预警原因（更真实的业务问题）
    reasons = {
        '价格力预警': [
            '价格高于同类目均价30%', '优惠券力度低于行业均值',
            '近7天价格波动超过20%', '促销后未及时恢复原价'
        ],
        '商品力预警': [
            '转化率连续3天低于行业均值', '评价星级降至4.0以下',
            '退货率超过10%', '投诉量日增5单以上'
        ],
        '库存预警': [
            '库存不足3天销量', '库存积压超过30天',
            '库存周转率低于0.5', '临期商品占比超15%'
        ],
        '质量预警': [
            '质检不合格', '用户反馈质量问题增加',
            '存在安全隐患', '材质与描述不符'
        ]
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


# --------------------------- 数据插入函数（保持稳定） ---------------------------
def batch_insert(table, data, batch_size=1000):
    """批量插入数据（增加异常处理）"""
    start_time = time.time()
    conn = get_db_connection()
    cursor = conn.cursor()

    total = len(data)
    inserted = 0

    try:
        while inserted < total:
            batch = data[inserted:inserted + batch_size]
            if not batch:
                break

            fields = batch[0].keys()
            placeholders = ', '.join(['%s'] * len(fields))
            sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"

            values = [tuple(item[field] for field in fields) for item in batch]
            cursor.executemany(sql, values)
            conn.commit()

            inserted += len(batch)
            print(f"插入 {table}：{inserted}/{total} 条，耗时 {time.time() - start_time:.2f} 秒")

    except Exception as e:
        print(f"插入失败：{str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    print(f"完成 {table} 插入，总耗时 {time.time() - start_time:.2f} 秒\n")


def generate_and_insert_time_series_data(table, products, skus=None, days=30, daily_avg=100):
    """生成带日期的时序数据（如销售、流量）"""
    start_time = time.time()
    conn = get_db_connection()
    cursor = conn.cursor()

    start_date = datetime.now() - timedelta(days=days)
    total = 0

    try:
        for day in range(days):
            stat_date = start_date + timedelta(days=day)
            daily_count = random.randint(int(daily_avg * 0.7), int(daily_avg * 1.3))
            sampled_products = random.sample(products, min(daily_count, len(products)))

            for product in sampled_products:
                if table == 'product_sales_daily':
                    # 商品每日销售数据
                    visitor_count = random.randint(50, 2000)
                    pay_buyer_count = random.randint(1, visitor_count // 5)
                    pay_conversion_rate = round(pay_buyer_count / visitor_count, 4) if visitor_count else 0
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
                    # SKU销售及库存数据
                    product_skus = [s for s in skus if s['product_id'] == product['product_id']]
                    if not product_skus:
                        continue
                    sku = random.choice(product_skus)
                    pay_quantity = random.randint(1, 50)
                    sales_amount = round(pay_quantity * product['original_price'] * random.uniform(0.8, 1.0), 2)
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
                    # 商品每小时趋势数据
                    for hour in range(24):
                        # 模拟小时流量差异（晚上更高）
                        hour_factor = 1.0
                        if 0 <= hour < 6:
                            hour_factor = 0.2
                        elif 18 <= hour < 23:
                            hour_factor = 1.8
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
                        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(['%s'] * len(fields))})"
                        cursor.execute(sql, tuple(data.values()))
                        total += 1
                    continue  # 跳过后续通用插入逻辑

                elif table == 'search_word_daily':
                    # 搜索词每日数据
                    search_word = random.choice([product['brand'], product['product_name']])
                    search_count = random.randint(10, 500)
                    click_count = random.randint(1, search_count // 2)
                    visitor_count = random.randint(1, max(1, click_count // 2))

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
                    source_name = random.choice([
                        '淘宝搜索', '抖音直播', '小红书', '微信小程序', 'APP首页'
                    ])
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
                sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(['%s'] * len(fields))})"
                cursor.execute(sql, tuple(data.values()))
                total += 1

            conn.commit()
            print(
                f"插入 {table}：日期 {stat_date.strftime('%Y-%m-%d')}，累计 {total} 条，耗时 {time.time() - start_time:.2f} 秒")

    except Exception as e:
        print(f"插入失败：{str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    print(f"完成 {table} 插入，总数据 {total} 条，耗时 {time.time() - start_time:.2f} 秒\n")


# --------------------------- 主函数 ---------------------------
def main():
    print("开始生成GMALL测试数据（优化版）...\n")

    # 1. 生成基础数据（核心优化部分）
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
    batch_insert('sku_info', skus)  # 假设表名为sku_info

    # 2. 生成关联数据
    print("生成价格力数据...")
    price_strengths = generate_price_strengths(products)
    batch_insert('price_strength_product', price_strengths)

    print("生成商品预警数据...")
    warnings = generate_product_warnings(2000, products)
    for w in warnings:
        w['stat_date'] = random_date(datetime(2023, 1, 1), datetime.now()).strftime('%Y-%m-%d')
    batch_insert('product_warning_daily', warnings)

    # 3. 生成时序数据
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
