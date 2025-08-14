import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 设置中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
sns.set_style("whitegrid")


# 1. 数据加载与预处理
def load_data(file_path):
    """加载交易数据并进行预处理"""
    # 假设数据格式：customer_id(客户ID), transaction_date(交易日期), transaction_amount(交易金额)
    df = pd.read_csv(file_path)

    # 转换日期格式
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # 检查缺失值
    print("缺失值情况：")
    print(df.isnull().sum())

    # 移除缺失值
    df = df.dropna(subset=['customer_id', 'transaction_date', 'transaction_amount'])

    return df


# 2. 计算RFM指标
def calculate_rfm(df):
    """计算每个客户的RFM指标"""
    # 确定分析日期（数据中最后一笔交易日期+1天）
    last_transaction_date = df['transaction_date'].max()
    analysis_date = last_transaction_date + pd.Timedelta(days=1)

    # 按客户分组计算RFM
    rfm = df.groupby('customer_id').agg({
        'transaction_date': lambda x: (analysis_date - x.max()).days,  # R：最近消费天数
        'transaction_id': 'count',  # F：消费频率（假设transaction_id为交易唯一标识）
        'transaction_amount': 'sum'  # M：消费总金额
    }).rename(columns={
        'transaction_date': 'Recency',
        'transaction_id': 'Frequency',
        'transaction_amount': 'Monetary'
    })

    return rfm


# 3. RFM指标打分（5分制）
def rfm_scoring(rfm):
    """对RFM指标进行打分（R值越小得分越高，F和M越大得分越高）"""
    # 对R打分（分5档，最近消费的客户得分高）
    r_labels = range(5, 0, -1)  # R的打分标签：5,4,3,2,1
    r_quartiles = pd.qcut(rfm['Recency'], 5, labels=r_labels)

    # 对F打分（分5档，频率高的客户得分高）
    f_labels = range(1, 6)  # F的打分标签：1,2,3,4,5
    # 处理频率为0的特殊情况（实际业务中频率不会为0，此处为健壮性处理）
    if rfm['Frequency'].min() == 0:
        f_quartiles = pd.qcut(
            rfm['Frequency'].rank(method='first'), 5, labels=f_labels
        )
    else:
        f_quartiles = pd.qcut(rfm['Frequency'], 5, labels=f_labels)

    # 对M打分（分5档，金额高的客户得分高）
    m_labels = range(1, 6)  # M的打分标签：1,2,3,4,5
    m_quartiles = pd.qcut(rfm['Monetary'], 5, labels=m_labels)

    # 合并分数
    rfm['R_Score'] = r_quartiles.astype(int)
    rfm['F_Score'] = f_quartiles.astype(int)
    rfm['M_Score'] = m_quartiles.astype(int)

    # 计算RFM总得分（可根据业务调整权重，此处简单相加）
    rfm['RFM_Total_Score'] = rfm['R_Score'] + rfm['F_Score'] + rfm['M_Score']

    return rfm


# 4. 客户分群（基于RFM分数组合）
def segment_customers(rfm):
    """根据RFM分数划分客户群体"""

    # 定义分群规则（可根据业务调整）
    def get_segment(row):
        r = row['R_Score']
        f = row['F_Score']
        m = row['M_Score']

        # 高价值客户：最近消费、高频、高金额
        if r >= 4 and f >= 4 and m >= 4:
            return '高价值客户'
        # 忠诚客户：高频但最近消费稍远
        elif r >= 3 and f >= 4 and m >= 3:
            return '忠诚客户'
        # 潜力客户：最近消费、金额高但频率低
        elif r >= 4 and f <= 3 and m >= 4:
            return '潜力客户'
        # 流失预警客户：曾经高频高金额，但很久没消费
        elif r <= 2 and f >= 3 and m >= 3:
            return '流失预警客户'
        # 一般维持客户：各项指标中等
        elif (r >= 3 and r <= 4) and (f >= 2 and f <= 3) and (m >= 2 and m <= 3):
            return '一般维持客户'
        # 低价值客户：各项指标较低
        else:
            return '低价值客户'

    rfm['Segment'] = rfm.apply(get_segment, axis=1)
    return rfm


# 5. 结果可视化
def visualize_results(rfm):
    """可视化RFM分析结果"""
    # 1. 客户分群分布
    plt.figure(figsize=(10, 6))
    segment_counts = rfm['Segment'].value_counts()
    sns.barplot(x=segment_counts.index, y=segment_counts.values)
    plt.title('客户分群分布')
    plt.xticks(rotation=45)
    plt.ylabel('客户数量')
    plt.tight_layout()
    plt.show()

    # 2. 各分群RFM平均得分对比
    segment_scores = rfm.groupby('Segment')[['R_Score', 'F_Score', 'M_Score']].mean()
    segment_scores.plot(kind='bar', figsize=(12, 6))
    plt.title('各客户分群的RFM平均得分')
    plt.ylabel('平均得分')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# 主函数
def main(file_path):
    # 加载数据
    df = load_data(file_path)
    print("\n数据预览：")
    print(df.head())

    # 计算RFM指标
    rfm = calculate_rfm(df)
    print("\nRFM指标计算结果：")
    print(rfm.head())

    # RFM打分
    rfm_scored = rfm_scoring(rfm)
    print("\nRFM打分结果：")
    print(rfm_scored[['R_Score', 'F_Score', 'M_Score', 'RFM_Total_Score']].head())

    # 客户分群
    rfm_segmented = segment_customers(rfm_scored)
    print("\n客户分群结果：")
    print(rfm_segmented[['R_Score', 'F_Score', 'M_Score', 'Segment']].head())

    # 结果可视化
    visualize_results(rfm_segmented)

    return rfm_segmented


if __name__ == "__main__":
    # 替换为实际数据文件路径
    transaction_data_path = "transaction_data.csv"
    rfm_result = main(transaction_data_path)
