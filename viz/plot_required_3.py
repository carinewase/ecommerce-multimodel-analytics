import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from pymongo import MongoClient
from datetime import datetime

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(PROJECT_DIR, "outputs")

# MongoDB configuration
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'ecommerce'

def ensure(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")

def barh(df, x, y, title, out_png, top_n=10):
    df = df.copy()
    df[y] = pd.to_numeric(df[y], errors="coerce")
    df = df.dropna(subset=[y])

    df = df.sort_values(y, ascending=False).head(top_n).sort_values(y, ascending=True)

    plt.figure(figsize=(12, 6))
    plt.barh(df[x].astype(str), df[y])
    plt.title(title)
    plt.xlabel(y)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()
    print(" Saved:", out_png)




def plot_08_revenue_by_category_over_time():
    """
    Visualization 1: Sales Performance Over Time by Category
    Shows monthly revenue trends for each product category
    """
    path = os.path.join(OUT, "revenue_by_category_2025.csv")
    ensure(path)
    df = pd.read_csv(path)
    
    # Clean and convert
    df['year_month'] = pd.to_datetime(df['year_month'])
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue'])
    
    # Sort by time
    df = df.sort_values('year_month')
    
    # Get category names
    if 'category_name' not in df.columns:
        df['category_name'] = df['category_id']
    
    # Create figure
    plt.figure(figsize=(14, 7))
    
    # Plot line for each category
    for cat_id in df['category_id'].unique():
        cat_data = df[df['category_id'] == cat_id].sort_values('year_month')
        cat_name = cat_data['category_name'].iloc[0] if len(cat_data) > 0 else cat_id
        plt.plot(cat_data['year_month'], cat_data['revenue'], 
                marker='o', linewidth=2, label=cat_name[:20])  # Truncate long names
    
    plt.title('Monthly Revenue by Category (2025)', fontsize=14, fontweight='bold')
    plt.xlabel('Month', fontsize=12)
    plt.ylabel('Revenue ($)', fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    plt.xticks(rotation=45)
    
    # Format y-axis to show values in thousands
    ax = plt.gca()
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))
    
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    out_png = os.path.join(OUT, "08_revenue_by_category_timeline.png")
    plt.savefig(out_png, dpi=200, bbox_inches='tight')
    plt.close()
    print(" Saved:", out_png)


def plot_09_customer_segmentation():
    """
    Visualization 2: Customer Segmentation by Spending and Order Frequency
    Shows distribution of customers across different segments
    """
    path = os.path.join(OUT, "clv.csv")
    ensure(path)
    df = pd.read_csv(path)
    
    # Clean data
    df['total_revenue'] = pd.to_numeric(df['total_revenue'], errors='coerce')
    df['num_orders'] = pd.to_numeric(df['num_orders'], errors='coerce')
    df = df.dropna(subset=['total_revenue', 'num_orders'])
    
    # Create segments based on quartiles
    spending_quartile = pd.qcut(df['total_revenue'], q=4, labels=['Low', 'Medium', 'High', 'Premium'])
    order_quartile = pd.qcut(df['num_orders'], q=4, labels=['Occasional', 'Regular', 'Frequent', 'VIP'])
    
    # Create a 2D segment analysis
    segment_data = pd.DataFrame({
        'spending': spending_quartile,
        'frequency': order_quartile
    }).value_counts().reset_index(name='count')
    
    # Create heatmap data
    pivot_data = segment_data.pivot_table(
        index='spending', 
        columns='frequency', 
        values='count', 
        fill_value=0
    )
    
    plt.figure(figsize=(10, 7))
    sns.heatmap(pivot_data, annot=True, fmt='.0f', cmap='YlOrRd', 
                cbar_kws={'label': 'Number of Customers'}, linewidths=0.5)
    
    plt.title('Customer Segmentation: Spending vs Order Frequency', fontsize=14, fontweight='bold')
    plt.xlabel('Purchase Frequency', fontsize=12)
    plt.ylabel('Spending Level', fontsize=12)
    plt.tight_layout()
    
    out_png = os.path.join(OUT, "09_customer_segmentation_heatmap.png")
    plt.savefig(out_png, dpi=200, bbox_inches='tight')
    plt.close()
    print(" Saved:", out_png)
    
    # Create additional scatter plot
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(df['num_orders'], df['total_revenue'], 
                         alpha=0.5, s=50, c=df['active_days'], cmap='viridis')
    plt.colorbar(scatter, label='Active Days')
    
    plt.title('Customer Distribution: Order Frequency vs Lifetime Value', fontsize=14, fontweight='bold')
    plt.xlabel('Number of Orders', fontsize=12)
    plt.ylabel('Total Revenue ($)', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Format axes
    ax = plt.gca()
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f'${x/1000:.0f}K'))
    
    plt.tight_layout()
    out_png = os.path.join(OUT, "09b_customer_ltv_scatter.png")
    plt.savefig(out_png, dpi=200, bbox_inches='tight')
    plt.close()
    print(" Saved:", out_png)


def plot_10_product_performance():
    """
    Visualization 3: Product Performance - Top Selling Products
    Shows comparison of top products by co-purchase and overall performance
    """
    path = os.path.join(OUT, "also_bought_pairs_top10.csv")
    ensure(path)
    df = pd.read_csv(path)
    
    # Extract individual products and their co-purchase counts
    products_a = df['product_a_name' if 'product_a_name' in df.columns else 'product_a'].values
    products_b = df['product_b_name' if 'product_b_name' in df.columns else 'product_b'].values
    counts = pd.to_numeric(df['co_count'] if 'co_count' in df.columns else df.iloc[:, -1], errors='coerce').values
    
    # Combine products and sum their co-purchase counts
    product_performance = {}
    for prod, count in zip(products_a, counts):
        product_performance[prod] = product_performance.get(prod, 0) + count
    for prod, count in zip(products_b, counts):
        product_performance[prod] = product_performance.get(prod, 0) + count
    
    # Sort and get top 15
    sorted_products = sorted(product_performance.items(), key=lambda x: x[1], reverse=True)[:15]
    products = [p[0][:25] for p in sorted_products]
    values = [p[1] for p in sorted_products]
    
    # Create horizontal bar chart
    plt.figure(figsize=(12, 8))
    bars = plt.barh(products, values, color=plt.cm.viridis(np.linspace(0, 1, len(products))))
    
    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, values)):
        plt.text(val, i, f' {int(val)}', va='center', fontsize=9)
    
    plt.title('Top 15 Products by Co-Purchase Frequency', fontsize=14, fontweight='bold')
    plt.xlabel('Co-Purchase Count', fontsize=12)
    plt.ylabel('Product', fontsize=12)
    plt.tight_layout()
    
    out_png = os.path.join(OUT, "10_product_performance_copurchase.png")
    plt.savefig(out_png, dpi=200, bbox_inches='tight')
    plt.close()
    print(" Saved:", out_png)


def plot_11_sales_distribution():
    """
    Visualization 4: Sales Distribution Analysis
    Shows distribution of revenue per transaction and customer lifetime value
    """
    path = os.path.join(OUT, "clv.csv")
    ensure(path)
    df = pd.read_csv(path)
    
    # Clean data
    df['total_revenue'] = pd.to_numeric(df['total_revenue'], errors='coerce')
    df['num_orders'] = pd.to_numeric(df['num_orders'], errors='coerce')
    df = df.dropna(subset=['total_revenue', 'num_orders'])
    
    # Calculate average transaction value
    df['avg_transaction'] = df['total_revenue'] / df['num_orders']
    
    # Create subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. Distribution of customer lifetime value
    axes[0, 0].hist(df['total_revenue'], bins=50, color='steelblue', edgecolor='black', alpha=0.7)
    axes[0, 0].set_title('Distribution of Customer Lifetime Value', fontsize=12, fontweight='bold')
    axes[0, 0].set_xlabel('Total Revenue ($)', fontsize=10)
    axes[0, 0].set_ylabel('Number of Customers', fontsize=10)
    axes[0, 0].grid(True, alpha=0.3)
    
    # 2. Distribution of order frequency
    axes[0, 1].hist(df['num_orders'], bins=30, color='coral', edgecolor='black', alpha=0.7)
    axes[0, 1].set_title('Distribution of Purchase Frequency', fontsize=12, fontweight='bold')
    axes[0, 1].set_xlabel('Number of Orders', fontsize=10)
    axes[0, 1].set_ylabel('Number of Customers', fontsize=10)
    axes[0, 1].grid(True, alpha=0.3)
    
    # 3. Distribution of average transaction value
    axes[1, 0].hist(df['avg_transaction'], bins=50, color='seagreen', edgecolor='black', alpha=0.7)
    axes[1, 0].set_title('Distribution of Average Transaction Value', fontsize=12, fontweight='bold')
    axes[1, 0].set_xlabel('Average Transaction Value ($)', fontsize=10)
    axes[1, 0].set_ylabel('Number of Customers', fontsize=10)
    axes[1, 0].grid(True, alpha=0.3)
    
    # 4. Box plot comparison
    box_data = [df['total_revenue'], df['num_orders']*1000, df['avg_transaction']]
    bp = axes[1, 1].boxplot(box_data, labels=['CLV', 'Orders*1K', 'Avg Tx'], patch_artist=True)
    for patch, color in zip(bp['boxes'], ['steelblue', 'coral', 'seagreen']):
        patch.set_facecolor(color)
    axes[1, 1].set_title('Statistical Comparison of Key Metrics', fontsize=12, fontweight='bold')
    axes[1, 1].set_ylabel('Value', fontsize=10)
    axes[1, 1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    out_png = os.path.join(OUT, "11_sales_distribution_analysis.png")
    plt.savefig(out_png, dpi=200, bbox_inches='tight')
    plt.close()
    print(" Saved:", out_png)


def main():
    print("Using outputs:", OUT)

    plot_08_revenue_by_category_over_time()
    plot_09_customer_segmentation()
    plot_10_product_performance()
    plot_11_sales_distribution()
  

    print("\n✓ Done. You now have 5 required visualizations in outputs/")

if __name__ == "__main__":
    main()
