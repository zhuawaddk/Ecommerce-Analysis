"""
巴西电商数据分析 - 可视化脚本
读取 analysis.py 输出的 CSV 文件，生成 matplotlib 图表
"""
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
CHART_DIR = os.path.join(os.path.dirname(__file__), "output", "charts")
os.makedirs(CHART_DIR, exist_ok=True)


def save_chart(fig, name):
    path = os.path.join(CHART_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  -> {path}")


# ============================================================
# 图1: 各州销售额 Top 10
# ============================================================
df_state = pd.read_csv(os.path.join(OUTPUT_DIR, "state_analysis.csv")).head(10)

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.bar(df_state["customer_state"], df_state["total_gmv"] / 10000,
              color=["#2B5B84" if i < 3 else "#7EA8C4" for i in range(10)])
ax.set_title("Top 10 States by Total GMV", fontsize=16, fontweight="bold")
ax.set_xlabel("State")
ax.set_ylabel("GMV (万 BRL)")
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.0f"))
for bar, val in zip(bars, df_state["total_gmv"] / 10000):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 3,
            f"{val:.0f}", ha="center", fontsize=8)
ax2 = ax.twinx()
ax2.plot(df_state["customer_state"], df_state["avg_order_value"],
         color="#C0504D", marker="o", linewidth=2, markersize=6)
ax2.set_ylabel("Avg Order Value (BRL)", color="#C0504D")
ax2.tick_params(axis="y", colors="#C0504D")
save_chart(fig, "01_state_sales.png")
plt.close()

# ============================================================
# 图2: 月度 GMV + 环比增长率
# ============================================================
df_monthly = pd.read_csv(os.path.join(OUTPUT_DIR, "monthly_sales.csv"))
df_monthly["order_month"] = df_monthly["order_month"].astype(str)

fig, ax1 = plt.subplots(figsize=(14, 6))
ax1.fill_between(range(len(df_monthly)), df_monthly["monthly_gmv"] / 10000,
                 alpha=0.3, color="#2B5B84")
ax1.plot(range(len(df_monthly)), df_monthly["monthly_gmv"] / 10000,
         color="#2B5B84", linewidth=2, marker="o", markersize=5)
ax1.set_title("Monthly GMV Trend", fontsize=16, fontweight="bold")
ax1.set_ylabel("GMV (万 BRL)")
ax1.set_xticks(range(len(df_monthly)))
ax1.set_xticklabels(df_monthly["order_month"], rotation=45, ha="right", fontsize=7)

ax2 = ax1.twinx()
valid = df_monthly["mom_growth_pct"].notna()
colors = ["#C0504D" if v >= 0 else "#4D8066" for v in df_monthly.loc[valid, "mom_growth_pct"]]
ax2.bar(df_monthly.index[valid], df_monthly.loc[valid, "mom_growth_pct"],
        color=colors, alpha=0.6, width=0.6)
ax2.axhline(y=0, color="gray", linewidth=0.5, linestyle="--")
ax2.set_ylabel("MoM Growth (%)")
save_chart(fig, "02_monthly_trend.png")
plt.close()

# ============================================================
# 图3: 品类 GMV Top 10 水平柱状图
# ============================================================
df_cat = pd.read_csv(os.path.join(OUTPUT_DIR, "category_ranking.csv")).head(10)
df_cat = df_cat.iloc[::-1]  # 倒序让最大排上面

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.barh(df_cat["product_category_name_english"].str.replace("_", " ").str.title(),
               df_cat["total_gmv"] / 10000,
               color=["#2B5B84" if i < 3 else "#7EA8C4" for i in range(9, -1, -1)])
ax.set_title("Top 10 Product Categories by GMV", fontsize=14, fontweight="bold")
ax.set_xlabel("GMV (万 BRL)")
for bar, val in zip(bars, df_cat["total_gmv"] / 10000):
    ax.text(bar.get_width() + 2, bar.get_y() + bar.get_height() / 2,
            f"{val:.0f}", va="center", fontsize=8)
save_chart(fig, "03_category_ranking.png")
plt.close()

# ============================================================
# 图4: RFM 客户分层饼图
# ============================================================
df_rfm = pd.read_csv(os.path.join(OUTPUT_DIR, "rfm_segmentation.csv"))
colors_rfm = ["#2B5B84", "#7EA8C4", "#B8CDE0", "#4D8066"]
seg_labels = [s.split("(")[0] for s in df_rfm["customer_segment"]]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

wedges, texts, autotexts = ax1.pie(
    df_rfm["customer_count"], labels=seg_labels, autopct="%1.1f%%",
    colors=colors_rfm, startangle=90, textprops={"fontsize": 9}
)
ax1.set_title("Customer Segment Distribution", fontsize=14, fontweight="bold")

ax2.bar(seg_labels, df_rfm["avg_purchase_amount"],
        color=colors_rfm, edgecolor="white")
ax2.set_title("Avg Purchase Amount by Segment", fontsize=14, fontweight="bold")
ax2.set_ylabel("Avg Purchase Amount (BRL)")
for i, val in enumerate(df_rfm["avg_purchase_amount"]):
    ax2.text(i, val + 5, f"{val:.0f}", ha="center", fontsize=9)
save_chart(fig, "04_rfm_segmentation.png")
plt.close()

print("\n可视化完成! 图表已保存至 output/charts/ 目录")
