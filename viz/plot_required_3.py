import os
import pandas as pd
import matplotlib.pyplot as plt

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(PROJECT_DIR, "outputs")

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

def plot_05_also_bought():
    path = os.path.join(OUT, "also_bought_pairs_top10.csv")
    ensure(path)
    df = pd.read_csv(path)

    # Build label
    if "product_a_name" in df.columns and "product_b_name" in df.columns:
        df["pair"] = df["product_a_name"].astype(str) + " + " + df["product_b_name"].astype(str)
    else:
        df["pair"] = df["product_a"].astype(str) + " + " + df["product_b"].astype(str)

    y = "co_count" if "co_count" in df.columns else df.columns[-1]

    barh(df, "pair", y, "Top 10 Also-Bought Product Pairs",
         os.path.join(OUT, "05_also_bought_pairs_top10.png"), top_n=10)

def plot_06_cohort_retention():
    path = os.path.join(OUT, "cohort_retention.csv")
    ensure(path)
    df = pd.read_csv(path)

    # Keep only cohort_index 0..6 if exists (cleaner)
    df["cohort_index"] = pd.to_numeric(df["cohort_index"], errors="coerce")
    df = df.dropna(subset=["cohort_index"])
    df = df[df["cohort_index"] <= 6]

    # Convert cohort_month to date
    df["cohort_month"] = pd.to_datetime(df["cohort_month"], errors="coerce")
    df = df.dropna(subset=["cohort_month"])

    # pivot to matrix (heatmap style)
    pivot = df.pivot_table(index="cohort_month", columns="cohort_index", values="retention_rate", aggfunc="mean")
    pivot = pivot.sort_index()

    plt.figure(figsize=(12, 6))
    plt.imshow(pivot.values, aspect="auto")
    plt.title("Cohort Retention Heatmap (Retention Rate)")
    plt.xlabel("Cohort Index (Months since first purchase)")
    plt.ylabel("Cohort Month")
    plt.xticks(range(len(pivot.columns)), [str(c) for c in pivot.columns])
    plt.yticks(range(len(pivot.index)), [d.strftime("%Y-%m") for d in pivot.index])
    plt.colorbar(label="Retention Rate")
    plt.tight_layout()
    out_png = os.path.join(OUT, "06_cohort_retention.png")
    plt.savefig(out_png, dpi=200)
    plt.close()
    print(" Saved:", out_png)

def plot_07_clv_top10():
    path = os.path.join(OUT, "clv.csv")
    ensure(path)
    df = pd.read_csv(path)

    # Guess columns
    user_col = "user_id" if "user_id" in df.columns else df.columns[0]
    clv_col = "clv" if "clv" in df.columns else ("predicted_clv" if "predicted_clv" in df.columns else df.columns[1])

    barh(df, user_col, clv_col, "Top 10 Customers by CLV",
         os.path.join(OUT, "07_top_customers_by_clv.png"), top_n=10)

def main():
    print("Using outputs:", OUT)
    plot_05_also_bought()
    plot_06_cohort_retention()
    plot_07_clv_top10()
    print("\n Done. You now have 3 required visualizations in outputs/")

if __name__ == "__main__":
    main()
