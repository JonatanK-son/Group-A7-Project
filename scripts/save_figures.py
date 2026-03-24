import os
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    # Set the style
    sns.set_theme(style="whitegrid")
    
    # Define paths
    ROOT = Path(__file__).resolve().parent.parent
    output_dir = ROOT / "output" / "figures"
    results_dir = ROOT / "output" / "results" # Dask results
    results_spark_dir = ROOT / "output" / "results_spark" # Spark results
    
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Saving figures to: {output_dir.resolve()}")

    # Determine which results to use (prefer Spark if available, else Dask)
    if (results_spark_dir / "revenue_by_category.parquet").exists():
        data_source = results_spark_dir
        print("Using Spark results for visualization.")
    elif (results_dir / "revenue_by_category.parquet").exists():
        data_source = results_dir
        print("Using Dask results for visualization.")
    else:
        print("Error: No result files found in either output/results or output/results_spark.")
        print("Please run the pipeline (just run-dask or just run-spark) first.")
        return

    try:
        # 1. Revenue by Category Chart
        rev_df = pd.read_parquet(data_source / "revenue_by_category.parquet").head(10)
        plt.figure(figsize=(12, 6))
        sns.barplot(data=rev_df, x="total_revenue", y="top_category", palette="viridis")
        plt.title("Top 10 Categories by Total Revenue", fontsize=16)
        plt.xlabel("Total Revenue ($)", fontsize=12)
        plt.ylabel("Category", fontsize=12)
        plt.tight_layout()
        plt.savefig(output_dir / "revenue_by_category.png", dpi=300)
        print(f"Generated: {output_dir / 'revenue_by_category.png'}")

        # 2. Conversion Funnel Chart
        funnel_df = pd.read_parquet(data_source / "conversion_funnel.parquet").head(5)
        # Melt for easier plotting
        funnel_melted = funnel_df.melt(id_vars=["top_category"], value_vars=["view", "cart", "purchase"], 
                                      var_name="Step", value_name="Count")
        plt.figure(figsize=(14, 7))
        sns.barplot(data=funnel_melted, x="top_category", y="Count", hue="Step")
        plt.title("Conversion Funnel for Top 5 Categories", fontsize=16)
        plt.xlabel("Category", fontsize=12)
        plt.ylabel("Event Count", fontsize=12)
        plt.legend(title="Funnel Step")
        plt.tight_layout()
        plt.savefig(output_dir / "conversion_funnel.png", dpi=300)
        print(f"Generated: {output_dir / 'conversion_funnel.png'}")

        # 3. Top Brands Chart
        brands_df = pd.read_parquet(data_source / "top_brands.parquet").head(10)
        plt.figure(figsize=(10, 6))
        sns.barplot(data=brands_df, x="total_revenue", y="brand", palette="magma")
        plt.title("Top 10 Brands by Revenue", fontsize=16)
        plt.xlabel("Total Revenue ($)", fontsize=12)
        plt.ylabel("Brand", fontsize=12)
        plt.tight_layout()
        plt.savefig(output_dir / "top_brands.png", dpi=300)
        print(f"Generated: {output_dir / 'top_brands.png'}")

        print("\nAll figures saved successfully in output/figures/")

    except Exception as e:
        print(f"An error occurred during figure generation: {e}")

if __name__ == "__main__":
    main()
