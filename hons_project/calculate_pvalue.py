
import pandas as pd
from scipy.stats import pearsonr

file_log = "/Users/apple/PROJECT/hons_project/outcome/tesla/figures_df_analysis_report_3"
sents_tilde = pd.read_csv(f"{file_log}/sents_index_tilde.csv", index_col=0)


# Convert date column to datetime and set as index
sents_tilde["Date"] = pd.to_datetime(sents_tilde["Date"])
sents_tilde.set_index("Date", inplace=True)

# Open file to write
with open(f"{file_log}/correlation_analysis_results.txt", "w") as f:

    # Correlation matrix
    corr = sents_tilde.corr(method='pearson')
    f.write('p_hat Pearson Correlation Matrix:\n')
    f.write(corr.to_string())
    f.write("\n\n")

    # Summary statistics
    stats = {
        "mean_ret": sents_tilde['ret'].mean(),
        "std_ret": sents_tilde['ret'].std(),
        "min_ret": sents_tilde['ret'].min(),
        "max_ret": sents_tilde['ret'].max(),
        "mean_vol": sents_tilde['vol'].mean(),
        "std_vol": sents_tilde['vol'].std(),
        "min_vol": sents_tilde['vol'].min(),
        "max_vol": sents_tilde['vol'].max(),
    }

    f.write("Summary Statistics:\n")
    for key, value in stats.items():
        f.write(f"{key}: {value:.6f}\n")
    f.write("\n")

    # Pearson correlations and p-values
    f.write("Pearson Correlations (Original Data):\n")
    pairs = [
        ('ret', 'vol'),
        ('ret', 'lm'),
        ('ret', 'index'),
        ('vol', 'lm'),
        ('vol', 'index'),
        ('index', 'lm')  # optional
    ]


    f.write("Pearson Correlations (Tilde Data):\n")
    for x, y in pairs:
        r, p = pearsonr(sents_tilde[x], sents_tilde[y])
        f.write(f"{x} vs {y}: correlation = {r:.4f}, p-value = {p:.4g}\n")

print("Results saved to 'correlation_analysis_results.txt'")
