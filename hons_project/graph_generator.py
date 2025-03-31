import pandas as pd
import matplotlib.pyplot as plt

fig_loc = './outcome/figures_df_analysis_report_summary_3'

# Load data points from CSV files
mod_avg_ret = pd.read_csv(f'{fig_loc}/mod_avg_ret.csv', index_col=0, parse_dates=True)
mod_kal_ret = pd.read_csv(f'{fig_loc}/mod_kal_ret.csv', index_col=0, parse_dates=True)
mod_avg_vol = pd.read_csv(f'{fig_loc}/mod_avg_vol.csv', index_col=0, parse_dates=True)
mod_kal_vol = pd.read_csv(f'{fig_loc}/mod_kal_vol.csv', index_col=0, parse_dates=True)
lm_avg = pd.read_csv(f'{fig_loc}/lm_avg.csv', index_col=0, parse_dates=True)
lm_kal = pd.read_csv(f'{fig_loc}/lm_kal.csv', index_col=0, parse_dates=True)

port_val = pd.read_csv(f'{fig_loc}/sp500_index.csv', index_col=0)
port_val = port_val[port_val['Date'] >= '2014-01-01']
port_val['Date'] = pd.to_datetime(port_val['Date'], format="%Y-%m-%d", errors='coerce')
port_val.set_index('Date', inplace=True)
port_val["index"] = pd.to_numeric(port_val["index"], errors='coerce')


sents_tilde = pd.read_csv(f'{fig_loc}/sents_index_tilde.csv', index_col=0)



# Apply smoothing
window_size = 7
mod_kal_ret = mod_kal_ret.rolling(window=window_size).mean()
mod_kal_vol = mod_kal_vol.rolling(window=window_size).mean()
lm_kal = (lm_kal + 0.5 - lm_kal.mean()).rolling(window=window_size).mean()

mod_kal_ret = mod_kal_ret[mod_kal_ret.index >= '2014-01-01']
mod_kal_vol = mod_kal_vol[mod_kal_vol.index >= '2014-01-01']
lm_kal = lm_kal[lm_kal.index >= '2014-01-01']

# Plotting RET Sentiment
plt.plot(mod_avg_ret, label='unfiltered')
plt.plot(mod_kal_ret, label='filtered', linewidth=1, linestyle='--')
plt.xlabel('Date')
plt.ylabel('Sentiment')
plt.legend(loc='upper right')
plt.legend(fontsize=18)
plt.title('RET Sentiment')
plt.tight_layout()
plt.savefig(f'{fig_loc}/ret_filter', dpi=500)
# plt.show()

# Plotting VOL Sentiment
plt.plot(mod_avg_vol, label='unfiltered')
plt.plot(mod_kal_vol, label='filtered', linewidth=1, linestyle='--')
plt.xlabel('Date')
plt.ylabel('Sentiment')
plt.legend(loc='upper right')
plt.legend(fontsize=18)
plt.title('VOL Sentiment')
plt.tight_layout()
plt.savefig(f'{fig_loc}/vol_filter', dpi=500)
# plt.show()

# Plotting LM Sentiment
plt.plot(lm_avg, label='unfiltered')
plt.plot(lm_kal, label='filtered')
plt.xlabel('Date')
plt.ylabel('Sentiment')
plt.legend(loc='upper right')
plt.title('LM Sentiments')
plt.tight_layout()
plt.savefig(f'{fig_loc}/LM_filter', dpi=500)
# plt.show()

# Plotting Portfolio
fig, ax = plt.subplots()
ax.plot(port_val, color='silver', linestyle='dashed', label='S&P 500 Index')
ax.set_xlabel('Date')
ax.set_ylabel('S&P 500 Index')
ax2 = ax.twinx()

ax2.plot(mod_kal_ret, label=r'${\tilde{p}^{RET}}$', linewidth=1, alpha=0.7, linestyle='-')
ax2.plot(mod_kal_vol, label=r'${\tilde{p}^{VOL}}$', linewidth=1, alpha=0.7, linestyle='-')
# ax2.plot(lm_kal, label=r'${\tilde{p}^{LM}}$', linewidth=1, alpha=0.5, linestyle='-')
ax2.set_ylabel('Sentiment Score')
ax2.set_ylabel('Sentiment Score' + '(' + r'${\tilde{p}}$' + ')')
fig.legend(bbox_to_anchor=(0.33, 0.7))
fig.autofmt_xdate(rotation=50)
plt.title('Buy-Side S&P 500 Sentiment Score Prediction')
plt.savefig(f'{fig_loc}/Buy-Side S&P 500 Sentiment Prediction', dpi=500)
# plt.show()

# Plotting Correlation
fig, ax = plt.subplots()
ax.plot(port_val, color='silver', linestyle='dashed', label='S&P 500 Index')
ax.set_xlabel('Date')
ax.set_ylabel('S&P 500 Index')
ax2 = ax.twinx()
ax2.plot(mod_kal_ret, label='RET')
ax2.plot(mod_kal_vol, label='VOL')
ax2.plot(lm_kal, label='LM')
ax2.set_ylabel('Sentiment(RET, VOL, LM)')
fig.legend(bbox_to_anchor=(0.33, 0.7))
plt.tight_layout()
plt.savefig(f'{fig_loc}/correlation_plot', dpi=500)
