# Hypothetical market capitalizations for the companies in billions at the end of the training window, which is 2019-12-31
market_caps = {
    'AAPL': 1287,  # Apple
    'MSFT': 1200,  # Microsoft
    'AMZN': 920.22,  # Amazon
    'AVGO': 125,   # Broadcom
    'META': 585.37,   # Meta (formerly Facebook)
    'NVDA': 144,   # NVIDIA
    'TSLA': 75.71,   # Tesla - 2019-12-31: 75.71 B, 2020-12-31: 668.09 B
    'GOOG': 921.13, # Alphabet (Google)
    'COST': 129.84    # Costco
}

# Calculate the total market capitalization
total_market_cap = sum(market_caps.values())

# Calculate the weights for each company
weights = {ticker: cap / total_market_cap for ticker, cap in market_caps.items()}

print(weights)
