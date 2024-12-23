import csv

# List of companies with their stock symbols and CIK numbers
companies = [
    {"Company": "Volkswagen", "Symbol": "VWAGY", "CIK": "0000037303"},
    {"Company": "Deutsche Bank", "Symbol": "DB", "CIK": "0001159508"},
    {"Company": "Tencent", "Symbol": "TCEHY", "CIK": "0001065521"},
    {"Company": "Turkcell", "Symbol": "TKC", "CIK": "0001117057"},
    {"Company": "Helios Towers", "Symbol": "HTWS.L", "CIK": "0001795079"},
    {"Company": "China Cinda Asset Management", "Symbol": "CDAHF", "CIK": "0001575965"},
    {"Company": "Barclays", "Symbol": "BCS", "CIK": "0000312070"},
    {"Company": "Aldar Properties", "Symbol": "ALDRY", "CIK": "0001495219"},
    {"Company": "Hormel Foods", "Symbol": "HRL", "CIK": "0000048465"},
    {"Company": "American Express", "Symbol": "AXP", "CIK": "0000004962"},
    {"Company": "Delta Air Lines", "Symbol": "DAL", "CIK": "0000027904"},
    {"Company": "Target", "Symbol": "TGT", "CIK": "0000027419"}
]

# Specify the CSV file name
csv_file = 'company_symbols_cik.csv'

# Write data to CSV
with open(csv_file, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["Company", "Symbol", "CIK"])
    writer.writeheader()
    for company in companies:
        writer.writerow(company)

print(f"CSV file '{csv_file}' has been created successfully.")
