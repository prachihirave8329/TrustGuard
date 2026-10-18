
import csv
import random
from pathlib import Path

random.seed(42)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "retail_transactions.csv"

customers = ["C101", "C102", "C103", "C104", "C105"]
products = {
    "P101": ("Laptop", 55000),
    "P102": ("Headphones", 2000),
    "P103": ("Keyboard", 1500),
    "P104": ("Mouse", 800),
    "P105": ("Monitor", 12000),
}

rows = []

# Generate 100 realistic retail transactions.
for transaction_id in range(1, 101):
    product_id = random.choice(list(products))
    product_name, unit_price = products[product_id]
    quantity = random.randint(1, 4)

    rows.append({
        "transaction_id": f"T{transaction_id:04d}",
        "customer_id": random.choice(customers),
        "product_id": product_id,
        "product_name": product_name,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": quantity * unit_price,
        "transaction_date": f"2026-10-{random.randint(1, 15):02d}",
    })

# Add intentional data quality issues for testing.
rows[5]["customer_id"] = ""             # Missing customer
rows[12]["quantity"] = -2               # Invalid quantity
rows[20]["unit_price"] = -500           # Invalid price
rows[30]["transaction_date"] = "bad-date"
rows[40]["transaction_id"] = ""         # Missing transaction ID
rows.append(rows[0].copy())             # Duplicate transaction

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Dataset created: {OUTPUT_FILE}")
print(f"Total records generated: {len(rows)}")
print("Expected: 101 records, including one duplicate.")
