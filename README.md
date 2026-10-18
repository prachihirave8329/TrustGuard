# TrustGuard — Data Quality Pipeline

TrustGuard is a PySpark-based data quality pipeline that validates retail transaction data, identifies invalid records, removes duplicate transactions, and generates data quality reports. This project demonstrates essential data engineering concepts such as data cleaning, validation, error handling, automated testing, and record reconciliation.

## 🚀 Project Overview

Real-world datasets often contain missing values, duplicate records, invalid dates, and incorrect numerical values. TrustGuard addresses these problems by separating valid transactions from rejected records and generating a summary report of the pipeline results.

## ✨ Key Features

- **Data Validation:** Validates retail transaction records.
- **Duplicate Detection:** Identifies duplicate transaction IDs.
- **Missing Value Handling:** Detects missing transaction IDs and customer IDs.
- **Numerical Validation:** Identifies invalid quantities and negative unit prices.
- **Date Validation:** Detects invalid transaction dates.
- **Data Segregation:** Stores clean and rejected records separately.
- **Quality Reporting:** Generates a CSV report containing pipeline metrics.
- **Record Reconciliation:** Verifies that all input records are accounted for.
- **Automated Testing:** Uses pytest to test data quality rules.

## 🛠️ Tech Stack

- Python
- Apache PySpark
- pytest
- CSV
- Git and GitHub

## 📁 Project Structure

```text
TrustGuard/
├── data/
│   ├── raw/
│   │   └── retail_transactions.csv
│   ├── clean/
│   │   └── transactions.csv
│   └── rejected/
│       └── transactions.csv
├── src/
│   ├── generate_data.py
│   ├── data_quality.py
│   └── pipeline.py
├── reports/
│   └── quality_summary.csv
├── tests/
│   └── test_data_quality.py
├── requirements.txt
├── .gitignore
└── README.md
```

## 🔍 Data Quality Rules

| Validation Rule | Description |
|---|---|
| Transaction ID | Must be present and unique |
| Customer ID | Must not be missing |
| Quantity | Must be a valid positive number |
| Unit Price | Must be a valid non-negative amount |
| Transaction Date | Must contain a valid date |
| Record Reconciliation | Input records must equal clean records plus rejected records |

## 📊 Pipeline Results

The pipeline was successfully executed on a dataset containing 101 records.

| Metric | Result |
|---|---:|
| Total Input Records | 101 |
| Clean Records | 95 |
| Rejected Records | 6 |
| Record Reconciliation | PASSED |
| Pipeline Status | SUCCESS |

The rejected records include examples of missing identifiers, duplicate transaction IDs, invalid quantities, negative prices, and invalid dates.

## ⚙️ Installation and Setup

### 1. Clone the Repository

```bash
git clone https://github.com/prachihirave8329/TrustGuard.git
cd TrustGuard
```

### 2. Create a Virtual Environment

For Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

```bash
python -m pip install -r requirements.txt
```

### 4. Run the Pipeline

```bash
python src/generate_data.py
python src/pipeline.py
```

The pipeline processes the raw transaction dataset and generates clean records, rejected records, and a quality summary report.

### 5. Run Automated Tests

```bash
python -m pytest tests/test_data_quality.py -v
```

## 📂 Output Files

- `data/clean/transactions.csv` — contains validated transaction records.
- `data/rejected/transactions.csv` — contains rejected records for further investigation.
- `reports/quality_summary.csv` — contains record counts and pipeline summary metrics.

## 🎯 Learning Outcomes

Through this project, I gained practical experience in:

- Building a data processing pipeline using PySpark.
- Implementing data quality and validation rules.
- Handling missing, duplicate, and invalid records.
- Writing automated tests using pytest.
- Generating CSV outputs and data quality reports.
- Using Git and GitHub for version control.

## 🔮 Future Enhancements

- Integrate Apache Airflow for workflow orchestration.
- Store processed data in a data lake or cloud storage.
- Add structured logging and pipeline monitoring.
- Introduce configurable data validation rules.
- Build a dashboard to visualize data quality metrics.

## 👩‍💻 Author

**Prachi Hirave**

GitHub: [prachihirave8329](https://github.com/prachihirave8329)

## 📌 Project Status

Completed initial implementation with successful pipeline execution and automated data quality tests.

---

*TrustGuard — Building trust in data through validation, quality checks, and reconciliation.*
