# Automated Historical Exchange Rate Data Pipeline (2015–2025)

A production-ready, automated data pipeline built with **Python**, **Apache Airflow**, and **Google Cloud Platform (GCP)** to collect, process, store, and visualize historical exchange rate data.

---

## Project Overview

- **Goal:** Build an end-to-end pipeline to collect historical exchange rate data (2015–2025), transform it, and load it into a data warehouse for visualization and reporting.
- **Scope:** Fully automated via Airflow, scalable for future data additions, cloud-native with GCS and BigQuery.

---

## Data Source

This project uses historical exchange rate data from exchangerate.host — a free and reliable public API.

- **Base URL:** https://exchangerate.host/
- **API Key** : Required (Free 100 request/monthly)
  
Example API call :

```json
{
    "success": true,
    "terms": "https://exchangerate.host/terms",
    "privacy": "https://exchangerate.host/privacy",
    "timestamp": 1430401802,
    "source": "USD",
    "quotes": {
        "USDAED": 3.672982,
        "USDAFN": 57.8936,
        "USDALL": 126.1652,
        "USDAMD": 475.306,
        "USDANG": 1.78952,
        "USDAOA": 109.216875,
        "USDARS": 8.901966,
        "USDAUD": 1.269072,
        "USDAWG": 1.792375,
        "USDAZN": 1.04945,
        "USDBAM": 1.757305,
    [...]
    }
}
```
---

## Tech Stack

| Tool/Service       | Role                                          |
|--------------------|-----------------------------------------------|
| Python             | API calls, data transformation, CSV export    |
| Apache Airflow     | Workflow orchestration & scheduling           |
| Google Cloud Storage (GCS) | Raw data storage (CSV files)           |
| BigQuery           | Data warehouse for analytics                  |
| Looker Studio      | Data visualization & reporting                |
| Docker             | Local development and environment consistency |

---
## Pipeline Architecture

![Pipeline Architecture](/img/architecture.png)
