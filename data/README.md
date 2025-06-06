# Data Directory Structure for Data Career Navigator

This directory organizes datasets used in the Data Career Navigator project according to industry-standard data lake conventions: **Bronze**, **Silver**, and **Gold** layers. This structure ensures a clear, logical progression from raw data ingestion to high-quality, analysis-ready datasets.

## Layers

### Bronze

- **Description:** Contains raw, unprocessed data exactly as ingested from source(s). Data here may be messy, incomplete, or inconsistent.
- **Purpose:** Serves as the immutable record of original data for reproducibility and auditing.
- **Location:** [`data/bronze/`](bronze/)

### Silver

- **Description:** Contains cleaned, validated, and transformed data. Typical processing includes removing duplicates, handling missing values, and standardizing formats.
- **Purpose:** Provides structured datasets ready for most analyses and further enrichment.
- **Location:** [`data/silver/`](silver/)

### Gold

- **Description:** Contains curated, high-quality datasets optimized for reporting, analytics, and visualization. Data here is aggregated, enriched, and ready for direct use in dashboards or machine learning models.
- **Purpose:** Enables fast, reliable insights for end-users and stakeholders.
- **Location:** [`data/gold/`](gold/)

---

**Note:**  
Adopting this layered approach helps maintain data quality, traceability, and clarity throughout the analytics workflow. It also aligns with best practices in data engineering, making collaboration and scaling easier.

For more details on the project, see the main [README.md](../README.md).