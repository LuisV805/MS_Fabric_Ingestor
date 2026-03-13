# MS_Fabric_Ingestor
### *A Metadata-Driven Data Engineering & Quality Framework for Microsoft Fabric*

## **Overview**
**MS_Fabric_Ingestor** is a modular Python framework designed specifically for the Microsoft Fabric ecosystem. It provides a standardized, object-oriented approach to ingesting data from external APIs and transforming it into refined, analyst-ready tables within **OneLake**.

By abstracting complex PySpark operations into a reusable library, this framework allows data professionals to build scalable, idempotent pipelines that prioritize data integrity and governance. It is designed to be lightweight and "drop-in" ready for any Fabric notebook environment.

---

## **Core Capabilities**

### **1. Enterprise Ingestion**
* **Paginated API Handling:** Native support for batch-based API requests to ensure stable ingestion of large datasets without memory overflows.
* **Delta Lake Integration:** Streamlined methods for saving and overwriting Delta tables directly into the Fabric Lakehouse.

### **2. Transformation & Standardization**
* **Metadata-Driven Mapping:** Decouples business logic from execution by using configuration dictionaries for column renaming.
* **Specialized Type Casting:**
    * **Currency:** Automatically strips symbols ($ ,) and casts to `Decimal(18,2)`.
    * **Booleans:** Maps diverse string/integer flags (Y/N, 1/0, Yes/No) to true Boolean types via `create_map` optimization.
    * **Dates:** Standardizes inconsistent date strings into Spark `DateType`.

### **3. Data Quality (DQ) & Governance Suite**
* **Statistical Outlier Detection:** Identifies numerical anomalies using the **Interquartile Range (IQR)** method with optional grouping (e.g., finding outliers per specific procedure code).
* **Logical Validation:**
    * **Range Violations:** Flags values that violate fixed business bounds (e.g., negative payment amounts).
    * **Date Typo Detection:** Identifies "impossible" dates or records falling outside valid historical eras.
* **Completeness Reporting:** Generates a real-time summary of NULL percentages across all columns to monitor data "missingness."
* **Audit Metadata:** Enriches every record with `load_timestamp` and `source_system` identifiers for end-to-end lineage and traceability.

---

## **Project Structure**
* `Ingestor.py`: The core class library containing all transformation, validation, and ingestion logic.
* `Notebook_Execution.ipynb`: A sample orchestration layer demonstrating how to instantiate the class and chain transformations fluently.

---

## **Getting Started**

### **Implementation**
Upload the `Ingestor.py` file to the `Files` section of your Microsoft Fabric Lakehouse. You can then import it into any Fabric Notebook:

```python
import sys
# Path to your library in the Fabric Lakehouse
sys.path.append("/lakehouse/default/Files/libraries/")
from Ingestor import Ingestor

# Initialize the framework
ingestor = Ingestor()

# Example ETL Process
raw_df = ingestor.fetch_data(spark, API_URL, "bronze_table", batch_size=1000)

refined_df = ingestor.update_column_names(raw_df, MAPPING_CONFIG)
refined_df = ingestor.transform_currency(CURRENCY_FIELDS)
refined_df = ingestor.transform_boolean_columns(BOOL_MAPPING)
refined_df = ingestor.flag_outliers("avg_paid_amount", group_cols=["service_code"])
refined_df = ingestor.add_audit_metadata(API_URL)

# Save to Silver/Gold Layer
ingestor.save_to_table(refined_df, "s_refined_table")
```

### **Monitoring Quality**
Use the built-in reporting tool to audit your data before it reaches the Gold layer:

```Python
# Returns a dictionary and prints a null-percentage report
ingestor.generate_completeness_report(refined_df)
```

### **Architecture Philosophy
This framework follows the Medallion Architecture (Bronze -> Silver -> Gold). It emphasizes Idempotency (the ability to re-run the pipeline without duplicating data) and Type-Safety, ensuring that downstream Power BI Semantic Models remain performant and accurate.

---

## **License**
This project is licensed under the MIT License—free for use, modification, and distribution in both personal and commercial environments.
