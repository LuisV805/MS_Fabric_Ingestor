To align with your updated `API_Ingestor.py`, the **README.md** needs to reflect the shift toward a CSV-driven metadata workflow and the specific method names you've implemented (e.g., using `apply_column_mapping` instead of individual transformation calls).

Here is the updated version:

---

# MS_Fabric_Ingestor
### *A Metadata-Driven Data Engineering Framework for Microsoft Fabric*

## **Overview**
**MS_Fabric_Ingestor** is a modular Python framework designed for the Microsoft Fabric ecosystem. It provides a standardized, object-oriented approach to ingesting data from external APIs and transforming it into refined tables within **OneLake**.

By using a **metadata-driven design**, this framework decouples transformation logic from the core code. Users manage column renames and data types via a simple CSV template, making the pipeline highly maintainable and "drop-in" ready for any Fabric notebook environment.

---

## **Core Capabilities**

### **1. Enterprise Ingestion**
* **Paginated API Handling:** Native support for batch-based API requests to ensure stable ingestion of large datasets.
* **Delta Lake Integration:** Streamlined methods for saving and overwriting Delta tables directly into the Fabric Lakehouse with schema evolution support.

### **2. Automated Metadata Workflow**
* **Template Generation:** Automatically generates a CSV mapping skeleton from any Spark DataFrame.
* **User-Driven Configuration:** Allows users to define new column names, target tables, and specific data types (Key, Text, Metric, Boolean, Date, Currency) in a familiar CSV format.
* **Single-Pass Transformation:** Applies all renames and type-casting logic in a single method call based on the user-maintained mapping file.

### **3. Specialized Type Casting**
* **Currency:** Strips symbols and casts to specified decimal precision.
* **Booleans:** Maps diverse string/integer flags (e.g., Y/N, 1/0) to true Boolean types using dictionary-based mapping.
* **Dates:** Standardizes inconsistent date strings into Spark `DateType` using custom format strings.
* **Null Handling:** Automatically fills NULL values in Key and Text columns to prevent "Blank" values in downstream Power BI models.

### **4. Governance & Traceability**
* **Surrogate Key Generation:** Adds deterministic "ID" columns via SHA-256 hashing of business keys.
* **Centralized Audit Logging:** Records every table update, including row counts and source URLs, into a centralized `z_model_tables` audit log.

---

## **Project Structure**
* `API_Ingestor.py`: The core class library containing all transformation, mapping, and ingestion logic.
* `Notebook_Execution.ipynb`: A sample orchestration layer demonstrating the end-to-end metadata-driven workflow.

---

## **Getting Started**

### **1. Implementation**
Upload `API_Ingestor.py` to the `/Files/libraries/` section of your Lakehouse. Then, initialize it in your notebook:

```python
import sys
sys.path.append("/lakehouse/default/Files/libraries/")
from API_Ingestor import Ingestor

ingestor = Ingestor()
```

### **2. The Metadata Workflow**
1. **Fetch and Generate Template:**
   ```python
   raw_df = ingestor.fetch_data(spark, API_URL, batch_size=1000, total_limit=5000)
   # Saves a template to /Files/column_mapping/my_table.csv
   ingestor.save_to_table(spark, raw_df, "my_table", generate_column_mapping=True)
   ```
2. **Edit the CSV:** Download the generated CSV from the Lakehouse, define your `new_name` and `data_type` in Excel, and re-upload it.
3. **Apply and Refine:**
   ```python
   # Automatically renames and casts based on your CSV
   refined_df = ingestor.apply_column_mapping(spark, raw_df, "my_table")
   
   # Add Surrogate Key and Save
   final_df = ingestor.add_surrogate_key(refined_df, ["provider_id", "service_code"])
   ingestor.save_to_table(spark, final_df, "s_refined_table", source_url=API_URL)
   ```

---

## **Architecture Philosophy**
This framework follows the **Medallion Architecture** (Bronze -> Silver -> Gold). It prioritizes **Idempotency** and **Metadata Portability**, ensuring that downstream Power BI Semantic Models remain performant and accurate without hard-coded technical debt.

---

## **License**
This project is licensed under the **MIT License**—free for use, modification, and distribution in both personal and commercial environments.