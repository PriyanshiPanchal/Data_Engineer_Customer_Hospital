# Airflow DAG: Customer Data Pipeline

This repository contains an **Airflow DAG** implementation for processing customer data stored in **Google BigQuery**. The pipeline handles billions of rows daily, extracts relevant customer information, processes it with derived columns, and loads it into country-specific tables (`Table_India` and `Table_USA`).

---

## **Pipeline Overview**

The pipeline performs the following tasks:
1. **Table Creation**:
   - Creates the following tables in BigQuery:
     - `Staging_Customers`: A staging table to temporarily store raw customer data.
     - `Table_India` and `Table_USA`: Country-specific tables with derived columns like `Age` and `Days_Since_Last_Consulted`.

2. **Data Ingestion**:
   - Loads raw customer data into the `Staging_Customers` table.

3. **Data Processing**:
   - Filters the latest record for each customer (`Customer_Id`) based on the most recent `Last_Consulted_Date`.
   - Derives the following columns:
     - **`Age`**: The customer's age based on their `DOB`.
     - **`Days_Since_Last_Consulted`**: The number of days since the customer's last consultation.
     - **`Flag_Days_GT_30`**: A boolean flag indicating if `Days_Since_Last_Consulted` is greater than 30.

4. **Data Loading**:
   - Inserts processed customer data into the appropriate country-specific tables:
     - **`Table_India`**: For customers in India.
     - **`Table_USA`**: For customers in the USA.

---

## **Airflow DAG Implementation**

The DAG is implemented as follows:
- **DAG Name**: `customer_data_pipeline`
- **Schedule**: Runs daily (`@daily`).
- **Tasks**:
  1. **Create Tables**: Creates the required BigQuery tables.
  2. **Load Staging Data**: Loads raw customer data into the staging table.
  3. **Filter Latest Data**: Extracts the latest record for each customer using a `ROW_NUMBER()` window function.
  4. **Load Country Tables**: Inserts processed data into `Table_India` and `Table_USA`.

---

## **Derived Columns**

The pipeline generates the following derived columns:
1. **`Age`**:
   - Formula: `EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM DOB)`
   - Calculates the customer's age in years.

2. **`Days_Since_Last_Consulted`**:
   - Formula: `DATE_DIFF(CURRENT_DATE(), Last_Consulted_Date, DAY)`
   - Calculates the number of days since the customer's last consultation.

3. **`Flag_Days_GT_30`**:
   - Formula: `DATE_DIFF(CURRENT_DATE(), Last_Consulted_Date, DAY) > 30`
   - Flags rows where `Days_Since_Last_Consulted` exceeds 30 days.

---

## **Table Definitions**

### **Staging Table (`Staging_Customers`)**
| Column               | Type    | Description                          |
|----------------------|---------|--------------------------------------|
| `Customer_Name`      | STRING  | Name of the customer.                |
| `Customer_Id`        | STRING  | Unique ID for the customer.          |
| `Open_Date`          | DATE    | Date the customer account was opened.|
| `Last_Consulted_Date`| DATE    | Date of the customer's last visit.   |
| `Vaccination_Id`     | STRING  | Vaccination type ID.                 |
| `Doctor_Consulted`   | STRING  | Name of the consulting doctor.       |
| `State`              | STRING  | State where the customer resides.    |
| `Country`            | STRING  | Country where the customer resides.  |
| `DOB`                | DATE    | Date of birth of the customer.       |
| `Is_Active`          | STRING  | Indicates if the customer is active. |

---

### **Country-Specific Tables (`Table_India` and `Table_USA`)**
| Column                       | Type    | Description                                      |
|------------------------------|---------|--------------------------------------------------|
| `Customer_Name`              | STRING  | Name of the customer.                            |
| `Customer_Id`                | STRING  | Unique ID for the customer.                      |
| `Open_Date`                  | DATE    | Date the customer account was opened.            |
| `Last_Consulted_Date`        | DATE    | Date of the customer's last visit.               |
| `Vaccination_Id`             | STRING  | Vaccination type ID.                             |
| `Doctor_Consulted`           | STRING  | Name of the consulting doctor.                   |
| `State`                      | STRING  | State where the customer resides.                |
| `Country`                    | STRING  | Country where the customer resides.              |
| `DOB`                        | DATE    | Date of birth of the customer.                   |
| `Is_Active`                  | STRING  | Indicates if the customer is active.             |
| **`Age`**                    | INT64   | Derived column: Age of the customer.             |
| **`Days_Since_Last_Consulted`** | INT64   | Derived column: Days since last consultation.     |
| **`Flag_Days_GT_30`**        | BOOLEAN | Derived column: Flag for days > 30 since consultation.|

---

## **Setup Instructions**

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/<your-username>/airflow-customer-data-pipeline.git
   cd airflow-customer-data-pipeline
