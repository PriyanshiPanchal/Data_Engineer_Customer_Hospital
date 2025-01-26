from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG
with DAG(
    dag_id='customer_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Process customer data and load into country-specific tables',
) as dag:

    # Step 1: Start DAG
    start_task = DummyOperator(task_id='start_pipeline')

    # Step 2: Create Tables
    create_tables = BigQueryInsertJobOperator(
        task_id='create_tables',
        configuration={
            "query": {
                "query": """
                -- Create Staging Table
                CREATE TABLE IF NOT EXISTS `project.dataset.Staging_Customers` (
                    Customer_Name STRING NOT NULL,
                    Customer_Id STRING NOT NULL,
                    Open_Date DATE NOT NULL,
                    Last_Consulted_Date DATE,
                    Vaccination_Id STRING,
                    Doctor_Consulted STRING,
                    State STRING,
                    Country STRING,
                    DOB DATE,
                    Is_Active STRING
                );

                -- Create Table for India
                CREATE TABLE IF NOT EXISTS `project.dataset.Table_India` (
                    Customer_Name STRING,
                    Customer_Id STRING,
                    Open_Date DATE,
                    Last_Consulted_Date DATE,
                    Vaccination_Id STRING,
                    Doctor_Consulted STRING,
                    State STRING,
                    Country STRING,
                    DOB DATE,
                    Is_Active STRING,
                    Age INT64,
                    Days_Since_Last_Consulted INT64,
                    Flag_Days_GT_30 BOOLEAN
                );

                -- Create Table for USA
                CREATE TABLE IF NOT EXISTS `project.dataset.Table_USA` (
                    Customer_Name STRING,
                    Customer_Id STRING,
                    Open_Date DATE,
                    Last_Consulted_Date DATE,
                    Vaccination_Id STRING,
                    Doctor_Consulted STRING,
                    State STRING,
                    Country STRING,
                    DOB DATE,
                    Is_Active STRING,
                    Age INT64,
                    Days_Since_Last_Consulted INT64,
                    Flag_Days_GT_30 BOOLEAN
                );
                """,
                "useLegacySql": False,
            }
        },
    )

    # Step 3: Load Data into Staging Table
    load_staging_data = BigQueryInsertJobOperator(
        task_id='load_staging_data',
        configuration={
            "query": {
                "query": """
                INSERT INTO `project.dataset.Staging_Customers`
                (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Doctor_Consulted, State, Country, DOB, Is_Active)
                VALUES
                ('Alice', 'CUST001', DATE('2023-01-01'), DATE('2023-02-01'), 'VAC01', 'Dr. Smith', 'KA', 'IND', DATE('1990-05-15'), 'Y'),
                ('Karen', 'CUST011', DATE('2022-01-01'), DATE('2023-01-15'), 'VAC01', 'Dr. Scott', 'CA', 'USA', DATE('1987-10-25'), 'Y');
                """,
                "useLegacySql": False,
            }
        },
    )

    # Step 4: Filter Latest Customer Data
    filter_latest_data = BigQueryInsertJobOperator(
        task_id='filter_latest_data',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `project.dataset.Latest_Customers` AS
                WITH Ranked_Customers AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY Customer_Id ORDER BY Last_Consulted_Date DESC) AS row_num
                    FROM `project.dataset.Staging_Customers`
                )
                SELECT * FROM Ranked_Customers WHERE row_num = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Step 5: Load Data into Table_India
    load_india_data = BigQueryInsertJobOperator(
        task_id='load_india_data',
        configuration={
            "query": {
                "query": """
                INSERT INTO `project.dataset.Table_India`
                (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Doctor_Consulted, State, Country, DOB, Is_Active, Age, Days_Since_Last_Consulted, Flag_Days_GT_30)
                SELECT
                    Customer_Name,
                    Customer_Id,
                    Open_Date,
                    Last_Consulted_Date,
                    Vaccination_Id,
                    Doctor_Consulted,
                    State,
                    Country,
                    DOB,
                    Is_Active,
                    -- Calculate Age
                    EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM DOB) AS Age,
                    -- Calculate Days Since Last Consulted
                    DATE_DIFF(CURRENT_DATE(), Last_Consulted_Date, DAY) AS Days_Since_Last_Consulted,
                    -- Flag for Days > 30
                    DATE_DIFF(CURRENT_DATE(), Last_Consulted_Date, DAY) > 30 AS Flag_Days_GT_30
                FROM `project.dataset.Latest_Customers`
                WHERE Country = 'IND';
                """,
                "useLegacySql": False,
            }
        },
    )

    # Step 6: Load Data into Table_USA
    load_usa_data = BigQueryInsertJobOperator(
        task_id='load_usa_data',
        configuration={
            "query": {
                "query": """
                INSERT INTO `project.dataset.Table_USA`
                (Customer_Name, Customer_Id, Open_Date, Last_Consulted_Date, Vaccination_Id, Doctor_Consulted, State, Country, DOB, Is_Active, Age, Days_Since_Last_Consulted, Flag_Days_GT_30)
                SELECT
                    Customer_Name,
                    Customer_Id,
                    Open_Date,
                    Last_Consulted_Date,
                    Vaccination_Id,
                    Doctor_Consulted,
                    State,
                    Country,
                    DOB,
                    Is_Active,
                    -- Calculate Age
                    EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM DOB) AS Age,
                    -- Calculate Days Since Last Consulted
                    DATE_DIFF(CURRENT_DATE(), Last_Consulted_Date, DAY) AS Days_Since_Last_Consulted,
                    -- Flag for Days > 30
                    DATE_DIFF(CURRENT_DATE(), Last_Consulted_Date, DAY) > 30 AS Flag_Days_GT_30
                FROM `project.dataset.Latest_Customers`
                WHERE Country = 'USA';
                """,
                "useLegacySql": False,
            }
        },
    )

    # Step 7: End DAG
    end_task = DummyOperator(task_id='end_pipeline')

    # Task Dependencies
    start_task >> create_tables >> load_staging_data >> filter_latest_data >> [load_india_data, load_usa_data] >> end_task
