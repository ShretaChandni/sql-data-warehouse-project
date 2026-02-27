# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as portfolio projects, highlights industry best practices in data engineering and analytics.

----------------------------------------------------------------------------------------------------------------------------
## Project Overview
This project involves:
  
  1. Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
  2. ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse. 
  3. Data Modeling: Developing fact and dimension tables optimized for analytical queries.
  4. Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

This repository showcases expertise in:
  - SQL Development
  - Data Architect
  - Data Engineering
  - ETL Pipeline Developer
  - Data Modeling
  - Data Analytics
----------------------------------------------------------------------------------------------------------------------------    
## Project Requirements

### Building the Data Warehouse (Data Engineering)

## Objective 
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues before analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only: historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

----------------------------------------------------------------------------------------------------------------------------

### BI: Analytics & Reporting (Data Analytics)

### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

For more details, refer to: docs/requirements.md
----------------------------------------------------------------------------------------------------------------------------
## Data Architecture

The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:

<img width="1023" height="589" alt="image" src="https://github.com/user-attachments/assets/d1ce8ee9-3675-412e-b084-4baaa328b265" />

1. Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into the SQL Server Database.
2. Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics. 

## DataWarehouse 

<img width="898" height="504" alt="image" src="https://github.com/user-attachments/assets/7190d215-f72f-40ec-968b-3d83af8a86df" />

## Data Source illustration 

<img width="943" height="417" alt="image" src="https://github.com/user-attachments/assets/501d359e-09d2-4a22-b03f-b2d8f442be3f" />

## Data Model 

<img width="993" height="576" alt="image" src="https://github.com/user-attachments/assets/678bac53-131f-4737-b21e-a4fc63e9dfb0" />

----------------------------------------------------------------------------------------------------------------------------
## License

This project is licensed under the [MIT License](License). 

## About me 

Hi there! I'm **Shreta Chandni**. I am an IT professional on a mission to keep learning and skill up as long as possible. 
Let's stay in touch! Feel free to connect with me shally2009@gmail.com 


