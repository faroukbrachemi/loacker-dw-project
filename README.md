# Loacker Quality & Sustainability Data Warehouse

A data warehouse project designed to support the analysis of **production quality and sustainability performance** at Loacker.

The project covers the complete data warehouse lifecycle, from data generation and ETL to dimensional modeling, OLAP analysis, SQL querying, and interactive visualization using Power BI.

---

## 🎥 Project Demo

A short video demonstrates the implemented data warehouse, analytical queries, and Power BI dashboard.

**[▶️ Watch the Project Demo](Dashboard_Demo.mov)**

The demo includes the interactive Power BI dashboard with filters for:

* Year
* Month
* Product
* Plant
* Shift

and visualizations for production, quality, and sustainability indicators.

---

## 📊 Dashboard

The Power BI dashboard provides an interactive overview of the main Quality & Sustainability KPIs, including:

* Total Carbon Emissions
* Average Quality Score
* Total Units Produced
* Total Units Rejected
* Monthly Units Produced by Year
* Carbon Emissions per Unit by Product Line
* Carbon Emissions per Unit by Plant

Users can interactively filter the results by time period, product, plant, and production shift.

---

## 🏗️ Data Warehouse Architecture

The data warehouse follows a **dimensional modeling approach** based on a star schema.

### Fact Table

`fact_batch_quality_sustainability`

Main measures:

* `units_produced`
* `units_rejected`
* `quality_test_score`
* `carbon_emissions_kg_co2e`
* `waste_kg`
* `energy_kwh`
* `water_liters`

### Dimension Tables

* `date_dim`
* `product_dim`
* `plant_dim`
* `batch_dim`

The fact table has a batch-level grain and is connected to the four dimensions through foreign keys.

---

## 🔄 ETL Process

The ETL pipeline transforms generated raw data into a clean and structured data warehouse.

The process consists of:

1. **Data Generation**
   Production data is generated using Python.

2. **Staging**
   The generated CSV data is loaded into a staging table.

3. **Data Cleaning and Transformation**
   Invalid, inconsistent, and dirty values are cleaned and transformed using SQL.

4. **Dimension Loading**
   The Date, Product, Plant, and Batch dimensions are populated.

5. **Fact Loading**
   The cleaned production records are loaded into the fact table.

6. **Analysis**
   SQL queries and Power BI are used to analyze the resulting data warehouse.

---

## 📁 Repository Structure

```text
├── README.md
├── Project_Report.pdf
├── Dashboard_Demo.mov
├── data_generation_script.py
├── data_warehousing_script.sql
└── dirty_data_generated.csv
```

### File Description

| File                          | Description                                                                                                 |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `Project_Report.pdf`          | Complete project report covering the conceptual, logical, physical, ETL, querying, and visualization phases |
| `Dashboard_Demo.mov`          | Video demonstration of the project and Power BI dashboard                                                   |
| `data_generation_script.py`   | Python script used to generate the production dataset                                                       |
| `data_warehousing_script.sql` | SQL script used to create, clean, populate, and query the data warehouse                                    |
| `dirty_data_generated.csv`    | Generated raw dataset used as input for the ETL process                                                     |
| `README.md`                   | Project documentation                                                                                       |

---

## 🛠️ Technologies Used

* **Python** — data generation
* **PostgreSQL** — data warehouse implementation
* **SQL** — ETL, dimensional modeling, and analytical queries
* **Power BI** — interactive dashboard and data visualization
* **CSV** — raw data exchange format

---

## 📈 Analytical Queries

The project demonstrates several analytical operations, including:

* `ROLLUP`
* `CUBE`
* `GROUPING SETS`
* `RANK`
* `NTILE`
* Window functions
* Period-to-period comparisons
* Aggregation across multiple dimensions

These queries are used to analyze production quality and sustainability from different perspectives.

---

## 📄 Project Report

The complete documentation of the project is available here:

**[📑 View the Project Report](Project_Report.pdf)**

The report covers:

* Business requirements
* Bus matrix
* Dimensional Fact Model
* Star schema
* ETL process
* Physical implementation
* SQL analytical queries
* Example data and query results
* Power BI dashboard

---

## 🚀 How to Run

### 1. Generate the data

Run:

```bash
python data_generation_script.py
```

This generates the raw production CSV file.

### 2. Set up PostgreSQL

Create a PostgreSQL database and execute:

```text
data_warehousing_script.sql
```

The SQL script creates the staging tables, dimensions, fact table, and analytical queries.

### 3. Load the generated CSV

Update the CSV path in the SQL script if necessary and load the generated dataset into the staging table.

### 4. Run the analytical queries

Execute the provided SQL queries in PostgreSQL/pgAdmin to reproduce the analysis.

### 5. Open the dashboard

The resulting data can be used in Power BI to explore the quality and sustainability KPIs interactively.

---

## 🎯 Main Objective

The objective of this project is to demonstrate how a dimensional data warehouse can transform raw manufacturing data into a structured analytical environment that supports:

* Production monitoring
* Quality analysis
* Reject-rate analysis
* Sustainability analysis
* Carbon-emission monitoring
* Time-based analysis
* Plant and product comparison
* Data-driven decision making
