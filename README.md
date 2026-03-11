# BNL Customer Lifecycle Analytics Model

**dbt + DuckDB Analytics Engineering Project**


# 1. Project Overview

This project implements a **BNL (Base–New–Lost) customer lifecycle model** using **dbt** and **DuckDB**.

The purpose of the model is to provide business leadership with a clear and consistent view of **customer lifecycle trends and revenue movement** across products and business units.

The pipeline converts raw transactional sales data into an **analytics-ready dimensional model** supporting downstream BI tools such as Tableau.

Key capabilities implemented in this project include:

* Customer identity resolution (handling customer merges)
* Rolling sales window calculations
* Business-defined lifecycle classification rules
* Business override governance
* Dimensional modeling (fact + dimension tables)
* Data quality and business logic tests
* Exposure definitions simulating Tableau dashboards

The repository demonstrates **modern analytics engineering practices using dbt**.

---

# 2. Business Context

Revenue operations leadership required a standardized way to measure **customer lifecycle performance**.

Specifically, leadership wanted to answer questions such as:

* How many customers are **New**, **Base**, or **Lost** each month?
* Which products are driving **customer growth or churn**?
* Are **existing customers expanding or contracting** their purchasing?

However, several issues prevented reliable analysis:

### Lack of Standard Definitions

Different business units used different definitions of:

* New customers
* Lost customers
* Base customers

### Customer Identity Fragmentation

Customers frequently:

* changed names
* merged accounts
* consolidated purchasing entities

Raw transaction systems therefore contained **multiple customer IDs representing the same customer**.

### Limited Analytical Data Modeling

The original systems were optimized for **transaction recording**, not lifecycle analytics.

As a result:

* business logic was difficult to maintain
* historical merges broke trend analysis
* reports were inconsistent across business units

This project addresses these problems through **a centralized dbt analytics model**.

---

# 3. Data Modeling Approach

The project follows a **layered dbt architecture** commonly used in modern analytics stacks.

```
seeds
  └── sample input datasets

staging
  └── data cleaning and normalization

intermediate
  ├── customer identity resolution
  ├── monthly sales aggregation
  ├── rolling sales window calculations
  ├── lifecycle classification logic
  └── override application

marts
  ├── fact tables
  └── dimension tables
```

Each layer serves a clear purpose:

| Layer            | Purpose                                                    |
| ---------------- | ---------------------------------------------------------- |
| **Seeds**        | Provide sample input datasets for reproducible development |
| **Staging**      | Standardize raw data structure                             |
| **Intermediate** | Implement business logic and transformations               |
| **Marts**        | Produce analytics-ready tables for BI tools                |

---

# 4. Customer Identity Resolution

Customer identities may change due to:

* account mergers
* acquisitions
* data corrections

To maintain consistent historical analysis, the model implements a **survivor mapping strategy**.

Each raw customer ID is mapped to a stable **analysis_customer_id**:

```
analysis_customer_id = COALESCE(survivor_customer_id, customer_id)
```

This approach ensures:

* historical records remain analyzable after customer merges
* revenue is attributed to the correct surviving customer
* aggregation remains stable across time

---

# 5. BNL Lifecycle Classification

Customer lifecycle classification operates at the following grain:

```
as_of_month × business_unit × product_id × analysis_customer_id
```

Two rule frameworks exist due to differences between business units.

---

## 5.1 Standard Lifecycle Logic

Used by the majority of business units.

### January – June

Lost Customer

```
sales_recent_6m = 0
AND sales_prior_18m > 0
```

New Customer

```
sales_recent_6m > 0
AND sales_prior_18m = 0
```

Base Customer

All remaining customers.

Base customers are further segmented based on **year-over-year sales variance**:

| Condition | Classification |
| --------- | -------------- |
| YoY > 0   | Base Gainer    |
| YoY < 0   | Base Drainer   |

---

### July – December

Lost Customer

```
sales_ytd_current_year = 0
AND sales_previous_year_full > 0
```

New Customer

```
sales_ytd_current_year > 0
AND sales_previous_year_full = 0
```

Base Customer

All remaining customers.

---

## 5.2 Alternative BU Lifecycle Logic

Certain business units use a simplified rule set without the July threshold.

Lost Customer

```
sales_recent_9m = 0
AND sales_prior_18m > 0
```

New Customer

```
sales_recent_9m > 0
AND sales_prior_18m = 0
```

---

# 6. Business Override Framework

In operational environments, lifecycle classifications occasionally require **manual override**.

Examples include:

* strategic account reclassification
* customer organizational changes
* product launch exceptions
* data anomalies

Overrides are stored in a dedicated table with the following grain:

```
as_of_month × business_unit × customer × product
```

If an override exists, the final classification becomes:

```
bnl_bucket_final = override_bucket
```

The model tracks override metadata:

| Field                 | Purpose                        |
| --------------------- | ------------------------------ |
| override_applied_flag | Indicates if override was used |
| override_reason       | Documentation for override     |

This design preserves **auditability and governance**.

---

# 7. Dimensional Model

The final data model follows a **star schema** design.

### Fact Table

`fct_bnl__product_customer_month`

Grain

```
as_of_month × BU × product_id × analysis_customer_id
```

Measures

* sales_amt
* yoy_variance_amt

Attributes

* bnl_bucket_raw
* bnl_bucket_final
* base_movement
* override_applied_flag

---

### Dimension Tables

**dim_customer**

Contains resolved customer identities and merge metadata.

**dim_product**

Product attributes and business unit classification.

**dim_calendar_month**

Calendar attributes used for reporting and time aggregation.

---

# 8. Data Quality & Business Logic Tests

The project includes **custom dbt data tests** validating lifecycle classification rules.

Examples include:

| Test                | Purpose                                            |
| ------------------- | -------------------------------------------------- |
| Lost Before July    | Ensures Lost classification matches window logic   |
| New Before July     | Ensures New classification matches window logic    |
| Lost After July     | Validates YTD rule framework                       |
| Override Validation | Ensures override rows correctly apply final bucket |

These tests ensure the model remains **correct as data evolves**.

---

# 9. Downstream Analytics Consumption

The model is designed to support **business intelligence platforms such as Tableau**.

dbt **exposures** are defined to simulate downstream dashboards including:

* Executive BNL Customer Health Dashboard
* Customer Lifecycle Drilldown
* Published Tableau Data Source

This documents how analytical outputs are consumed by stakeholders.

---

# 10. Running the Project Locally

### Install dbt

```
pip install dbt-duckdb
```

---

### Load seed data

```
dbt seed
```

---

### Build models

```
dbt run
```

---

### Execute tests

```
dbt test
```

---

### Generate documentation

```
dbt docs generate
dbt docs serve
```

---

# 11. Technology Stack

* **dbt Core**
* **DuckDB**
* **SQL**
* Dimensional data modeling
* Analytics engineering best practices

---

# 12. Future Enhancements

Potential extensions include:

* parameterized lifecycle rule configuration
* slowly changing customer dimensions
* automated reconciliation checks
* cloud warehouse deployment (BigQuery / Snowflake)
* semantic layer integration



