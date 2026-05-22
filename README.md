# Customer Segmentation dbt Project

**dbt + BigQuery Analytics Engineering Project**


# 1. Project Overview
This project implements a **customer segmentation model** using **dbt** and **Google BigQuery**.

The purpose of this model is to provide a clear and consistent view of customer purchase behavior, lifecycle trends, and revenue movement across product categories and customer segments.

The pipeline transforms raw transactional data into an analytics-ready dimensional model designed to support downstream BI tools such as Tableau.

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

Businesses need a standardized way to measure **customer lifecycle performance** over time.

This model is designed to answer questions such as:

* How many customers are **New**, **Declining**, **Growing**, or **Inactive** in a given period?
* Which products or categories are driving **customer growth or churn**?
* Are **existing customers expanding or contracting** their purchasing behavior?

However, several issues prevented reliable analysis:

### Lack of Standard Definitions

Different business units used different definitions of:

* New customers
* Inactive customers
* Declining customers

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

# 5. Customer Segmentation

Customer segmentation operates at the following grain:

```
as_of_month × segment × product_id × analysis_customer_id
```

Two rule frameworks exist due to differences between business units.

---

## 5.1 Customer Lifecycle Logic

Customers are segmented by comparing **recent activity** against a **historical baseline**.

**Inactive Customer**

```
No activity in the recent period
AND
Activity present in the historical period
```

**New Customer**

```
Activity in the recent period
AND
No prior historical activity
```

**Growing / Declining Customers**

Remaining customers are classified based on change in behavior between periods:

| Condition                     | Classification |
|------------------------------|----------------|
| Recent > Historical baseline | Growing        |
| Recent < Historical baseline | Declining      |

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
as_of_month × segment × customer × product
```

If an override exists, the final classification becomes:

```
customer_segmentation_bucket_final = override_bucket
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

`fct_customer_segmentation__product_customer_month`

Grain

```
as_of_month × segment × product_id × analysis_customer_id
```

Measures

* sales_amt
* yoy_variance_amt

Attributes

* customer_segmentation_bucket_raw
* customer_segmentation_bucket_final
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
| Inactive    | Ensures Inactive classification matches window logic   |
| New         | Ensures New classification matches window logic    |
| Override Validation | Ensures override rows correctly apply final bucket |

These tests ensure the model remains **correct as data evolves**.

---

# 9. Downstream Analytics Consumption

The model is designed to support **business intelligence platforms such as Tableau**.

dbt **exposures** are defined to simulate downstream dashboards including:

* Executive Customer Health Dashboard
* Customer Lifecycle Drilldown
* Published Tableau Data Source

This documents how analytical outputs are consumed by stakeholders.

---

# 10. Deployment

This project is deployed in **dbt Cloud** with **BigQuery** as the data warehouse.

The pipeline includes:

- Loading seed CSV files into BigQuery using `dbt seed`
- Building staging, intermediate, and mart models using `dbt run`
- Validating model quality using `dbt test`
- Running the full pipeline through a scheduled dbt Cloud job using `dbt build`

The project was originally developed locally and then migrated to BigQuery. During the migration, warehouse-specific SQL was updated for BigQuery compatibility, including data types, date functions, interval logic, and dimensional grain validation.

### Production Job

A scheduled dbt Cloud job runs the customer segmentation pipeline using:

```bash
dbt build
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
* **BigQuery**
* **SQL**
* Dimensional data modeling
* Analytics engineering best practices

---

# 12. Future Enhancements

Potential extensions include:

* parameterized lifecycle rule configuration
* slowly changing customer dimensions
* automated reconciliation checks
* semantic layer integration



