# Medicare Data Engineering Pipeline

A comprehensive data engineering project leveraging synthetic Medicare datasets (Enrollment, Fee-for-Service Claims, and Prescription Drug Events) to build a hybrid batch and streaming data platform.

## Project Overview

This project demonstrates a modern data engineering approach by implementing both batch and streaming pipelines, along with hybrid ETL/ELT transformation patterns, using Medicare healthcare data as the domain focus.

### Project Goals

- Build a scalable data platform capable of processing Medicare claims, enrollment, and prescription data
- Implement both batch processing for historical analysis and stream processing for real-time insights
- Demonstrate best practices in data engineering including data quality, testing, and monitoring
- Create analytical models that provide healthcare-specific insights and metrics
- Showcase both ETL and ELT patterns for different data transformation needs

### Key Performance Indicators (KPIs)

- **Technical KPIs**
  - Pipeline reliability: 99.9% successful job runs
  - Cost efficiency: Optimized resource utilization across batch/streaming

- **Business KPIs**
  - Claims processing metrics: Average processing time, rejection rates
  - Patient enrollment analysis: Coverage patterns, demographic distributions
  - Prescription analysis: Dispensing patterns, cost trends
  - Provider performance: Service utilization, cost efficiency
  - Fraud detection: Anomaly identification rates, false positive reduction

## Architecture

![Architecture Diagram](architecture/medicare_data_pipeline_architecture.png)


1. **Ingestion Layer**
   - Batch ingestion of Medicare datasets
   - Streaming simulation of real-time Medicare events
   - Change data capture for incremental updates

2. **Storage Layer**
   - Raw data zone (landing)
   - Cleansed data zone (validated)
   - Curated data zone (business entities)

3. **Processing Layer**
   - Batch processing with ETL for initial cleansing
   - Warehouse processing with ELT for analytics
   - Stream processing for real-time insights

4. **Serving Layer**
   - Data warehouse with dimensional models
   - Real-time analytics dashboards
   - API services for data access

### Data Flow

1. Medicare data is ingested through both batch and streaming pipelines
2. Initial ETL processes clean and standardize the data
3. Cleansed data is stored in the appropriate storage zone
4. ELT processes transform data for specific analytical needs
5. Transformed data is made available through the serving layer

## Technologies

| Component         | Technology Options          | Status |
| ----------------- | --------------------------- | ------ |
| Orchestration     | Apache Airflow              | TBD    |
| Stream Processing | Apache Kafka, Kafka Streams | TBD    |
| Batch Processing  | Apache Spark                | TBD    |
| Storage           | S3                          | TBD    |
| Warehouse         | Snowflake                   | TBD    |
| Transformations   | dbt                         | TBD    |
| Visualization     | Tableau, Power BI           | TBD    |
| CI/CD             | GitHub Actions              | TBD    |
| IaC               | Terraform                   | TBD    |
| Monitoring        | Prometheus, Grafana         | TBD    |

## Data Models

### Core Entities

1. **Patient/Beneficiary**
   - Demographics
   - Enrollment periods
   - Coverage details

2. **Provider**
   - Specialties
   - Locations
   - Network affiliations

3. **Claims**
   - Header information
   - Line items
   - Diagnosis codes
   - Procedure codes

4. **Prescriptions**
   - Medications
   - Dosages
   - Pharmacy information
   - Fill dates

## Getting Started

### Prerequisites

- 
### Installation

```bash
# Clone the repository
git clone https://github.com/lshunak/medicare_data_project.git
cd medicare_data_project
