# Medicare Data Engineering Pipeline

A data engineering project focused on processing and analyzing synthetic Medicare data files.

## Project Overview

This project implements an ELT (Extract, Load, Transform) pipeline for Medicare healthcare data using Apache Airflow, AWS S3, and data cataloging. The pipeline efficiently processes beneficiary (patient) data, claims data, and prescription drug events.


## Project Structure

### Data Pipeline Architecture
- 🔄 need to add *proper* diagram

┌─────────┐         ┌───────────┐         ┌───────────┐
│ CMS.gov │────────►│  Airflow  │────────►│  AWS S3   │
└─────────┘         │  (Python) │         └─────┬─────┘
                    └───────────┘               │
                                                ▼
┌────────────┐      ┌───────────┐         ┌───────────┐
│            │◄─────┤ Snowflake │◄────────┤ AWS Glue  │
│ Dashboard  │      │           │         │  Catalog  │
└────────────┘      └───────────┘         └───────────┘

## Implementation Status

- ✅ Airflow DAG setup and configuration
- ✅ Data extraction from source
- ✅ S3 upload functionality
- ✅ Local file cleanup after upload
- ✅ AWS Glue cataloging (in progress)
- ⬜ Data transformation layer
- ⬜ Analytics and dashboard implementation

## Next Steps

1. Complete AWS Glue cataloging implementation
2. Set up Athena for SQL-based data exploration
3. Evaluate data warehouse options (Redshift vs. Snowflake)
4. Implement data transformation layer
5. Create analytical dashboards

## From now here it's more "association sun"


### Optional Analytical Goals

1. **Demographics Analysis**
   - Analyze beneficiary age distribution, gender ratios, and geographic distribution
   - Identify demographic trends across different years (2015-2025)
   - Visualize population health characteristics by region

2. **Healthcare Utilization Patterns**
   - Quantify service utilization rates across different claim types
   - Analyze the relationship between demographics and service utilization
   - Identify seasonal patterns in healthcare service usage

3. **Cost and Spending Analysis**
   - Calculate average costs per beneficiary across different service categories
   - Identify high-cost procedures and services
   - Analyze cost variations by geographic region and provider type

4. **Chronic Condition Analysis**
   - Identify prevalence of chronic conditions in the beneficiary population. Emphasis on diabetes as example
   - Analyze healthcare utilization patterns for patients with specific conditions
   - Explore the relationship between multiple chronic conditions and healthcare costs

5. **Provider Performance Metrics**
   - Develop metrics for provider efficiency and quality
   - Compare utilization patterns across different provider types
   - Identify outlier providers in terms of cost or service volume

6. **Prescription Drug Analysis**
   - Analyze prescription drug utilization and costs
   - Identify frequently prescribed medications
   - Examine relationships between diagnoses and prescription patterns
   - Compare brand name versus generic drug usage

7. **Fraud Detection and Analysis**
   - Identify unusual billing patterns and potential fraud indicators
   - Flag unusual provider-beneficiary relationships
   - Analyze geographic hotspots for suspicious activity
   - Detect abnormal prescription patterns and drug combinations
   - Develop risk scores for potential fraudulent activities

## Available Data Files

The project currently works with the following synthetic Medicare data files:

### Beneficiary Files
- Multiple yearly snapshots (2015-2025)
- ~185 columns per file including demographics, eligibility, and enrollment data

### Claims Files
- `carrier.csv` - Professional services claims
- `inpatient.csv` - Hospital inpatient claims 
- `outpatient.csv` - Hospital outpatient claims
- `dme.csv` - Durable Medical Equipment claims
- `hha.csv` - Home Health Agency claims
- `hospice.csv` - Hospice claims
- `snf.csv` - Skilled Nursing Facility claims

### Part D (Prescription Drug) Files
- `pde.csv` - Prescription Drug Events
- Contains detailed information about medications, pharmacy dispensing, and drug costs
- Links to beneficiary data via beneficiary identifiers

## Data Structure

### Core Entities

1. **Beneficiary**
   - Demographics (age, gender, race)
   - Geographic information (state, county, zip)
   - Enrollment periods
   - Coverage details

2. **Claims**
   - Various types (carrier, inpatient, outpatient, etc.)
   - Service details
   - Diagnosis and procedure codes
   - Payment information

3. **Prescription Drugs**
   - Drug identifiers (NDC codes)
   - Dispensing information
   - Quantity and days supply
   - Payment and cost details
   - Pharmacy and prescriber information

## Data Pipeline1: ELT Approach

### Data Sources
- Synthetic Medicare RIF (Research Identifiable Files) datasets in CSV format
- Beneficiary enrollment data (yearly snapshots 2015-2025)
- Claims data (carrier, inpatient, outpatient, DME, HHA, hospice, SNF)
- Part D prescription drug events data

### Target Systems
- **Data Lake**: AWS S3 for raw data storage and organization
- **Data Warehouse**: Snowflake for structured data storage and transformation

### ELT Process

1. **Extract**
   - Pull Medicare CSV files from source locations
   - Validate file structure and completeness
   - Track extraction metadata (timestamps, file details)

2. **Load**
   - Load raw Medicare files directly into S3 data lake
   - Organize by data category, year, and file type
   - Maintain original data integrity

3. **Transform**
   - Perform transformations in Snowflake using dbt
   - Create healthcare-specific data models:
     * Beneficiary dimension
     * Provider dimension
     * Claims fact tables
     * Prescription drug fact tables
     * Medicare-specific analytical views
   - Generate derived metrics and aggregations

## Getting Started

### Prerequisites

- Python 3.8+
- Pandas, NumPy for data processing
- Jupyter notebooks for exploration
- Storage space for Medicare data files (~1GB)

### Installation

```bash
# Clone the repository
git clone https://github.com/lshunak/medicare_data_project.git
cd medicare_data_project

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  

# Install dependencies
pip install -r requirements.txt