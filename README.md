# NewYorkTaxi Pipeline

### Background?
The New York City Taxi and Limousine Commission (TLC), created in 1971, is the agency responsible for licensing and regulating NYC's Yellow Taxicabs and For-Hire Vehicles (FHVs). Yellow taxis are the iconic, yellow vehicles that have the right to pick up street-hailing passengers anywhere in NYC. By law there are only 13,587 yellow taxis in NYC. FHVs are prearranged services that are dispatched by TLC-licensed FHV bases. From these FHVs, green taxis are permitted to accept street-hails in northern Manhattan (above E 96th St and W 110th St) and in the outer borough.

### Personal Objectives
- Create a project to get better at these technologies: Airflow, PySpark, databricks, docker and CI/CD
- Benchmark transformations using a years worth of New York Taxi data against Pandas, Spark deployed locally, Spark deployed on Data Proc.

### Links
- [Set up instructions](setup.md) - Instructions to run the pipeline yourself

### Pipeline Introduction
This pipeline extracts [New York Taxi data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), stages it in Google Cloud Storage and loads it into BigQuery staging schema. PySpark is used for transformation.

### Infrastructure Introduction
Google Cloud Platform is being used as the cloud provider. All infrastructure is set up is through terraform files. Minimal configuration of terraform variables files are required for project id and bucket names. [Set up instructions](setup.md)

- Local setup of Airflow using Docker
- Local setup of PySpark using Docker and cloud setup using Terraform hosted on Data Proc
- A bucket is used for storing raw data. BigQuery is used as an OLAP database.

### Tools & Technologies
- **Infrastructure as code**: Terraform
- **DevOps**: Docker
- **Orchestrator**: Airflow (local setup using Docker and cloud setup using Cloud Composer)
- **Cloud storage**: Google Cloud Storage
- **Cloud database**: BigQuery
- **Transformations**: PySpark (local setup using Docker and cloud setup using Data Proc)
- **Language**: Python