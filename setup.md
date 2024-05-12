# Setup
Pipeline was tested on a windows machine running Windows Subsystem for Linux (WSL) on Ubuntu 22.04.3 LTS.

### Requirements
1. (For windows users) Windows Subsystem for Linux. [Instructions here](https://learn.microsoft.com/en-us/windows/wsl/install)
2. Terraform installed. [Instructions here](https://developer.hashicorp.com/terraform/install#linux)
3. Docker installed. [Instructions here](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
4. Google Cloud Platform account

### Initial Set Up
1. Git clone the repo
``` bash
git clone https://github.com/alfredzou/NewYorkTaxi_Pipeline.git
```
2. Navigate to root of the repo
``` bash
cd NewYorkTaxi_Pipeline
```

### Google Cloud Platform Set Up



### Terraform Set Up
1. Navigate to terraform folder
``` bash
cd terraform
```
2. Run:
``` bash
terraform init
terraform apply
```
3.

### Local Set Up Instructions
1. Navigate to root folder
``` bash
cd NewYorkTaxi_Pipeline
```
2. If you are using linux or WSL run this step
``` bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
3. Initialise airflow. Creates an account with login `airflow` and password `airflow`
``` bash
docker compose up airflow-init

# Should give you the message
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.9.0
start_airflow-init_1 exited with code 0
```
4. Run services in detached mode
``` bash
docker compose up -d
```
5. Navigate to `http://localhost:8080/` and login with user `airflow` and password `airflow`
