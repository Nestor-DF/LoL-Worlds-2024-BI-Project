# League of Legends Worlds 2024 – Business Intelligence Project

## Install dependencies (Apache Superset not included, see below)

Requirements: Python >= 3.5
In the root of the project:

```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
source venv/bin/activate
```

Deactivate environment:

```bash
deactivate
```

---

## Environment Setup

Create a `.env` file in the project root with the following variables (examples below):
You obtain the Airbyte variables after the corresponding installation (See the Airbyte section).

```env
POSTGRES_USER=nestor
POSTGRES_PASSWORD=12ab12ab
POSTGRES_DB=ERP_Database
POSTGRES_DB2=dwh
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

AIRBYTE_CLIENT_ID=5774d0b4-064b-416d-a252-2b8e6fa15e25
AIRBYTE_CLIENT_SECRET=RcOApHWnvPzLz0UJCNQHhaxQRxycb7qm
AIRBYTE_WORKSPACE_ID=936bd719-2f6a-46db-88d9-0f79191540e8
```

Export the variables (required for Dagster):

```bash
set -a
source .env
set +a
```

Verify the setup:

```bash
echo $AIRBYTE_CLIENT_ID
env | grep AIRBYTE
```

---

## Cron

Edit crontab:

```bash
crontab -e
```

Scheduled task:

```bash
0 3 * * 1 cd /YOUR_PATH/LoL-Worlds-2024-BI-Project/scripts && ../venv/bin/python load_incremental.py >> logs/cron.log 2>&1
```

Brief explanation:
Runs the incremental load script every Monday at 03:00, using the project’s virtual environment and logging output for monitoring and debugging.

---

## PostgreSQL

In the root of the project:

```bash
docker compose up
```

---

## AirByte

Official installation guide: [https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart#part-1-install-docker-desktop](https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart#part-1-install-docker-desktop)

```bash
abctl local status
```

---

## DBT

```bash
cd dbt_/
dbt debug
dbt build
```

---

## Dagster

```bash
cd dbt_/orchestrator/
dagster dev
```

---

## Installation of Apache Superset on Ubuntu 24.04 (with Python 3.11)

Ubuntu 24.04 uses Python 3.12 by default, which is **not compatible with Superset**.
For this reason, **Python 3.11** is installed and used inside a virtual environment.
If you have already done this before, you only need to activate the environment and run steps 4 and 6.

### 1. Install Python 3.11 and dependencies
```bash
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev build-essential libssl-dev -y
```

### 2. Create and activate the virtual environment
```bash
python3.11 -m venv superset-venv
source superset-venv/bin/activate
```

### 3. Install Superset and required libraries
```bash
python -m pip install --upgrade pip setuptools wheel
pip install apache-superset
pip install "marshmallow>=3,<4" "apispec[yaml]>=6,<7" pillow
pip install psycopg2-binary
```

### 4. Configure environment variables
```bash
export SUPERSET_CONFIG_PATH=$(pwd)/superset_config.py
export FLASK_APP=superset
```

### 5. Initialize Superset
```bash
superset db upgrade
superset fab create-admin
superset init
```

### 6. Run Superset
```bash
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
```