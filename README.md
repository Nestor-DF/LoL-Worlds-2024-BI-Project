# Monorepo para el proyecto de la asignatura de Gestión de la Comunicación y Conocimiento Empresarial

## Instalar las dependencias (Apache Superset no incluido, mirar más abajo)
Requisitos: Python >= 3.5
En la raíz del proyecto:
```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
source venv/bin/activate
```

Desactivar entorno:
```bash
deactivate
```

---

## PostgreSQL
En la raíz del proyecto:
```bash
docker compose up
```

---

## AirByte
Guía oficial para la instalación: [https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart#part-1-install-docker-desktop](https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart#part-1-install-docker-desktop)
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

## Instalación de Apache Superset en Ubuntu 24.04 (con Python 3.11)
Ubuntu 24.04 usa Python 3.12 por defecto, el cual **no es compatible con Superset**.  
Por eso, se instala y usa **Python 3.11** dentro de un entorno virtual.  
Si ya has hecho esto antes solo tienes que activar el entorno y ejecutar los pasos 4 y 6.

### 1. Instalar Python 3.11 y dependencias
```bash
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev build-essential libssl-dev -y
```

### 2. Crear y activar el entorno virtual
```bash
python3.11 -m venv superset-venv
source superset-venv/bin/activate
```

### 3. Instalar Superset y librerías necesarias
```bash
python -m pip install --upgrade pip setuptools wheel
pip install apache-superset
pip install "marshmallow>=3,<4" "apispec[yaml]>=6,<7" pillow
pip install psycopg2-binary
```

### 4. Configurar variables de entorno
```bash
export SUPERSET_CONFIG_PATH=$(pwd)/superset_config.py
export FLASK_APP=superset
```

### 5. Inicializar Superset
```bash
superset db upgrade
superset fab create-admin
superset init
```

### 6. Ejecutar Superset
```bash
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
```
