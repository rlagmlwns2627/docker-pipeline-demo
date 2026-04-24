# docker-pipeline-demo

Baseball game data pipeline — Crawler / ETL / dbt / MySQL, orchestrated with Airflow on Docker.

> Sample dataset generated via `generate_sample_data.py`

<br>

## Pipeline

```
Crawler → ETL → dbt → MySQL
                   ↑
         Airflow (daily 09:00 KST)
```

<br>

## Tech Stack

- Docker, Docker Compose
- Apache Airflow 2.8.1
- dbt 1.7
- MySQL 8.0
- Python 3.8 — pandas, scikit-learn, SQLAlchemy

<br>

## Project Structure

```
├── crawler/              # collects game data → CSV
├── etl.py                # feature engineering + MySQL load
├── dbt/models/           # splits into game_info / game_score / team_info
├── airflow/dags/         # pipeline DAG (crawler → etl)
└── docker-compose.yml
```

<br>

## Getting Started

```bash
git clone https://github.com/rlagmlwns2627/docker-pipeline-demo.git
cd docker-pipeline-demo

cp .env.example .env
# fill in your credentials

docker-compose up --build -d
```

Airflow UI → http://localhost:8080 `admin / admin`
