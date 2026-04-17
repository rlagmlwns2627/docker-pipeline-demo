FROM python:3.11-slim
# 가벼운 Python 3.11 기본 이미지 사용

RUN pip install dbt-mysql==1.7.0
# dbt MySQL 어댑터 설치 (dbt-core 포함)

WORKDIR /usr/app/dbt
# 작업 디렉토리 설정