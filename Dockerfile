# 1. 베이스 이미지 — Python 3.11이 설치된 환경에서 시작
FROM python:3.11

# 2. 컨테이너 안에서 작업할 폴더 지정
WORKDIR /app

# 3. requirements.txt 먼저 복사 후 라이브러리 설치
#    (코드보다 먼저 설치해야 캐시가 효율적으로 동작)
COPY requirements.txt .
RUN pip install -r requirements.txt

# 4. 나머지 파이썬 파일 복사
COPY generate_sample_data.py .
COPY etl.py .

# 5. 컨테이너 실행 시 자동으로 실행할 명령어
CMD ["python", "etl.py"]