FROM python:3.11-slim
# 가벼운 Python 3.11 베이스 이미지 사용

WORKDIR /app
# 컨테이너 안 작업 디렉토리를 /app으로 설정

COPY requirements.txt .
# requirements.txt를 컨테이너 안으로 복사

RUN pip install -r requirements.txt
# 패키지 설치

COPY crawler.py .
# 크롤러 스크립트 복사

CMD ["python", "crawler.py"]
# 컨테이너 실행 시 crawler.py 자동 실행