"""
ETL 파이프라인 메인 스크립트
CSV 병합 → 모델 로드 → 파생변수 생성 → 매핑 → MySQL 적재
"""

import pandas as pd
import numpy as np
import pickle
import os
import glob
import time
import pymysql

from sqlalchemy import create_engine
from datetime import datetime

# 경로 설정
# 로컬 실행 시 BASE_PATH 기본값, Docker 실행 시 환경변수로 덮어씀
BASE_PATH    = os.getenv("BASE_PATH")
INPUT_PATH   = os.getenv("INPUT_PATH",   os.path.join(BASE_PATH, "dataset"))
OUTPUT_PATH  = os.getenv("OUTPUT_PATH",  os.path.join(BASE_PATH, "output"))
MODEL_PATH   = os.getenv("MODEL_PATH",   os.path.join(BASE_PATH, "model/win_predictor.pkl"))
MAPPING_PATH = os.getenv("MAPPING_PATH", os.path.join(BASE_PATH, "dataset/team_info.csv"))
ENCODING     = os.getenv("ENCODING",     "cp949")

os.makedirs(OUTPUT_PATH, exist_ok=True)

print("ETL 파이프라인 시작")
print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# 개별 CSV 파일 병합 (FTP 수신 데이터)
# team_info.csv가 같은 폴더에 있으므로 game_*.csv 패턴으로 한정
print("\n개별 CSV 파일 병합 중")

csv_files = glob.glob(os.path.join(INPUT_PATH, "game_*.csv"))

if not csv_files:
    raise FileNotFoundError(f"'{INPUT_PATH}' 폴더에 game_*.csv 파일 부재")

df_list = []
for file in sorted(csv_files):
    df_temp = pd.read_csv(file, encoding=ENCODING)
    df_list.append(df_temp)
    print(f"{os.path.basename(file)}: {len(df_temp)}행 로드")

df = pd.concat(df_list, ignore_index=True)
print(f"병합 완료: 총 {len(df)}행")

# 머신러닝 모델 로드 → 파생변수 생성
print("모델 로드 및 파생변수 생성 중")

with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)
print(f"모델 로드 완료: {MODEL_PATH}")

# 임시 파생변수 생성
df["score_diff"] = df["home_score"] - df["away_score"]
df["home_win"] = (df["home_score"] > df["away_score"]).astype(int)
df["is_extra_inning"] = (df["innings"] > 9).astype(int)
features = df[["home_hits", "away_hits", "home_errors", "away_errors"]].values
df["win_probability"] = model.predict_proba(features)[:, 1].round(4)

print(f"파생변수 생성 완료: score_diff, home_win, is_extra_inning, win_probability")


# 매핑 파일 로드 → 팀 정보 매핑
print("팀 정보 매핑 중")

mapping_df = pd.read_csv(MAPPING_PATH, encoding=ENCODING)
print(f"매핑 파일 로드 완료: {len(mapping_df)}개 팀")

# 홈팀 정보 매핑
df = df.merge(
    mapping_df.add_prefix("home_"),
    left_on="home_team_id",
    right_on="home_team_id",
    how="left"
)

# 원정팀 정보 매핑
df = df.merge(
    mapping_df.add_prefix("away_"),
    left_on="away_team_id",
    right_on="away_team_id",
    how="left"
)

print(f"매핑 완료")

# 최종 결과 저장
print("최종 결과 저장 중")

output_file = os.path.join(OUTPUT_PATH, f"result_{datetime.now().strftime('%Y%m%d')}.csv")
df.to_csv(output_file, index=False, encoding=ENCODING)

print(f"\n저장 완료: {output_file}")
print(f"최종 행 수: {len(df)}행 / 컬럼 수: {len(df.columns)}개")

# MySQL 적재
print("MySQL 적재 중")

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# 최대 5번 재시도
max_retries = 5
for i in range(max_retries):
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        df.to_sql(
            name="game_result",
            con=engine,
            if_exists="replace",
            index=False
        )

        print(f"MySQL 적재 완료: {DB_NAME}.game_result 테이블")
        print(f"적재 행 수: {len(df)}행")
        break  # 성공하면 반복 종료

    except Exception as e:
        if i < max_retries - 1:
            print(f"MySQL 연결 대기 중 ({i+1}/{max_retries})")
            time.sleep(5)  # 5초 대기 후 재시도
        else:
            print(f"MySQL 적재 실패: {e}")
            print(f"CSV 저장은 완료된 상태입니다.")