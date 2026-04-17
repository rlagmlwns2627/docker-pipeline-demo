"""
샘플 데이터 생성 스크립트
경기 CSV, 팀 정보 CSV, ML 모델 생성.
"""

import pandas as pd
import numpy as np
import pickle
import os
from sklearn.ensemble import RandomForestClassifier

np.random.seed(42)

path = os.getenv("BASE_PATH")

# 경기 CSV 생성 (FTP로 수신했다고 가정)
teams = ["KIA", "삼성", "LG", "두산", "SSG", "롯데", "한화", "NC", "KT", "키움"]

for month in range(1, 4):
    n = 100
    df = pd.DataFrame({
        "game_id":      [f"2024{month:02d}{i:03d}" for i in range(n)],
        "date":         pd.date_range(f"2024-{month:02d}-01", periods=n, freq="D").strftime("%Y-%m-%d"),
        "home_team_id": np.random.randint(1, 11, n),
        "away_team_id": np.random.randint(1, 11, n),
        "home_score":   np.random.randint(0, 15, n),
        "away_score":   np.random.randint(0, 15, n),
        "innings":      np.random.choice([9, 10, 11, 12], n),
        "home_hits":    np.random.randint(3, 20, n),
        "away_hits":    np.random.randint(3, 20, n),
        "home_errors":  np.random.randint(0, 4, n),
        "away_errors":  np.random.randint(0, 4, n),
        "attendance":   np.random.randint(5000, 25000, n),
    })
    df.to_csv(os.path.join(path, f"dataset/game_2024_{month:02d}.csv"), index=False, encoding="cp949")
    print(f"생성 완료: game_2024_{month:02d}.csv ({len(df)}행)")

# 팀 정보 CSV 생성
mapping_df = pd.DataFrame({
    "team_id":    range(1, 11),
    "team_name":  teams,
    "city":       ["광주", "대구", "서울", "서울", "인천", "부산", "대전", "창원", "수원", "서울"],
    "stadium":    ["챔피언스필드", "라이온즈파크", "잠실", "잠실", "SSG랜더스필드",
                   "사직", "한화생명볼파크", "창원NC파크", "수원KT위즈파크", "고척스카이돔"],
    "founded":    [1982, 1982, 1982, 1982, 2000, 1975, 1985, 2011, 2013, 1986],
})
mapping_df.to_csv(os.path.join(path, "dataset/team_info.csv"), index=False, encoding="cp949")
print("생성 완료: team_info.csv")

# ML 모델 생성
X_train = np.random.randn(500, 4)
y_train = (X_train[:, 0] - X_train[:, 1] + np.random.randn(500) * 0.5 > 0).astype(int)

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

with open(os.path.join(path, "model/win_predictor.pkl"), "wb") as f:
    pickle.dump(model, f)
print("생성 완료: win_predictor.pkl")