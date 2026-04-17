# crawler.py — 실제 크롤링 대신 샘플 데이터를 생성하는 모의 크롤러

import csv
import os
import random
from datetime import datetime, timedelta

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output")
os.makedirs(OUTPUT_PATH, exist_ok=True)

print("크롤러 시작")

teams = ["TeamA", "TeamB", "TeamC", "TeamD"]
rows = []

for i in range(10):
    date = (datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d")
    home = random.choice(teams)
    away = random.choice([t for t in teams if t != home])
    rows.append({
        "date": date,
        "home_team": home,
        "away_team": away,
        "home_score": random.randint(0, 5),
        "away_score": random.randint(0, 5),
    })

filename = os.path.join(OUTPUT_PATH, f"game_{datetime.today().strftime('%Y%m%d')}.csv")
with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"크롤링 완료 → {filename}")