import csv
import random
from datetime import datetime, timedelta

user_ids = [f"user_{i}" for i in range(1, 51)]
song_ids = [f"song_{i}" for i in range(1, 101)]

def random_timestamp(start, end):
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

start_date = datetime(2025, 3, 18)
end_date = datetime(2025, 3, 25)

with open('listening_logs.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['user_id', 'song_id', 'timestamp', 'duration_sec'])
    
    for _ in range(5000):
        user = random.choice(user_ids)
        song = random.choice(song_ids)
        timestamp = random_timestamp(start_date, end_date).strftime('%Y-%m-%d %H:%M:%S')
        duration = random.randint(30, 300)  # 30 sec to 5 min
        writer.writerow([user, song, timestamp, duration])