import csv
import random

song_ids = [f"song_{i}" for i in range(1, 101)]
titles = [f"Title {i}" for i in range(1, 101)]
artists = [f"Artist {i}" for i in range(1, 31)]
genres = ['Pop', 'Rock', 'Jazz', 'Hip-Hop', 'Classical']
moods = ['Happy', 'Sad', 'Energetic', 'Chill']

with open('songs_metadata.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['song_id', 'title', 'artist', 'genre', 'mood'])
    
    for i in range(100):
        writer.writerow([
            song_ids[i],
            titles[i],
            random.choice(artists),
            random.choice(genres),
            random.choice(moods)
        ])