
---

## Spark Structured API: Music Streaming Platform Analytics

## **Prerequisites**

Before starting the assignment, ensure the following tools are installed:

1. **Python 3.x**  
   - [Download Python](https://www.python.org/downloads/)  
   - Verify installation:
     ```bash
     python --version
     ```

2. **PySpark**
   - Install using pip:
     ```bash
     pip install pyspark
     ```

---

## **Setup Instructions**

- **generate_songs_metadata.py**: Generates synthetic song metadata.
- **generate_listening_logs.py**: Simulates user listening behavior.
- **enrich_logs.py**: Joins logs with metadata and outputs enriched logs.
- **task1_user_favorite_genres.py**: Identifies each user’s favorite genre.
- **task2_avg_listen_time_per_song.py**: Calculates average listen time per song.
- **task3_top_songs_this_week.py**: Lists the most played songs this week.
- **task4_happy_recommendations.py**: Recommends “Happy” songs to “Sad” listeners.
- **task5_genre_loyalty_scores.py**: Computes genre loyalty score.
- **task6_night_owl_users.py**: Identifies users who listen between 12 AM and 5 AM.

---

### **2. Running the Scripts**

Generate the input datasets:

```bash
python src/generate_songs_metadata.py
python src/generate_listening_logs.py
```

Enrich the logs:

```bash
spark-submit src/enrich_logs.py
```

Run each task:

```bash
spark-submit src/task1_user_favorite_genres.py
spark-submit src/task2_avg_listen_time_per_song.py
spark-submit src/task3_top_songs_this_week.py
spark-submit src/task4_happy_recommendations.py
spark-submit src/task5_genre_loyalty_scores.py
spark-submit src/debug_loyalty_scores.py
spark-submit src/task6_night_owl_users.py
```

---

## **Overview**

This hands-on assignment uses Apache Spark Structured APIs to analyze user listening data on a music streaming platform. By enriching log data with metadata and performing various aggregations, we extract user behavior insights, genre loyalty, and music trends.

---

## **Objectives**

1. Analyze user listening behavior.
2. Identify favorite genres, average song play times, and weekly trends.
3. Recommend songs to listeners based on mood.
4. Detect loyal users and night-time listeners.

---

## **Dataset Structure**

### listening_logs.csv
| Column       | Type    | Description                              |
|--------------|---------|------------------------------------------|
| user_id      | String  | Unique ID of the user                    |
| song_id      | String  | Unique ID of the song                    |
| timestamp    | String  | Date and time the song was played        |
| duration_sec | Integer | Duration (in seconds) the song was played|

### songs_metadata.csv
| Column   | Type    | Description                    |
|----------|---------|--------------------------------|
| song_id  | String  | Unique ID of the song          |
| title    | String  | Title of the song              |
| artist   | String  | Name of the artist             |
| genre    | String  | Genre of the song (e.g., Pop)  |
| mood     | String  | Mood of the song (e.g., Happy) |

---

## **Assignment Tasks**

---

### **1. Enrich Logs**

**Objective:**
- Join user logs with song metadata by song_id

**Output Folder:**
- `output/enriched_logs/`

**Output:**

| song_id   | user_id   | timestamp                | duration (sec) | title     | artist     | genre   | mood      |
|-----------|-----------|--------------------------|----------------|-----------|------------|---------|-----------|
| song_67   | user_47   | 2025-03-23T06:30:20.000Z | 187            | Title 67  | Artist 12  | Pop     | Happy     |
| song_43   | user_50   | 2025-03-20T05:11:58.000Z | 184            | Title 43  | Artist 9   | Hip-Hop | Energetic |
| song_7    | user_7    | 2025-03-24T20:33:15.000Z | 61             | Title 7   | Artist 23  | Jazz    | Chill     |
| song_74   | user_50   | 2025-03-20T09:22:28.000Z | 78             | Title 74  | Artist 18  | Pop     | Happy     |
| song_94   | user_40   | 2025-03-19T12:15:06.000Z | 60             | Title 94  | Artist 13  | Jazz    | Sad       |

![image](https://github.com/user-attachments/assets/7b0d44e7-91d8-4f0f-bee9-3694d5e69b9a)

---

### **2. Favorite Genre Per User**

**Objective:**
- Count number of times each user listened to each genre and select the top one.

**Output Folder:**
- `output/task1_user_favorite_genres/`

**Output:**

| user_id   | genre   | count      |
|-----------|---------|------------|
| user_1    | Jazz    | 26         |
| user_10   | Hip-Hop | 30         |
| user_11   | Jazz    | 26         |
| user_12   | Hip-Hop | 32         |
| user_13   | Hip-Hop | 25         |

![image](https://github.com/user-attachments/assets/05c21c6e-1d2f-47bf-8e54-7616ad0623fe)

---

### **3. Average Listen Time Per Song**

**Objective:**
- Compute average duration each song was played.

**Output Folder:**
- `output/task2_avg_listen_time_per_song/`

**Output:**

| song_id   | title     | avg_listen_time_(sec) |
|-----------|-----------|------------------------|
| song_2    | Title 2   | 151.17                 |
| song_73   | Title 73  | 157.75                 |
| song_39   | Title 39  | 155.09                 |
| song_8    | Title 8   | 154.12                 |
| song_7    | Title 7   | 177.06                 |

![image](https://github.com/user-attachments/assets/42d0a99b-a9bb-4098-b5c9-30ae412127ed)

---

### **4. Top Songs This Week**

**Objective:**
- Find top 10 most played songs in the current week.

**Output Folder:**
- `output/task3_top_songs_this_week/`

**Output:**

| song_id   | title     | count      |
|-----------|-----------|------------|
| song_15   | Title 15  | 60         |
| song_14   | Title 14  | 56         |
| song_94   | Title 94  | 56         |
| song_67   | Title 67  | 56         |
| song_91   | Title 91  | 56         |
| song_97   | Title 97  | 55         |
| song_76   | Title 76  | 55         |
| song_93   | Title 93  | 54         |
| song_70   | Title 70  | 54         |
| song_6    | Title 6   | 54         |

![image](https://github.com/user-attachments/assets/725f7ae9-6f3b-4de5-8720-49ba69cf9c4f)


---

### **5. Recommend Happy Songs to Sad Listeners**

**Objective:**
- Recommend up to 3 happy songs to users who mostly listen to sad songs and haven't heard the recommendations yet.

**Output Folder:**
- `output/task4_happy_recommendations/`

**Output:**

| user_id   | song_id   | title     | artist     |
|-----------|-----------|-----------|------------|
| user_12   | song_10   | Title 10  | Artist 25  |
| user_12   | song_18   | Title 18  | Artist 14  |
| user_12   | song_21   | Title 21  | Artist 26  |
| user_13   | song_10   | Title 10  | Artist 25  |
| user_13   | song_13   | Title 13  | Artist 12  |

![image](https://github.com/user-attachments/assets/e004195f-e971-4e27-9a43-8cfeb0e828cd)



---

### **6. Genre Loyalty Score**

**Objective:**
- Compute the proportion of each user’s plays from their top genre. Filter for scores > 0.8.

**Output Folder:**
- `output/task5_genre_loyalty_scores/`

**Output:**

| user_id   | max_count        | total       | loyalty_score  |
|-----------|------------------|-------------|----------------|
| user_14   | 25               | 108         | 0.2315         |
| user_22   | 21               | 90          | 0.2333         |
| user_47   | 24               | 96          | 0.2500         |
| user_19   | 34               | 113         | 0.3009         |
| user_21   | 33               | 119         | 0.2773         |

![image](https://github.com/user-attachments/assets/e56460ed-71d3-429f-baed-f1cb04382369)


**Output Folder:**
- `output/debug_loyalty_scores/`
  
**Output:**

| user_id   | max_count        | total       | loyalty_score  |
|-----------|------------------|-------------|----------------|
|           |                  |             |                |

![image](https://github.com/user-attachments/assets/bcaa9127-8f53-4391-93b4-1e1920e9df9c)


---

### **7. Identify Night Owl Users**

**Objective:**
- Extract users who listen to music between 12:00 AM and 5:00 AM.

**Output Folder:**
- `output/task6_night_owl_users/`

**Output:**

| user_id   |
|-----------|
| user_14   |
| user_22   |
| user_47   |
| user_19   |
| user_21   |

![image](https://github.com/user-attachments/assets/3fab4a17-3288-4dde-bed0-6ce946884abf)

---

## **Output Format**

All outputs are stored in CSV format with headers:

```bash
output/
├── enriched_logs/
├── task1_user_favorite_genres/
├── task2_avg_listen_time_per_song/
├── task3_top_songs_this_week/
├── task4_happy_recommendations/
├── task5_genre_loyalty_scores/
├── task5_debug_loyalty_scores/
├── task6_night_owl_users/
```

---

## **Conclusion**

This assignment gave hands-on experience with Apache Spark’s DataFrame API. It simulates a music streaming analytics platform and provides practical exposure to data enrichment, transformation, filtering, aggregation, and recommendation use cases.

---
