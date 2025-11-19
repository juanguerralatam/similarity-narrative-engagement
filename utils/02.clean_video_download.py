import json
import csv
import os

# Load the videos.json file
with open('output/videos.json', 'r') as f:
    videos = json.load(f)

# Prepare the CSV data
csv_data = []
for video in videos:
    video_id = video['videoId']
    channel_id = video['channelId']
    url = f"https://www.youtube.com/watch?v={video_id}"
    status = ""  # or set to 'pending' or whatever default
    csv_data.append([video_id, channel_id, url, status])

# Write to download.csv
with open('output/download.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['videoId', 'channelId', 'video_url', 'status'])
    writer.writerows(csv_data)

print("download.csv created successfully.")
