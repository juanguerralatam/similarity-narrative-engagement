import json
import csv
from collections import defaultdict

def parse_duration(duration_str):
    """Parse duration string like '0:07:15' to seconds."""
    parts = duration_str.split(':')
    if len(parts) == 3:
        hours, minutes, seconds = map(int, parts)
        return hours * 3600 + minutes * 60 + seconds
    elif len(parts) == 2:
        minutes, seconds = map(int, parts)
        return minutes * 60 + seconds
    else:
        return 0

def generate_analytics(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    channel_stats = defaultdict(lambda: {
        'num_videos': 0,
        'total_duration': 0,
        'total_views': 0,
        'total_likes': 0,
        'total_favorites': 0,
        'total_comments': 0
    })
    
    for video in data:
        channel = video['channelTitle']
        channel_stats[channel]['num_videos'] += 1
        channel_stats[channel]['total_duration'] += parse_duration(video['duration'])
        channel_stats[channel]['total_views'] += int(video['viewCount'])
        channel_stats[channel]['total_likes'] += int(video['likeCount'])
        channel_stats[channel]['total_favorites'] += int(video['favoriteCount'])
        channel_stats[channel]['total_comments'] += int(video['commentCount'])
    
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['Channel', 'Num_Videos', 'Total_Duration_Hours', 'Total_Views', 'Total_Likes', 'Total_Favorites', 'Total_Comments']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for channel, stats in channel_stats.items():
            writer.writerow({
                'Channel': channel,
                'Num_Videos': stats['num_videos'],
                'Total_Duration_Hours': stats['total_duration'] / 3600,
                'Total_Views': stats['total_views'],
                'Total_Likes': stats['total_likes'],
                'Total_Favorites': stats['total_favorites'],
                'Total_Comments': stats['total_comments']
            })

if __name__ == "__main__":
    generate_analytics('output/videos.json', 'appendix/tables/01analytics_videos.csv')
