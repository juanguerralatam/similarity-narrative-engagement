import csv
import json
import os

def mark_cleaned_ids_as_done(cleaned_ids_file: str = "cleaned_video_ids.json", csv_file: str = "output/download.csv"):
    """
    Mark all video IDs from the cleaned IDs file as 'done' in the download.csv file.
    
    Args:
        cleaned_ids_file (str): Path to the JSON file with cleaned video IDs.
        csv_file (str): Path to the CSV file.
    """
    if not os.path.exists(cleaned_ids_file):
        print(f"Cleaned IDs file {cleaned_ids_file} not found.")
        return
    
    if not os.path.exists(csv_file):
        print(f"CSV file {csv_file} not found.")
        return
    
    # Load cleaned IDs
    with open(cleaned_ids_file, 'r') as f:
        cleaned_ids = set(json.load(f))
    
    # Load CSV rows
    rows = []
    with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['videoId'] in cleaned_ids:
                row['status'] = 'done'
            rows.append(row)
    
    # Write back
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        if rows:
            writer = csv.DictWriter(file, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    
    print(f"Marked {len(cleaned_ids)} IDs as done in {csv_file}.")

# Example usage:
if __name__ == "__main__":
    mark_cleaned_ids_as_done()