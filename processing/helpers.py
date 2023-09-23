import pandas as pd

def process_track_points(file_path, user, activity, max_allowed_rows: int = 2506):
    """
        Process a track point file and return a list of dictionaries
    """
    
    data = []
    with open(file_path) as f:
        # Check the number of rows in the file
        line_count = sum(1 for _ in f)
        if line_count > max_allowed_rows:
            print(f"Skipping {file_path} - File is too long")
            return data

        # Reset the file pointer to read the file again
        f.seek(0)

        # Continue with processing the file
        for _ in range(6):
            next(f)
        for line in f:
            lat, lon, _, altitude, date_days, date_string, time_string = line.split(',')
            data.append({
                'user': user,
                'activity': activity,
                'lat': lat,
                'lon': lon,
                'altitude': altitude,
                'date_days': date_days,
                'date_string': date_string,
                'time_string': time_string
            })
    return data

def find_removed_users(activity_df: pd.DataFrame, users_df: pd.DataFrame):
    """
        Find users that are not in the activity_df
    """
    all_users = users_df.id
    unique_users = set(activity_df["user"].astype(str))
    removed_users = [user for user in all_users if user not in unique_users]
    return removed_users
