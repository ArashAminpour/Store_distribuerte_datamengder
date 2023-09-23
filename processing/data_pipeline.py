import pandas as pd
from processing.data_processing import *

def pipeline():
    track_points_raw = get_track_points(DATA_PATH, process_track_points)
    track_points_df = clean_track_points(track_points_raw)

    users_df = get_users(DATA_PATH)
    labels_df = get_labels(DATA_PATH, get_users)

    removed_users = find_removed_users(track_points_df, users_df)

    users_table = make_user_df(users_df, removed_users)
    activities_table = make_activity_df(track_points_df, labels_df)
    track_points_table = make_track_point_df(track_points_df)
    
    return users_table, activities_table, track_points_table

if __name__ == '__main__':
    pipeline()