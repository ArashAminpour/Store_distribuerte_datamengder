import pandas as pd
import os

# Check file structure
def check_unique_activities(data_path):
    """
        Check if all the values in the activity column are unique for each directory.
    """
    data_path = os.path.join(data_path, "Data")
    lists_by_directory = {}

    # Format the directory name to have leading zeros if needed
    for i in range(182):
        directory_name = f"{i:03d}"

        files_in_directory = os.listdir(os.path.join(data_path, directory_name, "Trajectory"))
        lists_by_directory[directory_name] = files_in_directory

    # Check if all values in each list are unique
    non_unique_dirs = []
    for directory_name, file_list in lists_by_directory.items():
        if len(file_list) != len(set(file_list)):
            non_unique_dirs.append(directory_name)
    
    results = pd.DataFrame({'directory_name': non_unique_dirs})
    
    try:
        assert len(results) == 0
    except AssertionError as e:
        print(e)
        return results

# Final checks
def test_duplicate_rows(df, columns_to_check):
    """
        Check if there are any duplicate rows in the dataframe df for the columns_to_check.
    """
    print("Checking for duplicate rows...")
    duplicate_rows = df.duplicated(subset=columns_to_check, keep=False)
    results = df[duplicate_rows]
    try:
        assert len(results) == 0
        print("No duplicate rows found")
    except AssertionError as e:
        print(e)
        return results

def test_rows_count(df, max_rows=2500):
    """
        Check if the number of rows in the dataframe is less than the max_rows.
    """
    print("Checking partitions...")
    results = df.groupby(['activity_id']).size().reset_index(name='counts').query(f'counts > {max_rows}')
    
    try:
        assert len(results) == 0
        print("No partition with more than {max_rows} rows found}")
    except AssertionError as e:
        print(e)
        return results

def test_referential_integrity(df, df_ref, col, col_ref):
    """
        Check if the values in the column col of the dataframe df are present in the column col_ref of the dataframe df_ref.
    """
    print("Checking referential integrity...")
    results = df[~df[col].isin(df_ref[col_ref])]
    
    try:
        assert len(results) == 0
        print("No referential integrity errors found")
    except AssertionError as e:
        print(e)
        return results

def test_no_nulls(df, columns_to_check: list):
    """
        Check if there are any null values in the columns_to_check of the dataframe df.
    """
    print("Checking for null values...")
    results = df[df[columns_to_check].isnull().any(axis=1)]
    
    try:
        assert len(results) == 0
        print("No null values found")
    except AssertionError as e:
        print(e)
        return results
    

def run_tests(users, activities, track_points):
    # Test for duplicate rows
    users_unique_rows_test = test_duplicate_rows(users, ['id'])
    activities_unique_rows_test = test_duplicate_rows(activities, ['user_id', 'start_date_time'])
    track_points_unique_rows_test = test_duplicate_rows(track_points, ['activity_id', 'date_time'])

    # Test for rows count
    activities_rows_count_test = test_rows_count(track_points, max_rows=2500)

    # Test for referential integrity
    activities_referential_integrity_test = test_referential_integrity(activities, users, 'user_id', 'id')
    track_points_referential_integrity_test = test_referential_integrity(track_points, activities, 'activity_id', 'id')

    # Test for null values
    users_null_values_test = test_no_nulls(users, ['id'])
    activities_null_values_test = test_no_nulls(activities, ['id', 'user_id', 'start_date_time', 'end_date_time'])
    track_points_null_values_test = test_no_nulls(track_points, ['activity_id', 'lat', 'lon', 'altitude', 'date_days', 'date_time'])

    return {
        'users_unique_rows_test': users_unique_rows_test,
        'activities_unique_rows_test': activities_unique_rows_test,
        'track_points_unique_rows_test': track_points_unique_rows_test,
        'activities_rows_count_test': activities_rows_count_test,
        'activities_referential_integrity_test': activities_referential_integrity_test,
        'track_points_referential_integrity_test': track_points_referential_integrity_test,
        'users_null_values_test': users_null_values_test,
        'activities_null_values_test': activities_null_values_test,
        'track_points_null_values_test': track_points_null_values_test
    }

