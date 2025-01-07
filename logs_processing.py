import pandas as pd
from datetime import timedelta
from io import StringIO

def store_in_file(data, file_path):
    data.to_csv(file_path, index=False)
    return file_path

def file_data_processing_repetition_summary(file_content, time_threshold):
    # Convert file content into a StringIO object to simulate file reading
    data_io = StringIO(file_content)

    # Read the CSV content into a DataFrame
    df = pd.read_csv(data_io)

    # Drop rows with missing user information (email or name) and where 'Is Undo' is True
    df = df.dropna(subset=["User Email", "User Name"])
    df = df[df["Is Undo"] == False]

    # Convert 'Time' column to datetime format for proper time manipulation
    df["Time"] = pd.to_datetime(df["Time"])

    # Drop duplicate rows with the same timestamp (for accurate time tracking)
    df.drop_duplicates(subset=["Time"], inplace=True)

    # Get the total number of logs before processing
    total_logs = len(df)

    # Sort the logs by User Email and Time so that we can analyze them chronologically for each user
    df = df.sort_values(by=["User Email", "Time"])

    # Initialize an empty list to store the summarized results
    results = []

    # Iterate through each unique user (grouping by User Email)
    for user_email, group in df.groupby("User Email"):
        # Initialize variables to track the start time, end time, last edit type, and occurrence count for each user
        start_time = None
        end_time = None
        last_edit_type = None
        occurrence_count = 0

        # Process each log entry for the current user
        for index, row in group.iterrows():
            current_time = row["Time"]
            current_edit_type = row["Edit Type"]

            if start_time is None:
                # For the first log entry, initialize the start time, end time, and edit type
                start_time = current_time
                end_time = current_time
                last_edit_type = current_edit_type
                occurrence_count = 1
            else:
                # Calculate the time difference between the current log and the last log's end time
                time_diff = current_time - end_time

                # Check if we should start a new task (either due to a large time gap or a change in edit type)
                if (
                    time_diff > timedelta(minutes=time_threshold)  # More than the time threshold
                    or current_edit_type != last_edit_type  # Edit type changed
                ):
                    # If so, finalize the current task and store its details in the results
                    results.append(
                        {
                            "User Email": user_email,
                            "Edit Type": last_edit_type,
                            "Start Time": start_time,
                            "End Time": end_time,
                            "Occurrences": occurrence_count,
                            "Date": start_time.date(),
                        }
                    )
                    # Start a new task with the current log entry
                    start_time = current_time
                    occurrence_count = 1
                    last_edit_type = current_edit_type
                else:
                    # Otherwise, continue the current task by updating the end time and occurrence count
                    occurrence_count += 1

                # Update the end time to the current log's time
                end_time = current_time

        # After processing all logs for the user, add the final task (for the last sequence of logs)
        results.append(
            {
                "User Email": user_email,
                "Edit Type": last_edit_type,
                "Start Time": start_time,
                "End Time": end_time,
                "Occurrences": occurrence_count,
                "Date": start_time.date(),
            }
        )

    # Convert the results list into a DataFrame for easy analysis
    result_df = pd.DataFrame(results)

    # If there are no results, return an empty DataFrame and summary stats
    if result_df.empty:
        return result_df, total_logs, 0

    # Sort the resulting DataFrame by start time to maintain chronological order
    result_df = result_df.sort_values(by=["Start Time"])

    # Calculate the total time spent on each task (in seconds) by subtracting start time from end time
    result_df["Time Spent"] = (
        result_df["End Time"] - result_df["Start Time"]
    ).dt.total_seconds()

    # Group the results by date, user email, and edit type, summing occurrences and time spent
    result_df = (
        result_df.groupby(["Date", "User Email", "Edit Type"])
        .agg({"Occurrences": "sum", "Time Spent": "sum"})
        .reset_index()
    )

    # Get the total number of summarized tasks after processing
    summarized_logs = len(result_df)
    
    # Store the result in a file
    store_in_file(result_df, "output.csv")


# Example usage
file = "./input.csv"
time_threshold = 30 # minutes

with open(file, "r") as f:
    file_content = f.read()

file_data_processing_repetition_summary(file_content, time_threshold)
