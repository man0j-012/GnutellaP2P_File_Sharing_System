import pandas as pd
import glob
import os

def aggregate_csv_files():
    # Initialize an empty list to store DataFrames
    all_data_frames = []

    # Get all updated CSV files matching the pattern
    updated_csv_files = glob.glob("updated_results_*_clients.csv")

    if not updated_csv_files:
        print("No CSV files found matching the pattern 'updated_results_*_clients.csv'.")
        return

    for csv_file in updated_csv_files:
        print(f"Processing {csv_file}...")

        # Extract the number of clients from the filename
        # Expected filename format: updated_results_{num_clients}_clients.csv
        try:
            base_name = os.path.basename(csv_file)
            parts = base_name.split('_')
            # Assuming the third part is the number of clients
            num_clients_str = parts[2]
            num_clients = int(num_clients_str)
        except (IndexError, ValueError) as e:
            print(f"Error parsing filename '{csv_file}': {e}. Skipping this file.")
            continue

        # Load the CSV file
        try:
            df = pd.read_csv(csv_file)
            print(f"Loaded {len(df)} records from {csv_file}.")
        except Exception as e:
            print(f"Failed to read '{csv_file}': {e}. Skipping this file.")
            continue

        # Add 'Number of Clients' column
        df['Number of Clients'] = num_clients

        # Append the DataFrame to the list
        all_data_frames.append(df)

    if all_data_frames:
        # Concatenate all DataFrames in the list
        all_data = pd.concat(all_data_frames, ignore_index=True)
        print(f"Total records before sorting: {len(all_data)}.")

        # Sorting the DataFrame
        # Primary Sort: 'client_id' ascending
        # Secondary Sort: 'response_time_ms' ascending (optional)
        all_data.sort_values(by=['client_id', 'response_time_ms'], ascending=[True, True], inplace=True)

        print("DataFrame sorted by 'client_id' and 'response_time_ms'.")

        # Reset index after sorting
        all_data.reset_index(drop=True, inplace=True)

        # Save the consolidated and sorted data
        output_file = 'consolidated_results.csv'
        try:
            all_data.to_csv(output_file, index=False)
            print(f"Consolidated and sorted results saved to '{output_file}'.")
        except Exception as e:
            print(f"Failed to save '{output_file}': {e}.")
    else:
        print("No valid data to aggregate.")

if __name__ == "__main__":
    aggregate_csv_files()
