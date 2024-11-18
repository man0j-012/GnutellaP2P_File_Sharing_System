import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import pytz

# ------------------------------
# Configuration
# ------------------------------

# Directory where your CSV files are stored
CSV_DIRECTORY = r"C:\Users\dattu\p2p-file-sharing\project\Test_Push_Performance\csv"  # Update this path as needed

# TTR values corresponding to your CSV files (in seconds)
TTR_VALUES = [30, 60, 120, 180]

# CSV file naming pattern
CSV_FILE_PATTERN = "TTR={}.csv"  # E.g., "TTR=30.csv"

# Timezone used in your simulation
TIMEZONE = 'US/Central'

# ------------------------------
# Function Definitions
# ------------------------------

def list_all_files(directory):
    """
    Lists all files in the specified directory.
    
    Parameters:
        directory (str): Path to the directory.
    
    Returns:
        list: List of filenames in the directory.
    """
    try:
        files = os.listdir(directory)
        print(f"\nFiles in '{directory}':")
        for file in files:
            print(f"- {file}")
        return files
    except FileNotFoundError:
        print(f"Error: Directory '{directory}' does not exist.")
        return []
    except Exception as e:
        print(f"An error occurred while listing files: {e}")
        return []

def load_csv_files(directory, ttr_values, file_pattern):
    """
    Loads multiple CSV files into a dictionary of DataFrames keyed by TTR.

    Parameters:
        directory (str): Path to the directory containing CSV files.
        ttr_values (list): List of TTR values to load.
        file_pattern (str): Pattern of CSV filenames with a placeholder for TTR.

    Returns:
        dict: Dictionary with TTR as keys and DataFrames as values.
    """
    data_frames = {}
    for ttr in ttr_values:
        filename = file_pattern.format(ttr)
        filepath = os.path.join(directory, filename)
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} does not exist. Skipping.")
            continue
        try:
            df = pd.read_csv(filepath)
            data_frames[ttr] = df
            print(f"Loaded {filepath} with {len(df)} records.")
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
    return data_frames

def process_data(df, timezone):
    """
    Processes the DataFrame by parsing timestamps and flagging invalid queries.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        timezone (str): Timezone string for localization.

    Returns:
        pd.DataFrame: Processed DataFrame with new flags.
    """
    # Check if necessary columns exist
    required_columns = ['Query Last-Mod-Time', 'Origin Last-Mod-Time']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Convert timestamp columns to datetime objects with timezone
    try:
        df['Query Last-Mod-Time'] = pd.to_datetime(df['Query Last-Mod-Time'], errors='coerce')
        df['Origin Last-Mod-Time'] = pd.to_datetime(df['Origin Last-Mod-Time'], errors='coerce')
    except Exception as e:
        print(f"Error parsing dates: {e}")
        raise

    # Localize to the specified timezone
    # Assuming timestamps are in UTC; adjust if necessary
    if df['Query Last-Mod-Time'].dt.tz is None:
        df['Query Last-Mod-Time'] = df['Query Last-Mod-Time'].dt.tz_localize('UTC').dt.tz_convert(timezone)
    else:
        df['Query Last-Mod-Time'] = df['Query Last-Mod-Time'].dt.tz_convert(timezone)

    if df['Origin Last-Mod-Time'].dt.tz is None:
        df['Origin Last-Mod-Time'] = df['Origin Last-Mod-Time'].dt.tz_localize('UTC').dt.tz_convert(timezone)
    else:
        df['Origin Last-Mod-Time'] = df['Origin Last-Mod-Time'].dt.tz_convert(timezone)
    
    # Check for any parsing errors
    if df['Query Last-Mod-Time'].isnull().any():
        print("Warning: Some 'Query Last-Mod-Time' entries could not be parsed and are set as NaT.")
    if df['Origin Last-Mod-Time'].isnull().any():
        print("Warning: Some 'Origin Last-Mod-Time' entries could not be parsed and are set as NaT.")
    
    # Define invalid queries based on timestamp comparison
    df['Is Invalid Based on Time'] = df['Query Last-Mod-Time'] < df['Origin Last-Mod-Time']
    
    return df

def compute_statistics(df):
    """
    Computes statistics related to invalid queries.

    Parameters:
        df (pd.DataFrame): The processed DataFrame.

    Returns:
        dict: Dictionary containing total queries, invalid queries, and percentage.
    """
    total_queries = len(df)
    invalid_queries = df['Is Invalid Based on Time'].sum()
    percentage_invalid = (invalid_queries / total_queries) * 100 if total_queries > 0 else 0
    
    stats = {
        'Total Queries': total_queries,
        'Invalid Queries': invalid_queries,
        'Percentage of Invalid Queries': round(percentage_invalid, 2)
    }
    
    return stats

def aggregate_results(data_frames, timezone):
    """
    Aggregates statistics for multiple DataFrames.

    Parameters:
        data_frames (dict): Dictionary with TTR as keys and DataFrames as values.
        timezone (str): Timezone string for localization.

    Returns:
        pd.DataFrame: Summary DataFrame containing statistics for each TTR.
    """
    summary = []
    for ttr, df in data_frames.items():
        try:
            processed_df = process_data(df, timezone)
            stats = compute_statistics(processed_df)
            stats['TTR (seconds)'] = ttr
            summary.append(stats)
        except Exception as e:
            print(f"Error processing TTR={ttr}: {e}")
    summary_df = pd.DataFrame(summary)
    return summary_df

def visualize_results(summary_df):
    """
    Visualizes the percentage of invalid queries across different TTRs.

    Parameters:
        summary_df (pd.DataFrame): Summary DataFrame containing statistics for each TTR.

    Returns:
        None
    """
    sns.set(style="whitegrid")
    
    # Sort the summary_df by TTR for better visualization
    summary_df_sorted = summary_df.sort_values(by='TTR (seconds)')
    
    # a. Bar Chart: Percentage of Invalid Queries by TTR
    plt.figure(figsize=(10, 6))
    sns.barplot(x='TTR (seconds)', y='Percentage of Invalid Queries', data=summary_df_sorted, palette='viridis')
    plt.title('Percentage of Invalid Queries by TTR')
    plt.xlabel('TTR (seconds)')
    plt.ylabel('Invalid Query Percentage (%)')
    plt.ylim(0, 100)
    
    # Annotate bars with percentage values
    for idx, row in summary_df_sorted.iterrows():
        plt.text(x=row['TTR (seconds)'], y=row['Percentage of Invalid Queries'] + 1, 
                 s=f"{row['Percentage of Invalid Queries']}%", 
                 ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.show()
    
    # b. Line Plot: Invalid Query Percentage vs. TTR
    plt.figure(figsize=(10, 6))
    sns.lineplot(x='TTR (seconds)', y='Percentage of Invalid Queries', data=summary_df_sorted, marker='o', linestyle='-')
    plt.title('Invalid Query Percentage vs. TTR')
    plt.xlabel('TTR (seconds)')
    plt.ylabel('Invalid Query Percentage (%)')
    plt.ylim(0, 100)
    
    # Annotate points with percentage values
    for idx, row in summary_df_sorted.iterrows():
        plt.text(x=row['TTR (seconds)'], y=row['Percentage of Invalid Queries'] + 1, 
                 s=f"{row['Percentage of Invalid Queries']}%", 
                 ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.show()

def save_summary(summary_df, output_path="summary_statistics.csv"):
    """
    Saves the summary statistics to a CSV file.

    Parameters:
        summary_df (pd.DataFrame): Summary DataFrame containing statistics for each TTR.
        output_path (str): Path to save the summary CSV.

    Returns:
        None
    """
    summary_df_sorted = summary_df.sort_values(by='TTR (seconds)')
    summary_df_sorted.to_csv(output_path, index=False)
    print(f"Summary statistics saved to {output_path}")

# ------------------------------
# Main Execution
# ------------------------------

def main():
    # List all files in the directory for debugging
    all_files = list_all_files(CSV_DIRECTORY)
    
    # Load CSV files
    data_frames = load_csv_files(CSV_DIRECTORY, TTR_VALUES, CSV_FILE_PATTERN)
    
    if not data_frames:
        print("No CSV files loaded. Exiting.")
        return
    
    # Aggregate statistics
    summary_df = aggregate_results(data_frames, TIMEZONE)
    
    if summary_df.empty:
        print("No data to summarize. Exiting.")
        return
    
    # Display summary statistics
    print("\n--- Summary Statistics ---")
    print(summary_df[['TTR (seconds)', 'Total Queries', 'Invalid Queries', 'Percentage of Invalid Queries']])
    
    # Save summary to CSV
    save_summary(summary_df)
    
    # Visualize results
    visualize_results(summary_df)

# ------------------------------
# Run the Script
# ------------------------------

if __name__ == "__main__":
    main()
