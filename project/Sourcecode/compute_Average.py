import pandas as pd

def compute_averages():
    # Load consolidated data
    df = pd.read_csv('consolidated_results.csv')

    # Calculate average response time per client count
    summary = df.groupby('Number of Clients')['response_time_ms'].mean().reset_index()
    summary.rename(columns={'response_time_ms': 'Average Response Time (ms)'}, inplace=True)

    # Save summary to CSV
    summary.to_csv('summary_response_times.csv', index=False)
    print("Summary of Average Response Times:")
    print(summary)

if __name__ == "__main__":
    compute_averages()
