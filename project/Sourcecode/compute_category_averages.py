import pandas as pd

def compute_category_averages():
    # Load consolidated data
    df = pd.read_csv('consolidated_results.csv')

    # Segregate data based on category
    same_sp = df[df['category'] == 'Same Super-Peer']
    diff_sp = df[df['category'] == 'Different Super-Peer']

    # Calculate average response time per client count for each category
    summary_same = same_sp.groupby('Number of Clients')['response_time_ms'].mean().reset_index()
    summary_diff = diff_sp.groupby('Number of Clients')['response_time_ms'].mean().reset_index()

    # Rename columns for clarity
    summary_same.rename(columns={'response_time_ms': 'Average Response Time (ms)'}, inplace=True)
    summary_diff.rename(columns={'response_time_ms': 'Average Response Time (ms)'}, inplace=True)

    # Save summaries to CSV
    summary_same.to_csv('summary_same_sp.csv', index=False)
    summary_diff.to_csv('summary_diff_sp.csv', index=False)

    print("Average Response Times - Same Super-Peer:")
    print(summary_same)
    print("\nAverage Response Times - Different Super-Peer:")
    print(summary_diff)

if __name__ == "__main__":
    compute_category_averages()
