import pandas as pd

# Define the data for Linear Topology
data = {
    'Client Number': [
        'Client 1', 'Client 2', 'Client 3', 'Client 4', 'Client 5',
        'Client 6', 'Client 7', 'Client 8', 'Client 9', 'Client 10'
    ],
    'Average Response Time (ms)': [0.73, 0.50, 0.64, 0.43, 0.39, 0.48, 0.46, 0.48, 0.54, 0.41],
    'Total Hits': [400, 800, 1200, 1400, 1600, 1800, 2000, 2000, 2000, 2000]
}

# Create a DataFrame from the data
df = pd.DataFrame(data)

# Calculate Overall Average Response Time and Total Hits
overall_average = df['Average Response Time (ms)'].mean()
overall_hits = df['Total Hits'].sum()

# Create a summary row as a DataFrame
summary = pd.DataFrame({
    'Client Number': ['Overall Average'],
    'Average Response Time (ms)': [round(overall_average, 2)],
    'Total Hits': [overall_hits]
})

# Concatenate the summary row to the original DataFrame using pd.concat
df = pd.concat([df, summary], ignore_index=True)

# Specify the CSV file name
csv_file = 'linear_topology_results.csv'

# Write the DataFrame to a CSV file
df.to_csv(csv_file, index=False)

print(f"CSV file '{csv_file}' has been created successfully.")
