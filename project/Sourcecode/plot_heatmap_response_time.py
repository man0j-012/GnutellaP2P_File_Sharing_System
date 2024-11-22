import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_heatmap_response_time():
    # Load consolidated data
    df = pd.read_csv('consolidated_results.csv')
    
    # Pivot the data to create a matrix suitable for heatmap
    heatmap_data = df.pivot_table(index='client_id', columns='Number of Clients', values='response_time_ms', aggfunc='mean')
    
    # Set the aesthetic style of the plots
    sns.set(style="white")
    
    # Initialize the matplotlib figure
    plt.figure(figsize=(16, 12))
    
    # Create a heatmap
    sns.heatmap(heatmap_data, annot=True, fmt=".2f", cmap="YlGnBu", linewidths=.5)
    
    # Add titles and labels
    plt.title('Heatmap of Average Response Times by Client ID and Number of Clients', fontsize=16)
    plt.xlabel('Number of Concurrent Clients', fontsize=14)
    plt.ylabel('Client ID', fontsize=14)
    
    # Save the plot
    plt.savefig('heatmap_response_time.png')
    plt.show()

if __name__ == "__main__":
    plot_heatmap_response_time()
