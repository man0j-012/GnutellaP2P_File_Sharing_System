import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_scatter_response_time():
    # Load consolidated data
    df = pd.read_csv('consolidated_results.csv')
    
    # Set the aesthetic style of the plots
    sns.set(style="whitegrid")
    
    # Initialize the matplotlib figure
    plt.figure(figsize=(14, 8))
    
    # Create a scatter plot
    scatter = sns.scatterplot(
        x='Number of Clients',
        y='response_time_ms',
        hue='category',
        palette={'Same Super-Peer': 'g', 'Different Super-Peer': 'r'},
        data=df,
        alpha=0.6,
        edgecolor=None
    )
    
    # Add titles and labels
    plt.title('Response Time vs. Number of Concurrent Clients', fontsize=16)
    plt.xlabel('Number of Concurrent Clients', fontsize=14)
    plt.ylabel('Response Time (ms)', fontsize=14)
    
    # Adjust legend
    plt.legend(title='Category')
    
    # Save the plot
    plt.savefig('scatterplot_response_time.png')
    plt.show()

if __name__ == "__main__":
    plot_scatter_response_time()
