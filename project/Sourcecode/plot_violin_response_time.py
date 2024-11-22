import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_violin_response_time():
    # Load consolidated data
    df = pd.read_csv('consolidated_results.csv')
    
    # Set the aesthetic style of the plots
    sns.set(style="whitegrid")
    
    # Initialize the matplotlib figure
    plt.figure(figsize=(14, 8))
    
    # Create a violin plot
    sns.violinplot(x='Number of Clients', y='response_time_ms', data=df, palette="Set2", inner='quartile')
    
    # Add titles and labels
    plt.title('Violin Plot of Response Times per Number of Clients', fontsize=16)
    plt.xlabel('Number of Concurrent Clients', fontsize=14)
    plt.ylabel('Response Time (ms)', fontsize=14)
    
    # Save the plot
    plt.savefig('violinplot_response_time.png')
    plt.show()

if __name__ == "__main__":
    plot_violin_response_time()
