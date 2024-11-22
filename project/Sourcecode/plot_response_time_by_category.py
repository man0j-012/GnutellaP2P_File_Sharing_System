import matplotlib.pyplot as plt
import pandas as pd

def plot_response_time_by_category():
    # Load summary data
    summary_same = pd.read_csv('summary_same_sp.csv')
    summary_diff = pd.read_csv('summary_diff_sp.csv')

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(summary_same['Number of Clients'], summary_same['Average Response Time (ms)'], marker='o', linestyle='-', color='g', label='Same Super-Peer')
    plt.plot(summary_diff['Number of Clients'], summary_diff['Average Response Time (ms)'], marker='o', linestyle='-', color='r', label='Different Super-Peer')
    plt.title('Average Response Time by Super-Peer Category')
    plt.xlabel('Number of Concurrent Clients')
    plt.ylabel('Average Response Time (ms)')
    plt.grid(True)
    plt.xticks(sorted(summary_same['Number of Clients'].unique()))
    plt.legend()
    plt.savefig('response_time_by_category_plot.png')
    plt.show()

if __name__ == "__main__":
    plot_response_time_by_category()
