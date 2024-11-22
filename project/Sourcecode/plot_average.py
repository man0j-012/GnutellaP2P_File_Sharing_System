import matplotlib.pyplot as plt
import pandas as pd

def plot_average_response_time():
    # Load summary data
    summary = pd.read_csv('summary_response_times.csv')

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(summary['Number of Clients'], summary['Average Response Time (ms)'], marker='o', linestyle='-', color='b')
    plt.title('Average Response Time vs. Number of Concurrent Clients')
    plt.xlabel('Number of Concurrent Clients')
    plt.ylabel('Average Response Time (ms)')
    plt.grid(True)
    plt.xticks(summary['Number of Clients'])
    plt.savefig('average_response_time_plot.png')
    plt.show()

if __name__ == "__main__":
    plot_average_response_time()
