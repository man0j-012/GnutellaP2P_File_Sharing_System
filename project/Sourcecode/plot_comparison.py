import matplotlib.pyplot as plt

# Data for All-to-All Topology
all_to_all_clients = list(range(1, 11))
all_to_all_hits = [400, 800, 1200, 1600, 2000, 2400, 2800, 3200, 3187, 4000]
all_to_all_response_times = [1.24, 1.19, 3.59, 151.96, 511.72, 879.73, 1650.13, 1535.96, 1812.33, 2146.98]

# Data for Linear Topology
linear_clients = list(range(1, 11))
linear_hits = [400, 800, 1200, 1400, 1600, 1800, 2000, 2000, 2000, 2000]
linear_response_times = [0.73, 0.50, 0.64, 0.43, 0.39, 0.48, 0.46, 0.48, 0.54, 0.41]

# Plot Average Response Time for Linear Topology
plt.figure(figsize=(12, 6))
plt.plot(linear_hits, linear_response_times, marker='s', color='blue', label='Linear Topology')
plt.xlabel('Number of Hits')
plt.ylabel('Average Response Time (ms)')
plt.title('Average Response Time vs. Number of Hits for Linear Topology')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# Plot Comparison Between All-to-All and Linear Topologies
plt.figure(figsize=(12, 6))
plt.plot(all_to_all_hits, all_to_all_response_times, marker='o', color='red', label='All-to-All Topology')
plt.plot(linear_hits, linear_response_times, marker='s', color='blue', label='Linear Topology')
plt.xlabel('Number of Hits')
plt.ylabel('Average Response Time (ms)')
plt.title('Performance Comparison: All-to-All vs. Linear Topology')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
