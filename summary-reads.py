import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file using the correct relative path
data = pd.read_csv('log/read_thread.csv', sep=',', header=None)

# Add column names for better readability
data.columns = ['Timestamp', 'Operation', 'Thread', 'Duration']

# Convert timestamp to datetime
data['Timestamp'] = pd.to_datetime(data['Timestamp'], format='%Y%m%d%H%M%S')

# Create the plot
plt.figure(figsize=(12, 6))
plt.plot(data['Timestamp'], data['Duration'], 'b-', label='Read Duration')
plt.title('Read Operations Duration Over Time')
plt.xlabel('Timestamp')
plt.ylabel('Duration (ms)')
plt.grid(True)
plt.legend()

# Save the plot
plt.savefig('read_operations.png')
plt.close() 