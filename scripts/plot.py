import csv
import argparse

from matplotlib import pyplot as plt

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("files", help="Files to plot", type=str, nargs='+')
parser.add_argument("-o", "--output", help="Save image to the given filename.")
args = parser.parse_args()

# Define the path to your CSV files
csv_files = args.files

# Create a list to store data from each file
data = []

# Load data from each CSV file
for file in csv_files:
    with open(file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        file_data = {'name': file, 'elapsed': [], 'lag': []}
        for row in reader:
            file_data['elapsed'].append(float(row['elapsed']))
            file_data['lag'].append(float(row['lag']))
        data.append(file_data)

# Create a plot
fig, ax = plt.subplots()

# Assign different colors to each file
colors = ['red', 'blue', 'green', 'orange', 'purple', 'black']

# Plot the data from each file
for i, file_data in enumerate(data):
    elapsed = file_data['elapsed']
    lag = file_data['lag']
    ax.plot(elapsed, lag, color=colors[i], label=file_data['name'], linewidth=1.0)

# Set labels and title for the plot
ax.set_xlabel('Time (s)')
ax.set_ylabel('Lag (#)')
ax.set_title('Kafka-to-kafka throughput')

# Add a legend
ax.legend()

# Display or save the plot
if args.output:
    plt.savefig(args.output, dpi=400)
else:
    plt.show()
