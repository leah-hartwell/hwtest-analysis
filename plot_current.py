import matplotlib.pyplot as plt
from raw_data_tools.post_process import RawDataFile
import os
import shutil
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="Choose file to parse and plot")
args = parser.parse_args()

data_streams = []
# Filename
file = args.filename
# Load raw data
raw_data = RawDataFile(file)

folder_name = file.split(".")
folder_name = folder_name[0]

# Create a figure and add subplots
fig = plt.figure()

# First subplot
ax1 = fig.add_subplot(211)
ax1.set_title("Subcomponents")

# Second subplot
ax2 = fig.add_subplot(212)
ax2.set_title("Power Lines")

# Plot data
for stream in raw_data.available_streams:
    if ".current" in stream:
        data_streams.append(stream)
        time = raw_data.streams[stream]['relative_sec']
        current = raw_data.streams[stream]['current']
        ax1.plot(time, current, label=stream)
    elif ".get_current" in stream:
        data_streams.append(stream)
        time = raw_data.streams[stream]['relative_sec']
        current = raw_data.streams[stream]['get_current']
        ax2.plot(time, current, label=stream)

save_folder = os.path.join(r"C:\Users\leah.hartwell\Documents\DockCamera\ChamberTesting", folder_name)
if not os.path.exists(save_folder):
    os.makedirs(save_folder)
raw_data.write_csvs(save_folder, stream_names=data_streams)
shutil.move(file, save_folder)

# Set labels and legends for the first subplot
ax1.set_xlabel('Time [s]')
ax1.set_ylabel('Current [mA]')
ax1.legend()

# Set y-axis limits
ax1.set_ylim(0, 500)

# Add major and minor grid lines
ax1.grid(which='major', linestyle='-', linewidth='0.5', color='black')
ax1.grid(which='minor', linestyle=':', linewidth='0.5', color='gray')
ax1.minorticks_on()

# Set labels and legends for the second subplot
ax2.set_xlabel('Time [s]')
ax2.set_ylabel('Current [mA]')
ax2.legend()

# Set y-axis limits
ax2.set_ylim(0, 800)

# Add major and minor grid lines
ax2.grid(which='major', linestyle='-', linewidth='0.5', color='black')
ax2.grid(which='minor', linestyle=':', linewidth='0.5', color='gray')
ax2.minorticks_on()

plt.savefig(save_folder + f"\{folder_name}.png")
# Show the plot
plt.show()