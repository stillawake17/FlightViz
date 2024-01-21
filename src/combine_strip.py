import json

# Replace 'file1.json' and 'file2.json' with your actual file paths
file1_path = 'data\combined_flights_data_strip.json'
file2_path = 'data\converted_data.json'

def read_json(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

# Read the first file
data1 = read_json(file1_path)
if data1 is None or not isinstance(data1, list):
    print(f"File {file1_path} is empty or not a list")
else:
    print(f"File {file1_path} read successfully with {len(data1)} records")

# Read the second file
data2 = read_json(file2_path)
if data2 is None or not isinstance(data2, list):
    print(f"File {file2_path} is empty or not a list")
else:
    print(f"File {file2_path} read successfully with {len(data2)} records")

# Append and save only if both files are read successfully
if data1 is not None and data2 is not None:
    combined_data = data1 + data2
    output_path = 'data/combined_strip.json'
    with open(output_path, 'w') as file:
        json.dump(combined_data, file, indent=4)
    print(f"Combined data saved successfully with {len(combined_data)} records")
