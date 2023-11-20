import pandas as pd

def merge_location_data(weather_file, location_file, output_file):
    # Read the weather data
    weather_data = pd.read_csv(weather_file, delimiter=';', header=None)

    # Read the location data
    location_data = pd.read_csv(location_file, delimiter=';', header=None)

    # Merge the data based on the common ID
    merged_data = pd.merge(weather_data, location_data, on=0)

    # Save the merged data to a new CSV file
    merged_data.to_csv(output_file, sep=';', index=False, header=False)

# Example usage
weather_file = r'E:\amanca\data\kecamatanforecast-jakarta.csv'
location_file = r'E:\amanca\data\kecamatan_geofeatures.csv'
output_file = r'E:\amanca\data\merged_data_jakarta.csv'
merge_location_data(weather_file, location_file, output_file)

import csv
import json

def csv_to_json(csv_file_path, json_file_path):
    data = {}
    
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        next(csv_reader)  # Skip header row
        
        for row in csv_reader:
            location_id = row[0]
            
            if location_id not in data:
                data[location_id] = {
                    "id": location_id,
                    "kecamatan": row[11].strip(),
                    "kabkot": row[12].strip(),
                    "lintang": float(row[14]),
                    "bujur": float(row[15]),
                    "weather": []
                }
            
            weather = {
                "tanggal": row[1],
                "cuaca": int(row[8]),
                "suhu": row[7].strip(),
                "rh": row[6].strip(),
                "arah": row[9].strip(),
                "kec": row[10].strip()
            }
            
            data[location_id]["weather"].append(weather)
    
    json_data = list(data.values())
    
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

# Example usage
csv_file_path = r'E:\amanca\data\merged_data_jakarta.csv'
json_file_path = r'E:\amanca\data\outputjakarta.json'
csv_to_json(csv_file_path, json_file_path)

print ("Alhamdulillah Sudah Jadi File JSON nya Broo")


