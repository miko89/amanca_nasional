import pandas as pd
import csv
import json

def merge_location_data(weather_file, location_file, output_file):
    # Read the weather data
    weather_data = pd.read_csv(weather_file, delimiter=';', header=None)

    # Read the location data
    location_data = pd.read_csv(location_file, delimiter=';', header=None)

    # Merge the data based on the common ID
    merged_data = pd.merge(weather_data, location_data, on=0)

    # Save the merged data to a new CSV file
    merged_data.to_csv(output_file, sep=';', index=False, header=False)

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
                "cuaca": int(float(row[8])),
                "suhu": row[7].strip(),
                "rh": row[6].strip(),
                "arah": row[9].strip(),
                "kec": row[10].strip()
            }
            
            data[location_id]["weather"].append(weather)
            print(row[0])
    print("ok")

    json_data = list(data.values())
    
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

# Example usage
def main():
    weather_files = [
        r'E:\amanca_nasional\data\presentweather-banten.csv',
        r'E:\amanca_nasional\data\presentweather-jakarta.csv',
        r'E:\amanca_nasional\data\presentweather-jawabarat.csv',
        r'E:\amanca_nasional\data\presentweather-sumsel.csv',
        r'E:\amanca_nasional\data\presentweather-sumut.csv',
        r'E:\amanca_nasional\data\presentweather-sulsel.csv',
        r'E:\amanca_nasional\data\presentweather-sultenggara.csv',
        r'E:\amanca_nasional\data\presentweather-sulut.csv',
        r'E:\amanca_nasional\data\presentweather-sumbar.csv',
        r'E:\amanca_nasional\data\presentweather-papuabarat.csv',
        r'E:\amanca_nasional\data\presentweather-riau.csv',
        r'E:\amanca_nasional\data\presentweather-sulbar.csv',
        r'E:\amanca_nasional\data\presentweather-maluku.csv',
        r'E:\amanca_nasional\data\presentweather-malut.csv',
        r'E:\amanca_nasional\data\presentweather-ntb.csv',
        r'E:\amanca_nasional\data\presentweather-ntt.csv',
        r'E:\amanca_nasional\data\presentweather-papua.csv',
        r'E:\amanca_nasional\data\presentweather-kaltim.csv',
        r'E:\amanca_nasional\data\presentweather-kepriau.csv',
        r'E:\amanca_nasional\data\presentweather-lampung.csv',
        r'E:\amanca_nasional\data\presentweather-jawatengah.csv',
        r'E:\amanca_nasional\data\presentweather-jawatimur.csv',
        r'E:\amanca_nasional\data\presentweather-kalbar.csv',
        r'E:\amanca_nasional\data\presentweather-kalsel.csv',
        r'E:\amanca_nasional\data\presentweather-kalteng.csv',
        r'E:\amanca_nasional\data\presentweather-babel.csv',
        r'E:\amanca_nasional\data\presentweather-bali.csv',
        r'E:\amanca_nasional\data\presentweather-bengkulu.csv',
        r'E:\amanca_nasional\data\presentweather-gorontalo.csv',
        r'E:\amanca_nasional\data\presentweather-jambi.csv',
        r'E:\amanca_nasional\data\presentweather-sulteng.csv',
        r'E:\amanca_nasional\data\presentweather-aceh.csv',
        r'E:\amanca_nasional\data\presentweather-kaluta.csv',
        r'E:\amanca_nasional\data\presentweather-bali.csv',
        r'E:\amanca_nasional\data\presentweather-jogyakarta.csv'
    ]
    location_file = r'E:\amanca_nasional\data\kecamatan_geofeatures.csv'
    output_files = [
        r'E:\amanca_nasional\data\merged_data_banten_ps.csv',
        r'E:\amanca_nasional\data\merged_data_jakarta_ps.csv',
        r'E:\amanca_nasional\data\merged_data_jawabarat_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sumsel_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sumut_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sulsel_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sultenggara_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sulut_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sumbar_ps.csv',
        r'E:\amanca_nasional\data\merged_data_papuabarat_ps.csv',
        r'E:\amanca_nasional\data\merged_data_riau_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sulbar_ps.csv',
        r'E:\amanca_nasional\data\merged_data_maluku_ps.csv',
        r'E:\amanca_nasional\data\merged_data_malut_ps.csv',
        r'E:\amanca_nasional\data\merged_data_ntb_ps.csv',
        r'E:\amanca_nasional\data\merged_data_ntt_ps.csv',
        r'E:\amanca_nasional\data\merged_data_papua_ps.csv',
        r'E:\amanca_nasional\data\merged_data_kaltim_ps.csv',
        r'E:\amanca_nasional\data\merged_data_kepriau_ps.csv',
        r'E:\amanca_nasional\data\merged_data_lampung_ps.csv',
        r'E:\amanca_nasional\data\merged_data_jawatengah_ps.csv',
        r'E:\amanca_nasional\data\merged_data_jawatimur_ps.csv',
        r'E:\amanca_nasional\data\merged_data_kalbar_ps.csv',
        r'E:\amanca_nasional\data\merged_data_kalsel_ps.csv',
        r'E:\amanca_nasional\data\merged_data_kalteng_ps.csv',
        r'E:\amanca_nasional\data\merged_data_babel_ps.csv',
        r'E:\amanca_nasional\data\merged_data_bali_ps.csv',
        r'E:\amanca_nasional\data\merged_data_bengkulu_ps.csv',
        r'E:\amanca_nasional\data\merged_data_gorontalo_ps.csv',
        r'E:\amanca_nasional\data\merged_data_jambi_ps.csv',
        r'E:\amanca_nasional\data\merged_data_sulteng_ps.csv',
        r'E:\amanca_nasional\data\merged_data_aceh_ps.csv',
        r'E:\amanca_nasional\data\merged_data_kaluta_ps.csv',
        r'E:\amanca_nasional\data\merged_data_bali_ps.csv',
        r'E:\amanca_nasional\data\merged_data_jogyakarta_ps.csv'
        
    ]
    json_files = [
        r'E:\amanca_nasional\data\outputbanten_ps.json',
        r'E:\amanca_nasional\data\outputjakarta_ps.json',
        r'E:\amanca_nasional\data\outputjawabarat_ps.json',
        r'E:\amanca_nasional\data\outputsumsel_ps.json',
        r'E:\amanca_nasional\data\outputsumut_ps.json',
        r'E:\amanca_nasional\data\outputsulsel_ps.json',
        r'E:\amanca_nasional\data\outputsultenggara_ps.json',
        r'E:\amanca_nasional\data\outputsulut_ps.json',
        r'E:\amanca_nasional\data\outputsumbar_ps.json',
        r'E:\amanca_nasional\data\outputpapuabarat_ps.json',
        r'E:\amanca_nasional\data\outputriau_ps.json',
        r'E:\amanca_nasional\data\outputsulbar_ps.json',
        r'E:\amanca_nasional\data\outputmaluku_ps.json',
        r'E:\amanca_nasional\data\outputmalut_ps.json',
        r'E:\amanca_nasional\data\outputntb_ps.json',
        r'E:\amanca_nasional\data\outputntt_ps.json',
        r'E:\amanca_nasional\data\outputpapua_ps.json',
        r'E:\amanca_nasional\data\outputkaltim_ps.json',
        r'E:\amanca_nasional\data\outputkepriau_ps.json',
        r'E:\amanca_nasional\data\outputlampung_ps.json',
        r'E:\amanca_nasional\data\outputjawatengah_ps.json',
        r'E:\amanca_nasional\data\outputjawatimur_ps.json',
        r'E:\amanca_nasional\data\outputkalbar_ps.json',
        r'E:\amanca_nasional\data\outputkalsel_ps.json',
        r'E:\amanca_nasional\data\outputkalteng_ps.json',
        r'E:\amanca_nasional\data\outputbabel_ps.json',
        r'E:\amanca_nasional\data\outputbali_ps.json',
        r'E:\amanca_nasional\data\outputbengkulu_ps.json',
        r'E:\amanca_nasional\data\outputgorontalo_ps.json',
        r'E:\amanca_nasional\data\outputjambi_ps.json',
        r'E:\amanca_nasional\data\outputsulteng_ps.json',
        r'E:\amanca_nasional\data\outputaceh_ps.json',
        r'E:\amanca_nasional\data\outputkaluta_ps.json',
        r'E:\amanca_nasional\data\outputbali_ps.json',
         r'E:\amanca_nasional\data\outputjogyakarta_ps.json'
        
    ]

    for i in range(len(weather_files)):
        merge_location_data(weather_files[i], location_file, output_files[i])
        csv_to_json(output_files[i], json_files[i])

    print("Alhamdulillah Sudah Jadi File JSON PRESENT WEATHER")

if __name__ == "__main__":
    main()

