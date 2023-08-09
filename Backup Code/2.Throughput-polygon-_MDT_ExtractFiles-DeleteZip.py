#!/home/nivag/.env/bin/python

import os
import zipfile

for files in os.listdir('/home/nivag/Compile-ZIP-UETraffic-Polygon'):
    print(files)
    fullpath = os.path.join('/home/nivag/Compile-ZIP-UETraffic-Polygon', files)

    # Extract the zip file
    with zipfile.ZipFile(fullpath, 'r') as zip_ref:
        zip_ref.extractall('/home/nivag/Compile-UETraffic-Polygon')

    # Get the name of the zip file without the extension
    zip_name = os.path.splitext(files)[0]
    zip_name = zip_name.replace('_ue_traffic', '')

    # Rename the extracted file
    extracted_file = os.path.join('/home/nivag/Compile-UETraffic-Polygon/', "ue_traffic.csv")
    new_file_name = os.path.join('/home/nivag/Compile-UETraffic-Polygon/', f"ue_traffic_{zip_name}.csv")
    os.rename(extracted_file, new_file_name)

zip_files = [file for file in os.listdir('/home/nivag/Compile-ZIP-UETraffic-Polygon') if file.endswith('.zip')]

for file in zip_files:
    file_path = os.path.join('/home/nivag/Compile-ZIP-UETraffic-Polygon', file)
    os.remove(file_path)
