#!/home/nivag/.env/bin/python

import os
import zipfile

for files in os.listdir('/home/nivag/Compile-ZIP-MDT-Polygon'):
    print(files)
    fullpath = os.path.join('/home/nivag/Compile-ZIP-MDT-Polygon', files)

    # Extract the zip file
    with zipfile.ZipFile(fullpath, 'r') as zip_ref:
        zip_ref.extractall('/home/nivag/Compile-MDT-Polygon/')

    # Get the name of the zip file without the extension
    zip_name = os.path.splitext(files)[0]
    zip_name = zip_name.replace('_mdt_result', '')

    # Rename the extracted file
    extracted_file = os.path.join('/home/nivag/Compile-MDT-Polygon/', "mdt_result.csv")
    new_file_name = os.path.join('/home/nivag/Compile-MDT-Polygon/', f"mdt_result_{zip_name}.csv")
    os.rename(extracted_file, new_file_name)

zip_files = [file for file in os.listdir('/home/nivag/Compile-ZIP-MDT-Polygon') if file.endswith('.zip')]

for file in zip_files:
    file_path = os.path.join('/home/nivag/Compile-ZIP-MDT-Polygon', file)
    os.remove(file_path)
