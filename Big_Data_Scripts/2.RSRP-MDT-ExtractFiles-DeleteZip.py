#!/home/nivag/.python-3.10/bin/python

import os
import zipfile

for files in os.listdir('/home/nivag/Compile-ZIP-MDT'):
    print(files)
    fullpath = os.path.join('/home/nivag/Compile-ZIP-MDT', files)

    # Extract the zip file
    with zipfile.ZipFile(fullpath, 'r') as zip_ref:
        zip_ref.extractall('/home/nivag/Compile-MDT/')

    # Get the name of the zip file without the extension
    zip_name = os.path.splitext(files)[0]
    zip_name = zip_name.replace('_mdt_result', '')

    # Rename the extracted file
    extracted_file = os.path.join('/home/nivag/Compile-MDT/', "mdt_result.csv")
    new_file_name = os.path.join('/home/nivag/Compile-MDT/', f"mdt_result_{zip_name}.csv")
    
    try:
        os.rename(extracted_file, new_file_name)
    except:
        pass

zip_files = [file for file in os.listdir('/home/nivag/Compile-ZIP-MDT') if file.endswith('.zip')]

for file in zip_files:
    file_path = os.path.join('/home/nivag/Compile-ZIP-MDT', file)
    os.remove(file_path)
