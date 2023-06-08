#!/home/nivag/.env/bin/python

import os
import shutil

source_folders = ['/home/nivag/8007-MDT', '/home/nivag/8008-MDT']
target_folder = '/home/nivag/Compile-ZIP-MDT'

# Create the target folder if it doesn't exist
os.makedirs(target_folder, exist_ok=True)

# Traverse the source folders and their subfolders
for source_folder in source_folders:
    for root, dirs, files in os.walk(source_folder):
        for file_name in files:
            # Get the full path of the source file
            source_path = os.path.join(root, file_name)

            # Get the relative path of the source file
            relative_path = os.path.relpath(source_path, source_folder)

            # Generate the new filename by combining the original filename and relative path
            new_file_name = file_name + '_' + relative_path.replace('/', '_')

            new_file_name = new_file_name.replace('mdt_result.zip_var_opt_common5_mdt_', source_folder.split('/')[-1] + '_')

            # Create the target file path by joining the target folder and new filename
            target_path = os.path.join(target_folder, new_file_name)

            # Copy the file to the target folder with the new filename
            shutil.copy(source_path, target_path)
