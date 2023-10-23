from hardware import Radio_Sector
import glob
import os, sys

def get_executable_folder():
    """Get the folder where the executable is located."""
    if getattr(sys, 'frozen', False):
        # If running in a bundle created by PyInstaller or similar
        return os.path.dirname(sys.executable)
    else:
        # If running in the development environment
        return os.path.dirname(os.path.abspath(__file__))

def find_xlsb_file():
    """Find the .xlsb file in the same folder as the executable."""
    executable_folder = get_executable_folder()
    for file_name in os.listdir(executable_folder):
        if file_name.lower().endswith('.xlsb'):
            return os.path.join(executable_folder, file_name)
    return None

if __name__ == '__main__':

    xlsb_file_path = find_xlsb_file()
    radiosector = Radio_Sector(xlsb_file_path).newmethod1_radiosector()