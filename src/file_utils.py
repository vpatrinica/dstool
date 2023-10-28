import os
import shutil

def prepend_prefix_to_path(input_path, prefix):
    # Split the path into directory and filename
    directory, filename = os.path.split(input_path)
    
    # Prepend the prefix to the directory and filename
    new_directory = directory
    new_filename = prefix + filename
    
    # Combine the new directory and filename to create the new path
    new_path = os.path.join(new_directory, new_filename)
    
    return new_path


def prepare_output_file(_file, _append_mode):
    if (not(_append_mode) and os.path.exists(_file)):
        os.remove(_file)


def make_dirs(_file, _append):
    _file_dir = os.path.dirname(_file)
    if (not(_append) and os.path.exists(_file_dir)):
        shutil.rmtree(_file_dir)
    if not os.path.exists(_file_dir):
        os.makedirs(_file_dir)