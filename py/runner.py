# runner.py

import file_utils as fu
import json

def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

def main():
    config = load_config('config.json')
    
    for input_file, output_file, skip_lines, datetime_format, input_delimiter, output_delimiter in zip(
        config['input_file_list'], config['output_file_list'], config['skip_lines_list'],
        config['datetime_format_list'], config['input_delimiter_list'], config['output_delimiter_list']
    ):
        fu.prep_file(input_file, output_file, profile=config['profile'], skip_lines=skip_lines,
                     datetime_format=datetime_format, input_delimiter=input_delimiter, output_delimiter=output_delimiter)

if __name__ == "__main__":
    main()
