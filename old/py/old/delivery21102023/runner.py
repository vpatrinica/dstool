import file_utils as fu
import argparse
import json
from multiprocessing import Pool

def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

def process_file(args):
    input_file, output_file, skip_lines, datetime_format, input_delimiter, output_delimiter, profile = args
    fu.prep_file(input_file, output_file, profile=profile, skip_lines=skip_lines,
                 datetime_format=datetime_format, input_delimiter=input_delimiter, output_delimiter=output_delimiter)

def main():
    # Create a command-line argument parser
    parser = argparse.ArgumentParser(description='Process files in parallel using configuration from a JSON file.')
    parser.add_argument('--config', default='config.json', help='Path to the configuration JSON file')

    args = parser.parse_args()
    config = load_config(args.config)

    # Create a pool of worker processes
    with Pool() as pool:
        # Prepare arguments for each prep_file call
        args_list = []
        for input_file, output_file, skip_lines, datetime_format, input_delimiter, output_delimiter, profile in zip(
            config['input_file_list'], config['output_file_list'], config['skip_lines_list'],
            config['datetime_format_list'], config['input_delimiter_list'], config['output_delimiter_list'], config['profile_list']
        ):
            args_list.append((input_file, output_file, skip_lines, datetime_format, input_delimiter, output_delimiter, profile))

        # Run prep_file in parallel
        pool.map(process_file, args_list)

if __name__ == "__main__":
    main()
