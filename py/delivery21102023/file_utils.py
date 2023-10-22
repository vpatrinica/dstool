from datetime import datetime
import inspect
import zipfile
import gzip
import os
import io
from concurrent.futures import ThreadPoolExecutor

C_I_DEFAULT_CHUNK_SIZE_BYTES = 2000000000
C_I_DEFAULT_CHUNK_SIZE_LINES = 10000000
C_empty_str = ""

def timer_on(f_name):
    print('Starting Timer... ' + f_name)
    _start = datetime.now()
    print(_start)
    return _start


def timer_off(f_name):
    print('Stopping Timer... ' + f_name)
    _stop = datetime.now()
    print(_stop)
    return _stop


def process_file_chunk(chunk, carry_over, output_file, datetime_format, input_delimiter, output_delimiter):
    
    _carry_over = carry_over

    output_buffer = []

    if isinstance(chunk, bytes):
        chunk = chunk.decode('utf-8')  # Decode bytes to string for gzipped files

    lines = (_carry_over + chunk).split('\n')
    if len(lines) == 0:
        _carry_over = C_empty_str
    elif (len(lines) == 1):
        _carry_over = C_empty_str
        lines = lines + lines[0]
    else:
        _carry_over = lines[-1]

    for _line in lines[:-1]:
        if input_delimiter == " ":
            row = _line.strip().split()
        else:
            row = _line.strip().split(input_delimiter)

        if datetime_format:
            try:
                parsed_datetime = datetime.strptime(
                    row[0] + '-' + row[1] + '-' + row[2] + ' ' + row[3] + ':' + row[4] + ':' + row[5], datetime_format)
                row[:6] = [parsed_datetime.strftime('%Y-%m-%d %H:%M:%S.%3f')]
            except ValueError:
                pass

        output_line = output_delimiter.join(row) + '\n'
        output_buffer.append(output_line)

    if output_file.endswith('.gz'):
        return (''.join(output_buffer).encode('utf-8'), _carry_over)
    else:
        return (''.join(output_buffer), _carry_over)


def prep_file(input_file_path, output_file_path, chunk_size=C_I_DEFAULT_CHUNK_SIZE_LINES, profile=True, skip_lines=0, datetime_format='', input_delimiter=',', output_delimiter='  '):
    _current_function_name = inspect.currentframe().f_code.co_name
    if profile:
        _start_time = timer_on(_current_function_name)

    carry_over = C_empty_str
    output_dir = os.path.dirname(output_file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Recreate parent directories if they don't exist
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    if output_file_path.endswith('.gz'):
        _w_flags = 'ab'
    else:
        _w_flags = 'a'

    with ThreadPoolExecutor(max_workers=8) as executor:
        if input_file_path.endswith('.zip'):
            chunk_size = C_I_DEFAULT_CHUNK_SIZE_BYTES
            with zipfile.ZipFile(input_file_path, 'r') as zip_file:
                with zip_file.open(zip_file.namelist()[0]) as input_file:
                    while True:
                        chunk = input_file.read(chunk_size)
                        if ((not chunk) and (carry_over == C_empty_str)):
                            break
                        (output_chunk, carry_over) = executor.submit(process_file_chunk, chunk, carry_over, output_file_path, datetime_format, input_delimiter, output_delimiter).result()
                        with open(output_file_path, _w_flags) as output_file:
                            output_file.write(output_chunk)
        elif input_file_path.endswith('.gz'):
            chunk_size = C_I_DEFAULT_CHUNK_SIZE_BYTES
            with gzip.open(input_file_path, 'rb') as input_file:
                while True:
                    chunk = input_file.read(chunk_size)
                    if ((not chunk) and (carry_over == C_empty_str)):
                            break
                    (output_chunk, carry_over) = executor.submit(process_file_chunk, chunk, carry_over, output_file_path, datetime_format, input_delimiter, output_delimiter).result()
                    with open(output_file_path, _w_flags) as output_file:
                        output_file.write(output_chunk)
        else:
            with open(input_file_path, 'r') as input_file:
                while True:
                    chunk = input_file.read(chunk_size)
                    if ((not chunk) and (carry_over == C_empty_str)):
                            break
                    (output_chunk, carry_over) = executor.submit(process_file_chunk, chunk, carry_over, output_file_path, datetime_format, input_delimiter, output_delimiter).result()
                    with open(output_file_path, _w_flags) as output_file:
                        output_file.write(output_chunk)

    if profile:
        _end_time = timer_off(_current_function_name)
        print(_end_time - _start_time)
