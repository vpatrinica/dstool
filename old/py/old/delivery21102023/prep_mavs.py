
from datetime import datetime
import os
import inspect
# default nr of lines for batch I/O
C_I_DEFAULT_CHUNK_SIZE = 1000000

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
 
 
def prep_file(input_file_path, output_file_path, chunk_size=C_I_DEFAULT_CHUNK_SIZE):
    # profiling
    _current_function_name = inspect.currentframe().f_code.co_name
    _start_time = timer_on(_current_function_name)   
    # read/write in equal chunks
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        while 1:
            _chunk_lines = input_file.readlines(chunk_size)
            output_buffer = []
            for _line in _chunk_lines:
                try:
                    row = _line.strip().split()
                    _time_string = row[0] + '-' + row[1] + '-' + row[2] + ' ' + row[3] + ':' + row[4] + ':' + row[5]
                    # print(_time_string)
                    parsed_datetime = datetime.strptime(_time_string, "%m-%d-%y %H:%M:%S.%f")
                    # print(parsed_datetime)
                    row[:6] = [parsed_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')]
                except ValueError:
                    print('fail')
                    print(row)
                    pass
                # 09-21-23 09:47:15.86
                output_line = ','.join(row) + '\n'
                output_buffer.append(output_line)
                # print(output_buffer)
                # time.sleep(1000)
            if len(_chunk_lines) > 0:
                output_file.write(''.join(output_buffer))
            else:
                break
    # profiling
    _end_time = timer_off(_current_function_name) 
    print(_end_time - _start_time)

# files_to_convert = ["SN10337-1-2.csv", "SN10395-1-3.csv"]
# file_output_names = ["output4/SN10337-1-2/m_d1_SN10337-1-2.csv", "output4/SN10395-1-3/m_d1_SN10395-1-3.csv"]
files_to_convert = ["DATA0001_work.csv"]
file_output_names = ["output7/DATA0001_work.csv/m_DATA0001_work.csv"]

_out_index = 0
for _file in files_to_convert:
    print(_file)
    _o_file = file_output_names[_out_index]
    output_dir = os.path.dirname(_o_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Recreate parent directories if they don't exist
    if os.path.exists(_o_file):
        os.remove(_o_file)
    prep_file(_file, file_output_names[_out_index])
    _out_index=+1