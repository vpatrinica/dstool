
from datetime import datetime
import os
import inspect
from multiprocessing import Pool
from time import sleep
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

 
def prepare_output_dir(_file, _file_flag):
    _file_dir = os.path.dirname(_file)
    if not os.path.exists(_file_dir):
        os.makedirs(_file_dir)  # Recreate parent directories if they don't exist
    if (('w' in _file_flag) and os.path.exists(_file)):
        os.remove(_file)


def parse_mavs_date(_rec_line):
    
    # print("!!!" + str(_rec_line))
    _row = _rec_line.strip().split()
    try:
        _time_string = _row[0] + '-' + _row[1] + '-' + _row[2] + ' ' + _row[3] + ':' + _row[4] + ':' + _row[5]
        # print(_time_string)
        parsed_datetime = datetime.strptime(_time_string, "%m-%d-%y %H:%M:%S.%f")
        # print(parsed_datetime)
        _row[:6] = [parsed_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')]
    except ValueError:
        print('fail')
        print(_row)
        # sleep(10000)
        pass
    finally:
        return ','.join(_row) + '\n'


def prep_file(input_file_path, output_file_path, output_file_flag, chunk_size=C_I_DEFAULT_CHUNK_SIZE):
    # profiling
    _current_function_name = inspect.currentframe().f_code.co_name
    _start_time = timer_on(_current_function_name)   
    # read/write in equal chunks
    with open(input_file_path, 'r') as input_file, open(output_file_path, output_file_flag) as output_file:
        while 1:
            _chunk_lines = input_file.readlines(chunk_size)
            output_buffer = []
            for _line in _chunk_lines:
                output_buffer.append(parse_mavs_date(_line))
            if len(output_buffer) > 0:
                output_file.write(''.join(output_buffer))
            else:
                break
    # profiling
    _end_time = timer_off(_current_function_name) 
    print(_end_time - _start_time)


def process_file(args):
    _i_file, (_o_file, _o_f_flags) = args
    print(f"{_i_file}: --> {_o_file}")
    prepare_output_dir(_o_file, _o_f_flags)
    prep_file(_i_file, _o_file, _o_f_flags)


def process_pool(_input_map):
    pool = Pool(processes=len(_input_map))
    pool.map(process_file, _input_map.items())
    pool.close()
    pool.join()
    

if __name__ == "__main__":
    proc_steps = [
        {"C:\\usr\\20231025\\mavs-converted\\nk1-2\\DATA0001.DAT": ("C:\\usr\\20231025\\mavs-prep\\nk1-2\\DATA0001.csv", "w"),
        "C:\\usr\\20231025\\mavs-converted\\nk1-3\\DATA0001.DAT": ("C:\\usr\\20231025\\mavs-prep\\nk1-3\\DATA0001.csv", "w"),
        "C:\\usr\\20231025\\mavs-converted\\nk2-2\\DATA0001.DAT": ("C:\\usr\\20231025\\mavs-prep\\nk2-2\\DATA0001.csv", "w"),
        "C:\\usr\\20231025\\mavs-converted\\nk2-3\\DATA0001.DAT": ("C:\\usr\\20231025\\mavs-prep\\nk2-3\\DATA0001.csv", "w")}
        ]
    for _proc_step in proc_steps:
        process_pool(_proc_step)
