
from datetime import datetime
import os
import inspect
from multiprocessing import Pool
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
                output_line = ','.join(_line.strip().split()) + '\n'
                output_buffer.append(output_line)
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
        {"C:\\usr\\20231025\\vector-converted\\nk1-4\\NK1-402.dat": ("C:\\usr\\20231025\\vector-prep\\nk1-4\\NK1-402-403.csv", "w"),
        "C:\\usr\\20231025\\vector-converted\\nk2-4\\NK2-403.dat": ("C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-403-404.csv", "w")},
        {"C:\\usr\\20231025\\vector-converted\\nk1-4\\NK1-403.dat": ("C:\\usr\\20231025\\vector-prep\\nk1-4\\NK1-402-403.csv", "a"),
        "C:\\usr\\20231025\\vector-converted\\nk2-4\\NK2-404.dat": ("C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-403-404.csv", "a")}
        ]
    for _proc_step in proc_steps:
        process_pool(_proc_step)

