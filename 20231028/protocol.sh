
# Prepare

py prep_mavs.py
py prep_vector.py


# Process

streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk1-4\\NK1-402.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk1-4\\NK1-402-403.csv" --heading=13.4 --seed_time="21.09.2023 14:25:20.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" --step=125000 --seconds_to_spread=1 
streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk1-4\\NK1-403.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk1-4\\NK1-402-403.csv" --heading=12.8 --seed_time="01.10.2023 11:29:15.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" --step=125000 --append --seconds_to_spread=1  

streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-403.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk2-4\\NK2-403-404.csv" --heading=36.7 --seed_time="22.09.2023 10:26:08.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" --step=125000 --seconds_to_spread=1 
streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-404.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk2-4\\NK2-403-404.csv" --heading=35.6 --seed_time="01.10.2023 10:05:17.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" --step=125000 --append --seconds_to_spread=1 


streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-2\\DATA0001-part1.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-2\\DATA0001.csv" --seconds_to_spread=1

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-2\\DATA0001-part2.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-2\\DATA0001.csv" --append --seconds_to_spread=1

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-3\\DATA0001.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-3\\DATA0001.csv" --seconds_to_spread=1

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk2-2\\DATA0001.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk2-2\\DATA0001.csv" --seconds_to_spread=1

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk2-3\\DATA0001.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk2-3\\DATA0001.csv" --seconds_to_spread=1


Vector Data:
Nk1-2
402 
Date and Time              Ecode Scode Level      Description
21.09.2023 14:25:20.000    00    e2    Info       First measurement
01.10.2023 11:25:42.000    00    e2    Info       Last measurement

pdate	Vx	Vy	Vz	P_mBar	heading
datetime[μs]	f32	f32	f32	f32	f64
2023-09-21 14:25:20	0.3831	0.3254	-0.0983	994.299988	13.4
2023-09-21 14:25:20.125	-0.2944	0.306	-0.1333	994.200012	13.4
...
2023-10-01 11:25:36.500	0.0954	0.0101	0.029	1017.200012	13.4
2023-10-01 11:25:36.625	-0.4882	0.2594	-0.0007	1017.099976	13.4
Last 10 rows

403
Date and Time              Ecode Scode Level      Description
01.10.2023 11:29:15.000    00    e0    Info       First measurement
25.10.2023 16:05:17.000    00    e0    Info       Last measurement

pdate	Vx	Vy	Vz	P_mBar	heading
datetime[μs]	f32	f32	f32	f32	f64
2023-10-01 11:29:15	3.067	-1.569	-0.482	1017.200012	12.8
2023-10-01 11:29:15.125	-3.901	-2.865	-0.038	1017.099976	12.8
...
2023-10-25 16:04:58.500	0.938	-4.932	0.268	993.799988	12.8
2023-10-25 16:04:58.625	0.386	-1.824	0.132	993.700012	12.8
Last 10 rows


Nk2-4
403
Date and Time              Ecode Scode Level      Description
22.09.2023 10:26:08.000    00    e2    Info       First measurement
01.10.2023 09:59:45.000    00    e2    Info       Last measurement

pdate	Vx	Vy	Vz	P_mBar	heading
datetime[μs]	f32	f32	f32	f32	f64
2023-09-22 10:26:08	-0.2089	-0.0101	0.0084	996.799988	36.7
2023-09-22 10:26:08.125	0.0928	-0.4091	-0.0409	996.799988	36.7
...

2023-10-01 09:59:40.125	0.0112	-0.0092	-0.0052	1018.700012	36.7
2023-10-01 09:59:40.250	0.0147	-0.0407	-0.0062	1018.700012	36.7
Last 10 rows


404
Date and Time              Ecode Scode Level      Description
01.10.2023 10:05:17.000    00    e0    Info       First measurement
25.10.2023 15:03:16.000    00    e0    Info       Last measurement

pdate	Vx	Vy	Vz	P_mBar	heading
datetime[μs]	f32	f32	f32	f32	f64
2023-10-01 10:05:17	0.11	-0.179	-0.02	1018.700012	35.6
2023-10-01 10:05:17.125	-0.075	0.149	0.019	1018.700012	35.6
...
2023-10-25 15:03:11.250	-0.314	1.756	0.449	995.099976	35.6
2023-10-25 15:03:11.375	-0.062	1.929	0.488	995.099976	35.6
Last 10 rows


Timestamps
DATA0001-part1.cs
more DATA0001-part1.cs P 1
more DATA0001-part2.csv P 1
2023-10-18 10:56:34.080000,-183.7,-183.7,-183.7,-183.7,999.0,999.0,999.0,2.13,-0.54,0.84,-0.4,2.2
2023-10-08 16:02:58.020000
 - nk1-2(10337) - (15:27:00.00) - 

9896353 - nk1-3(10395) (13:05:00.00) - 10 25 23 13 05 08.33 -183.7  -183.7  -183.7  -183.7  999.0  999.0  999.0    0.73 -0.32  0.95  -0.1   0.9

20456900 - nk2-2(10396) -(14:23:30.00)- 10 25 23 14 23 31.03 -183.7  -183.7  -183.7  -183.7  999.0  999.0  999.0    0.01 -0.45  0.90  -0.1   1.4

12853512 - nk2-3(10410)-(13:54:00.50) -10 25 23 13 54 02.10 -183.7  -183.7  -183.7  -183.7  999.0  999.0  999.0  15.62    0.55 -0.37  0.93   1.5   0.1











#check head

py runner.py --config=config_mavs_csv_gz.json
python runner.py --config=config_mavs_csv_raw.json
python runner.py --config=config_mavs_csv_raw_plain.json

python runner.py --config=config_mavs_csv_raw_plain2.json

streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-403-404.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk2-4\\NK2-403-404.csv" --heading=0.69 --seed_time="2023-09-21 09:47:15.69"

streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-403-404.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk2-4\\NK2-403-404.csv" --heading=0.69 --seed_time="2023-09-21 09:47:15.690000" --step=125000







streamlit run rust.py -- --proc_filename=output/SN10337-1-2/m_d1_SN10337-1-2.csv --out_filename=output_proc/SN10337-1-2/m_d1_SN10337-1-2.csv --seed_time="23-09-21 09:47:15"


streamlit run rust.py -- --proc_filename=output/SN10395-1-3/m_d1_SN10395-1-3.csv --out_filename=output_proc/SN10395-1-3/m_d1_SN10395-1-3.csv --seed_time="23-09-21 10:28:49"



"output7/DATA0001_work.csv/m_DATA0001_work.csv"


streamlit run rust.py -- --proc_filename=output4/SN10337-1-2/m_d1_SN10337-1-2.csv --out_filename=output_proc4/SN10337-1-2/m_d1_SN10337-1-2.csv --seed_time="23-09-21 09:47:15"


streamlit run rust.py -- --proc_filename=output4/SN10395-1-3/m_d1_SN10395-1-3.csv --out_filename=output_proc4/SN10395-1-3/m_d1_SN10395-1-3.csv --seed_time="23-09-21 10:28:49"



streamlit run rsfreq.py -- --proc_filename=output_proc4/SN10337-1-2/m_d1_SN10337-1-2.csv --out_filename=output_proc6/SN10337-1-2/m_d1_SN10337-1-2.csv 

streamlit run rsfreq2.py -- --proc_filename=output_proc4/SN10395-1-3/m_d1_SN10395-1-3.csv --out_filename=output_proc6/SN10395-1-3/m_d1_SN10395-1-3.csv 

streamlit run rsfreq3.py -- --proc_filename="output7/DATA0001_work.csv/m_DATA0001_work.csv" --out_filename="output_proc7/DATA0001_work.csv/m_DATA0001_work.csv" 

streamlit run rs.py -- --proc_filename=output_proc4/SN10337-1-2/m_d1_SN10337-1-2.csv --out_filename=output_proc5/SN10337-1-2/m_d1_SN10337-1-2.csv 


streamlit run rs.py -- --proc_filename=output_proc4/SN10395-1-3/m_d1_SN10395-1-3.csv --out_filename=output_proc5/SN10395-1-3/m_d1_SN10395-1-3.csv 