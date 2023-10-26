py prep_mavs.py
py prep_vector.py



py runner.py --config=config_mavs_csv_gz.json
python runner.py --config=config_mavs_csv_raw.json
python runner.py --config=config_mavs_csv_raw_plain.json

python runner.py --config=config_mavs_csv_raw_plain2.json



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