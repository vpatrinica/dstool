py runner.py --config=config_mavs_csv_gz.json
python runner.py --config=config_mavs_csv_raw.json
streamlit run rust.py -- --proc_filename=output/SN10337-1-2/m_d1_SN10337-1-2.csv --out_filename=output_proc/SN10337-1-2/m_d1_SN10337-1-2.csv --seed_time="23-09-21 09:47:15"


streamlit run rust.py -- --proc_filename=output/SN10395-1-3/m_d1_SN10395-1-3.csv --out_filename=output_proc/SN10395-1-3/m_d1_SN10395-1-3.csv --seed_time="23-09-21 10:28:49"