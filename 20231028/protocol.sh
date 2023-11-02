
# update
cd c:\prj\ds-tool\
(optional) git stash
git pull origin master


cd ./src
# Prepare

py prep_mavs.py
py prep_vector.py

# open venv Python terminal in anaconda
cd c:\prj\ds-tool\src
# Process

# --seconds_to_offset

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231101\\mavs-prep\\nk1-1\\DATA0001-part0.csv" --out_filename="C:\\usr\\20231101\\mavs-proc\\nk1-1\\_DATA0001.csv" --seconds_to_spread=0

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231101\\mavs-prep\\nk1-1\\DATA0001-partX.csv" --out_filename="C:\\usr\\20231101\\mavs-proc\\nk1-1\\_DATA0001.csv" --append --seconds_to_spread=0 --seconds_to_offset=1697828090.42 



streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk1-4\\NK1-402.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk1-4\\_NK1-402-403.csv" --heading=13.4 --seed_time="21.09.2023 14:25:20.000" --end_time="01.10.2023 11:25:42.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" 
streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk1-4\\NK1-403.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk1-4\\_NK1-402-403.csv" --heading=12.8 --seed_time="01.10.2023 11:29:15.000" --end_time="25.10.2023 16:05:17.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" --append 

streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-403.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk2-4\\_NK2-403-404.csv" --heading=36.7 --seed_time="22.09.2023 10:26:08.000" --end_time="01.10.2023 09:59:45.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" 
streamlit run rsvector.py -- --proc_filename="C:\\usr\\20231025\\vector-prep\\nk2-4\\NK2-404.csv" --out_filename="C:\\usr\\20231025\\vector-proc\\nk2-4\\_NK2-403-404.csv" --heading=35.6 --seed_time="01.10.2023 10:05:17.000" --end_time="25.10.2023 15:03:16.000" --time_fmt="%d.%m.%Y %H:%M:%S.%f" --append


streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-2\\DATA0001-part0.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-2\\_DATA0001.csv" --seconds_to_spread=-20.7

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-2\\DATA0001-part1.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-2\\_DATA0001.csv" --append --seconds_to_spread=-12

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-2\\DATA0001-part2.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-2\\_DATA0001.csv" --append --seconds_to_spread=-8.5


streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-3\\DATA0001-part0.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-3\\_DATA0001.csv"  --seconds_to_spread=-19

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-3\\DATA0001-part1.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-3\\_DATA0001.csv" --append --seconds_to_spread=-11

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk1-3\\DATA0001-part2.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk1-3\\_DATA0001.csv" --append --seconds_to_spread=-8.33


streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk2-2\\DATA0001-part1.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk2-2\\_DATA0001.csv" 

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk2-2\\DATA0001-part2.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk2-2\\_DATA0001.csv" --append 


streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk2-3\\DATA0001-part1.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk2-3\\_DATA0001.csv" --mavs_type=2

streamlit run rsmavs.py -- --proc_filename="C:\\usr\\20231025\\mavs-prep\\nk2-3\\DATA0001-part2.csv" --out_filename="C:\\usr\\20231025\\mavs-proc\\nk2-3\\_DATA0001.csv" --mavs_type=2 --append 


# commit changes
cd c:\prj\ds-tool
git add --all
git commit -m "ASS-102 comment"

git push origin master


Vector Data:
Nk1-2
402 
Date and Time              Ecode Scode Level      Description
21.09.2023 14:25:20.000    00    e2    Info       First measurement
01.10.2023 11:25:42.000    00    e2    Info       Last measurement

125000769.1480506
shape: (2, 8)
┌──────────┬─────────┬────────┬─────────┬────────────┬─────────┬─────────────────────────┬───────┐
│ Ensemble ┆ Vx      ┆ Vy     ┆ Vz      ┆ P_mBar     ┆ heading ┆ pdate                   ┆ steps │
│ ---      ┆ ---     ┆ ---    ┆ ---     ┆ ---        ┆ ---     ┆ ---                     ┆ ---   │
│ i64      ┆ f32     ┆ f32    ┆ f32     ┆ f32        ┆ f64     ┆ datetime[μs]            ┆ i64   │
╞══════════╪═════════╪════════╪═════════╪════════════╪═════════╪═════════════════════════╪═══════╡
│ 1        ┆ 0.3831  ┆ 0.3254 ┆ -0.0983 ┆ 994.299988 ┆ 13.4    ┆ 2023-09-21 14:25:20     ┆ 0     │
│ 2        ┆ -0.2944 ┆ 0.306  ┆ -0.1333 ┆ 994.200012 ┆ 13.4    ┆ 2023-09-21 14:25:20.125 ┆ 1     │
└──────────┴─────────┴────────┴─────────┴────────────┴─────────┴─────────────────────────┴───────┘
shape: (2, 8)
┌──────────┬─────────┬────────┬─────────┬─────────────┬─────────┬────────────────────────────┬─────────┐
│ Ensemble ┆ Vx      ┆ Vy     ┆ Vz      ┆ P_mBar      ┆ heading ┆ pdate                      ┆ steps   │
│ ---      ┆ ---     ┆ ---    ┆ ---     ┆ ---         ┆ ---     ┆ ---                        ┆ ---     │
│ i64      ┆ f32     ┆ f32    ┆ f32     ┆ f32         ┆ f64     ┆ datetime[μs]               ┆ i64     │
╞══════════╪═════════╪════════╪═════════╪═════════════╪═════════╪════════════════════════════╪═════════╡
│ 3        ┆ 0.0954  ┆ 0.0101 ┆ 0.029   ┆ 1017.200012 ┆ 13.4    ┆ 2023-10-01 11:25:41.749998 ┆ 6825732 │
│ 4        ┆ -0.4882 ┆ 0.2594 ┆ -0.0007 ┆ 1017.099976 ┆ 13.4    ┆ 2023-10-01 11:25:41.874999 ┆ 6825733 │
└──────────┴─────────┴────────┴─────────┴─────────────┴─────────┴────────────────────────────┴─────────┘
6825734


403
Date and Time              Ecode Scode Level      Description
01.10.2023 11:29:15.000    00    e0    Info       First measurement
25.10.2023 16:05:17.000    00    e0    Info       Last measurement
125001091.43210845
shape: (2, 8)
┌──────────┬────────┬────────┬────────┬─────────────┬─────────┬────────────────────────────┬───────┐
│ Ensemble ┆ Vx     ┆ Vy     ┆ Vz     ┆ P_mBar      ┆ heading ┆ pdate                      ┆ steps │
│ ---      ┆ ---    ┆ ---    ┆ ---    ┆ ---         ┆ ---     ┆ ---                        ┆ ---   │
│ i64      ┆ f32    ┆ f32    ┆ f32    ┆ f32         ┆ f64     ┆ datetime[μs]               ┆ i64   │
╞══════════╪════════╪════════╪════════╪═════════════╪═════════╪════════════════════════════╪═══════╡
│ 1        ┆ 3.067  ┆ -1.569 ┆ -0.482 ┆ 1017.200012 ┆ 12.8    ┆ 2023-10-01 11:29:15        ┆ 0     │
│ 2        ┆ -3.901 ┆ -2.865 ┆ -0.038 ┆ 1017.099976 ┆ 12.8    ┆ 2023-10-01 11:29:15.125001 ┆ 1     │
└──────────┴────────┴────────┴────────┴─────────────┴─────────┴────────────────────────────┴───────┘
shape: (2, 8)
┌──────────┬───────┬────────┬───────┬────────────┬─────────┬────────────────────────────┬──────────┐
│ Ensemble ┆ Vx    ┆ Vy     ┆ Vz    ┆ P_mBar     ┆ heading ┆ pdate                      ┆ steps    │
│ ---      ┆ ---   ┆ ---    ┆ ---   ┆ ---        ┆ ---     ┆ ---                        ┆ ---      │
│ i64      ┆ f32   ┆ f32    ┆ f32   ┆ f32        ┆ f64     ┆ datetime[μs]               ┆ i64      │
╞══════════╪═══════╪════════╪═══════╪════════════╪═════════╪════════════════════════════╪══════════╡
│ 15       ┆ 0.938 ┆ -4.932 ┆ 0.268 ┆ 993.799988 ┆ 12.8    ┆ 2023-10-25 16:05:16.749997 ┆ 16721148 │
│ 16       ┆ 0.386 ┆ -1.824 ┆ 0.132 ┆ 993.700012 ┆ 12.8    ┆ 2023-10-25 16:05:16.874998 ┆ 16721149 │
└──────────┴───────┴────────┴───────┴────────────┴─────────┴────────────────────────────┴──────────┘
16721150
Nk2-4
403
Date and Time              Ecode Scode Level      Description
22.09.2023 10:26:08.000    00    e2    Info       First measurement
01.10.2023 09:59:45.000    00    e2    Info       Last measurement


125000744.99456273
shape: (2, 8)
┌──────────┬─────────┬─────────┬─────────┬────────────┬─────────┬─────────────────────────┬───────┐
│ Ensemble ┆ Vx      ┆ Vy      ┆ Vz      ┆ P_mBar     ┆ heading ┆ pdate                   ┆ steps │
│ ---      ┆ ---     ┆ ---     ┆ ---     ┆ ---        ┆ ---     ┆ ---                     ┆ ---   │
│ i64      ┆ f32     ┆ f32     ┆ f32     ┆ f32        ┆ f64     ┆ datetime[μs]            ┆ i64   │
╞══════════╪═════════╪═════════╪═════════╪════════════╪═════════╪═════════════════════════╪═══════╡
│ 1        ┆ -0.2089 ┆ -0.0101 ┆ 0.0084  ┆ 996.799988 ┆ 36.7    ┆ 2023-09-22 10:26:08     ┆ 0     │
│ 2        ┆ 0.0928  ┆ -0.4091 ┆ -0.0409 ┆ 996.799988 ┆ 36.7    ┆ 2023-09-22 10:26:08.125 ┆ 1     │
└──────────┴─────────┴─────────┴─────────┴────────────┴─────────┴─────────────────────────┴───────┘
shape: (2, 8)
┌──────────┬────────┬─────────┬─────────┬─────────────┬─────────┬────────────────────────────┬─────────┐
│ Ensemble ┆ Vx     ┆ Vy      ┆ Vz      ┆ P_mBar      ┆ heading ┆ pdate                      ┆ steps   │
│ ---      ┆ ---    ┆ ---     ┆ ---     ┆ ---         ┆ ---     ┆ ---                        ┆ ---     │
│ i64      ┆ f32    ┆ f32     ┆ f32     ┆ f32         ┆ f64     ┆ datetime[μs]               ┆ i64     │
╞══════════╪════════╪═════════╪═════════╪═════════════╪═════════╪════════════════════════════╪═════════╡
│ 6        ┆ 0.0112 ┆ -0.0092 ┆ -0.0052 ┆ 1018.700012 ┆ 36.7    ┆ 2023-10-01 09:59:44.749998 ┆ 6208097 │
│ 7        ┆ 0.0147 ┆ -0.0407 ┆ -0.0062 ┆ 1018.700012 ┆ 36.7    ┆ 2023-10-01 09:59:44.874999 ┆ 6208098 │
└──────────┴────────┴─────────┴─────────┴─────────────┴─────────┴────────────────────────────┴─────────┘
6208099

404
Date and Time              Ecode Scode Level      Description
01.10.2023 10:05:17.000    00    e0    Info       First measurement
25.10.2023 15:03:16.000    00    e0    Info       Last measurement

125000268.94901182
shape: (2, 8)
┌──────────┬────────┬────────┬───────┬─────────────┬─────────┬─────────────────────────┬───────┐
│ Ensemble ┆ Vx     ┆ Vy     ┆ Vz    ┆ P_mBar      ┆ heading ┆ pdate                   ┆ steps │
│ ---      ┆ ---    ┆ ---    ┆ ---   ┆ ---         ┆ ---     ┆ ---                     ┆ ---   │
│ i64      ┆ f32    ┆ f32    ┆ f32   ┆ f32         ┆ f64     ┆ datetime[μs]            ┆ i64   │
╞══════════╪════════╪════════╪═══════╪═════════════╪═════════╪═════════════════════════╪═══════╡
│ 1        ┆ 0.11   ┆ -0.179 ┆ -0.02 ┆ 1018.700012 ┆ 35.6    ┆ 2023-10-01 10:05:17     ┆ 0     │
│ 2        ┆ -0.075 ┆ 0.149  ┆ 0.019 ┆ 1018.700012 ┆ 35.6    ┆ 2023-10-01 10:05:17.125 ┆ 1     │
└──────────┴────────┴────────┴───────┴─────────────┴─────────┴─────────────────────────┴───────┘
shape: (2, 8)
┌──────────┬────────┬───────┬───────┬────────────┬─────────┬────────────────────────────┬──────────┐
│ Ensemble ┆ Vx     ┆ Vy    ┆ Vz    ┆ P_mBar     ┆ heading ┆ pdate                      ┆ steps    │
│ ---      ┆ ---    ┆ ---   ┆ ---   ┆ ---        ┆ ---     ┆ ---                        ┆ ---      │
│ i64      ┆ f32    ┆ f32   ┆ f32   ┆ f32        ┆ f64     ┆ datetime[μs]               ┆ i64      │
╞══════════╪════════╪═══════╪═══════╪════════════╪═════════╪════════════════════════════╪══════════╡
│ 6        ┆ -0.314 ┆ 1.756 ┆ 0.449 ┆ 995.099976 ┆ 35.6    ┆ 2023-10-25 15:03:15.749999 ┆ 16731794 │
│ 7        ┆ -0.062 ┆ 1.929 ┆ 0.488 ┆ 995.099976 ┆ 35.6    ┆ 2023-10-25 15:03:15.874999 ┆ 16731795 │
└──────────┴────────┴───────┴───────┴────────────┴─────────┴────────────────────────────┴──────────┘
16731796










Timestamps
DATA0001-part1.cs
more DATA0001-part1.cs P 1
more DATA0001-part2.csv P 1
2023-10-18 10:56:34.080000,-183.7,-183.7,-183.7,-183.7,999.0,999.0,999.0,2.13,-0.54,0.84,-0.4,2.2
2023-10-08 16:02:58.020000
 - nk1-2(10337) - (15:27:00.00) - 
ahead 8.33
9896353 - nk1-3(10395) (13:05:00.00) - 10 25 23 13 05 08.33 -183.7  -183.7  -183.7  -183.7  999.0  999.0  999.0    0.73 -0.32  0.95  -0.1   0.9

20456900 - nk2-2(10396) -(14:23:30.00)- 10 25 23 14 23 31.03 -183.7  -183.7  -183.7  -183.7  999.0  999.0  999.0    0.01 -0.45  0.90  -0.1   1.4

12853512 - nk2-3(10410)-(13:54:00.50) -10 25 23 13 54 02.10 -183.7  -183.7  -183.7  -183.7  999.0  999.0  999.0  15.62    0.55 -0.37  0.93   1.5   0.1

______
mavs seconds to spread 
______

nk1-2
-  10781256  -20,7
- 6160787  -12
- 4500000 -8,5

nk1-3
- 10125308 -19
- 5732263 -11
-611330seconds 4164090records -8 seconds

nk2-2
-
-
nk2-3
-
-
______




