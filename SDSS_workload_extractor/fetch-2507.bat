call ..\venv\Scripts\activate.bat
python main.py fetch --year 2025 --month 7 --day 1
python main.py fetch --year 2025 --month 7 --day 2
python main.py fetch --year 2025 --month 7 --day 3
python main.py fetch --year 2025 --month 7 --day 4
python main.py fetch --year 2025 --month 7 --day 5
python main.py fetch --year 2025 --month 7 --day 6
python main.py fetch --year 2025 --month 7 --day 7
python main.py fetch --year 2025 --month 7 --day 8
python main.py fetch --year 2025 --month 7 --day 9
python main.py fetch --year 2025 --month 7 --day 10
python main.py fetch --year 2025 --month 7 --day 11
python main.py fetch --year 2025 --month 7 --day 12
python main.py fetch --year 2025 --month 7 --day 13
python main.py fetch --year 2025 --month 7 --day 14
python main.py fetch --year 2025 --month 7 --day 15
python main.py fetch --year 2025 --month 7 --day 16
python main.py fetch --year 2025 --month 7 --day 17
python main.py fetch --year 2025 --month 7 --day 18
python main.py fetch --year 2025 --month 7 --day 19
python main.py fetch --year 2025 --month 7 --day 20
python main.py fetch --year 2025 --month 7 --day 21
python main.py fetch --year 2025 --month 7 --day 22
python main.py fetch --year 2025 --month 7 --day 23
python main.py fetch --year 2025 --month 7 --day 24
python main.py fetch --year 2025 --month 7 --day 25
python main.py fetch --year 2025 --month 7 --day 26
python main.py fetch --year 2025 --month 7 --day 27
python main.py fetch --year 2025 --month 7 --day 28
python main.py fetch --year 2025 --month 7 --day 29
python main.py fetch --year 2025 --month 7 --day 30
python main.py fetch --year 2025 --month 7 --day 31
copy /b data\fetched_2025_07_??_all.txt data\2025_07\fetched_2025_07.txt
python main.py parse --input 2025_07\fetched_2025_07.txt --output 2025_07
python main.py group --folder 2025_07 --threshold 0.01 --jaccard 0.8 --modifiers top
