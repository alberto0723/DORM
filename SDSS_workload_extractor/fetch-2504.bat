call ..\venv\Scripts\activate.bat
python main.py fetch --year 2025 --month 4 --day 1
python main.py fetch --year 2025 --month 4 --day 2
python main.py fetch --year 2025 --month 4 --day 3
python main.py fetch --year 2025 --month 4 --day 4
python main.py fetch --year 2025 --month 4 --day 5
python main.py fetch --year 2025 --month 4 --day 6
python main.py fetch --year 2025 --month 4 --day 7
python main.py fetch --year 2025 --month 4 --day 8
python main.py fetch --year 2025 --month 4 --day 9
python main.py fetch --year 2025 --month 4 --day 10
python main.py fetch --year 2025 --month 4 --day 11
python main.py fetch --year 2025 --month 4 --day 12
python main.py fetch --year 2025 --month 4 --day 13
python main.py fetch --year 2025 --month 4 --day 14
python main.py fetch --year 2025 --month 4 --day 15
python main.py fetch --year 2025 --month 4 --day 16
python main.py fetch --year 2025 --month 4 --day 17
python main.py fetch --year 2025 --month 4 --day 18
python main.py fetch --year 2025 --month 4 --day 19
python main.py fetch --year 2025 --month 4 --day 20
python main.py fetch --year 2025 --month 4 --day 21
python main.py fetch --year 2025 --month 4 --day 22
python main.py fetch --year 2025 --month 4 --day 23
python main.py fetch --year 2025 --month 4 --day 24
python main.py fetch --year 2025 --month 4 --day 25
python main.py fetch --year 2025 --month 4 --day 26
python main.py fetch --year 2025 --month 4 --day 27
python main.py fetch --year 2025 --month 4 --day 28
python main.py fetch --year 2025 --month 4 --day 29
python main.py fetch --year 2025 --month 4 --day 30
copy /b data\fetched_2025_04_??_all.txt data\2025_04\fetched_2025_04.txt
python main.py parse --input 2025_04\fetched_2025_04.txt --output 2025_04
python main.py group --folder 2025_04 --threshold 0.02 --jaccard 0.8 --modifiers top
