call ..\venv\Scripts\activate.bat
python main.py fetch --year 2025 --month 6 --day 1
python main.py fetch --year 2025 --month 6 --day 2
python main.py fetch --year 2025 --month 6 --day 3
python main.py fetch --year 2025 --month 6 --day 4
python main.py fetch --year 2025 --month 6 --day 5
python main.py fetch --year 2025 --month 6 --day 6
python main.py fetch --year 2025 --month 6 --day 7
python main.py fetch --year 2025 --month 6 --day 8
python main.py fetch --year 2025 --month 6 --day 9
python main.py fetch --year 2025 --month 6 --day 10
python main.py fetch --year 2025 --month 6 --day 11
python main.py fetch --year 2025 --month 6 --day 12
python main.py fetch --year 2025 --month 6 --day 13
python main.py fetch --year 2025 --month 6 --day 14
python main.py fetch --year 2025 --month 6 --day 15
python main.py fetch --year 2025 --month 6 --day 16
python main.py fetch --year 2025 --month 6 --day 17
python main.py fetch --year 2025 --month 6 --day 18
python main.py fetch --year 2025 --month 6 --day 19
python main.py fetch --year 2025 --month 6 --day 20
python main.py fetch --year 2025 --month 6 --day 21
python main.py fetch --year 2025 --month 6 --day 22
python main.py fetch --year 2025 --month 6 --day 23
python main.py fetch --year 2025 --month 6 --day 24
python main.py fetch --year 2025 --month 6 --day 25
python main.py fetch --year 2025 --month 6 --day 26
python main.py fetch --year 2025 --month 6 --day 27
python main.py fetch --year 2025 --month 6 --day 28
python main.py fetch --year 2025 --month 6 --day 29
python main.py fetch --year 2025 --month 6 --day 30
copy /b data\fetched_2025_06_??_all.txt data\2025_06\fetched_2025_06.txt
python main.py parse --input 2025_06\fetched_2025_06.txt --output 2025_06
python main.py group --input 2025_06\parsed.json --output 2025_06\queries.json --threshold 0.01 --jaccard 0.8 --modifiers top
