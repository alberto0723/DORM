call ..\venv\Scripts\activate.bat
python main.py fetch --year 2003 --month 12 --day 1
python main.py fetch --year 2003 --month 12 --day 2
python main.py fetch --year 2003 --month 12 --day 3
python main.py fetch --year 2003 --month 12 --day 4
python main.py fetch --year 2003 --month 12 --day 5
python main.py fetch --year 2003 --month 12 --day 6
python main.py fetch --year 2003 --month 12 --day 7
python main.py fetch --year 2003 --month 12 --day 8
python main.py fetch --year 2003 --month 12 --day 9
python main.py fetch --year 2003 --month 12 --day 10
python main.py fetch --year 2003 --month 12 --day 11
python main.py fetch --year 2003 --month 12 --day 12
python main.py fetch --year 2003 --month 12 --day 13
python main.py fetch --year 2003 --month 12 --day 14
python main.py fetch --year 2003 --month 12 --day 15
python main.py fetch --year 2003 --month 12 --day 16
python main.py fetch --year 2003 --month 12 --day 17
python main.py fetch --year 2003 --month 12 --day 18
python main.py fetch --year 2003 --month 12 --day 19
python main.py fetch --year 2003 --month 12 --day 20
python main.py fetch --year 2003 --month 12 --day 21
python main.py fetch --year 2003 --month 12 --day 22
python main.py fetch --year 2003 --month 12 --day 23
python main.py fetch --year 2003 --month 12 --day 24
python main.py fetch --year 2003 --month 12 --day 25
python main.py fetch --year 2003 --month 12 --day 26
python main.py fetch --year 2003 --month 12 --day 27
python main.py fetch --year 2003 --month 12 --day 28
python main.py fetch --year 2003 --month 12 --day 29
python main.py fetch --year 2003 --month 12 --day 30
python main.py fetch --year 2003 --month 12 --day 31
copy /b data\fetched_2003_12_??_all.txt data\2003_12\fetched_2003_12.txt
python main.py parse --input 2003_12\fetched_2003_12.txt --output 2003_12\parsed.json
python main.py clean --input 2003_12\parsed.json --output 2003_12\cleaned.json
python main.py group --input 2003_12\cleaned.json --output 2003_12\queries.json --threshold 0.01 --jaccard 0.8 --modifiers top
