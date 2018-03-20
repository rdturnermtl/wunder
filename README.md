# wunder
wunderground data logging: look up historical weather data for a given date range and save daily and hourly data in csv files. The package `requests-cache` is used to avoid wasteful calls to wunderground that burns your API rate limit. The script has a lot of room for speed improvements but the wunderground rate limit will be the bottleneck for free users anyway.

Usage:
```
python wunder_script.py wunder_key.txt
```
Your real wunderground API key must be in `wunder_key.txt`.

The data is saved in `[station short name]_hourly.csv` and `[station short name]_daily.csv`.

The date range, stations, and desired data fields are specified at the beginning of the script.

The only direct requirements are:
```
python==2.7.14 requests-cache==0.4.13
```
The full conda enviroment that was used for testing is in `environment.yml`.
