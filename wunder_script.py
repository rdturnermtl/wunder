# Ryan Turner (turnerry@iro.umontreal.ca)
import csv
import datetime
import pprint
from sys import argv
from time import time
import requests
import requests_cache

# TODO change quote style

# Ideally, we can move to non-monkey patch version later
requests_cache.install_cache('wunderground_history',
                             backend='sqlite', expire_after=None)

print_response = False

# We could make this dict to rename them, but names good enough for now.
date_fields = ['year', 'mon', 'mday', 'hour', 'min']

tz_short_names = {'UTC': 'UTC',
                  'America/Vancouver': 'PT',
                  'America/Los_Angeles': 'PT'}

# https://www.wunderground.com/weather/api/d/docs?d=resources/phrase-glossary
# TODO add units to later ones
hourly_fields = {'temperature_C': 'tempm',
                 'wind_speed_kph': 'wspdm',
                 'wind_gust_kph': 'wgustm',
                 'precip_mm': 'precipm',
                 'rain': 'rain',
                 'snow': 'snow',
                 'hail': 'hail',
                 'thunder': 'thunder'}

daily_fields = {'mean_temperature_C': 'meantempm',
                'min_temperature_C': 'mintempm',
                'max_temperature_C': 'maxtempm',
                'mean_wind_speed_kph': 'meanwindspdm',
                'min_wind_speed_kph': 'minwindspdm',
                'max_wind_speed_kph': 'maxwindspdm',
                'precip_mm': 'precipm',
                'rain': 'rain',
                'snow': 'snow',
                'snowfall_cm': 'snowfallm',  # TODO check units
                'cum_snowfall_cm': 'since1julsnowfallm',
                'snow_depth_cm': 'snowdepthm',
                'hail': 'hail',
                'thunder': 'thunder'}

# This could be moved to a config file
station_ids = {"SF": "CA/San_Francisco",
               "whistler": "airport/CVOC"}
start_date = (2018, 03, 01)
end_date = (2018, 03, 10)

url_fmt = "http://api.wunderground.com/api/%s/history_%s/q/%s.json"

# General setup
pp = pprint.PrettyPrinter(indent=4)
end_date = datetime.date(*end_date)
# Setup list of json keys and column names
date_cols_utc = [ss + tz_short_names['UTC'] for ss in date_fields]
hourly_cols, hourly_json_keys = hourly_fields.keys(), hourly_fields.values()

# Load in wunderground API key
api_key_file = argv[1]
with open(api_key_file, 'rb') as f:
    api_key = f.read().strip()  # Remove any whitespace/newlines

# TODO check key valid

for short_name, station_id in station_ids.iteritems():
    print "Fetching data for station ID (%s): %s" % (short_name, station_id)
    # initialise your csv file
    with open('%s.csv' % short_name, 'wb') as outfile:
        writer = csv.writer(outfile, lineterminator='\n')
        station_tz = None  # infer on first iteration, then print headers

        date = datetime.date(*start_date)
        while date <= end_date:
            # format the date as YYYYMMDD as per wunderground api spec
            date_string = date.strftime('%Y%m%d')
            # build the url
            url = url_fmt % (api_key, date_string, station_id)
            # make the request and parse json (and time it)
            print "Loading %s @ %s" % (short_name, date_string)
            t = time()
            data = requests.get(url).json()
            print (time() - t), 's'

            if print_response:
                pp.pprint(data)

            # build row for hourly data
            for history in data['history']['observations']:
                # Setup for headers on first iteration
                if station_tz is None:
                    station_tz = history['date']['tzname']
                    date_cols_local = [ss + tz_short_names[station_tz] for
                                       ss in date_fields]
                    headers = date_cols_utc + date_cols_local + hourly_cols
                    writer.writerow(headers)
                assert(station_tz is not None)
                assert(station_tz == history['date']['tzname'])

                row = []
                for col in date_fields:
                    row.append(str(history['utcdate'][col]))
                for col in date_fields:
                    row.append(str(history['date'][col]))
                for col in hourly_json_keys:
                    row.append(str(history[col]))
                writer.writerow(row)
            # increment the day by one
            date += datetime.timedelta(days=1)
print "Done"
