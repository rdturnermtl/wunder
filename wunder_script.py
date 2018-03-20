# Ryan Turner (turnerry@iro.umontreal.ca)
from collections import OrderedDict
import csv
import datetime
import pprint
from sys import argv
from time import time
import requests
import requests_cache

# Setup request cache. Ideally, we can move to non-monkey patch version later.
requests_cache.install_cache('wunderground_history',
                             backend='sqlite', expire_after=None)

# ============================================================================
# User settings. This could go in a config file at some point.
# ============================================================================

station_ids = {'SF': 'CA/San_Francisco',
               'whistler': 'airport/CVOC',
               'heavenly': 'airport/KTVL',
               'montreal': 'airport/CYHU'}
start_date = (2018, 3, 1)
end_date = (2018, 3, 10)

print_response = False
hourly_fname_fmt = '%s_hourly.csv'
daily_fname_fmt = '%s_daily.csv'

tz_short = {'UTC': 'UTC',
            'America/Vancouver': 'PT',
            'America/Los_Angeles': 'PT',
            'America/Montreal': 'ET'}

'''See the phrase glossary for field descriptions:
https://www.wunderground.com/weather/api/d/docs?d=resources/phrase-glossary
'''
hourly_fields = OrderedDict([('temperature_C', 'tempm'),
                             ('wind_speed_kph', 'wspdm'),
                             ('wind_gust_kph', 'wgustm'),
                             ('precip_mm', 'precipm'),
                             ('rain_bool', 'rain'),
                             ('snow_bool', 'snow'),
                             ('hail_bool', 'hail'),
                             ('thunder_bool', 'thunder')])

daily_fields = OrderedDict([('mean_temperature_C', 'meantempm'),
                            ('min_temperature_C', 'mintempm'),
                            ('max_temperature_C', 'maxtempm'),
                            ('mean_wind_speed_kph', 'meanwindspdm'),
                            ('min_wind_speed_kph', 'minwspdm'),
                            ('max_wind_speed_kph', 'maxwspdm'),
                            ('precip_mm', 'precipm'),
                            ('rain_bool', 'rain'),
                            ('snow_bool', 'snow'),
                            ('hail_bool', 'hail'),
                            ('thunder_bool', 'thunder')])

# ============================================================================
# The actual script.
# ============================================================================


def cleanup(x, na_value='', trace_value='0', as_type=float):
    '''Wunder API uses -999 or -9999 for missing data. Clean up that crap.'''
    if x == '' or x == 'MM':
        return na_value
    if x.startswith('-999'):
        assert(float(x) in (-999.0, -9999.0))
        return na_value
    if x == 'T':
        return trace_value

    if as_type is not None:
        try:
            x_typed = as_type(x)
            # Don't bother importing numpy just for isnan
            assert(x_typed == x_typed)
        except:
            # Could use warnings package to be fancier
            print '%s not valid as type %s' % (x, str(as_type))
            return na_value

    x = str(x)  # Ensure not unicode
    return x

# This is the url to make the request to wunderground:
url_fmt = 'http://api.wunderground.com/api/%s/history_%s/q/%s.json'

# We could make this dict to rename them, but names good enough for now.
time_fields = ['hour', 'min']
date_fields = ['year', 'mon', 'mday']
datetime_fields = date_fields + time_fields

# General setup
pp = pprint.PrettyPrinter(indent=4)
end_date = datetime.date(*end_date)
# Setup list of json keys and column names
date_cols_utc = ['_'.join((ss, tz_short['UTC'])) for ss in datetime_fields]
hourly_cols, hourly_json_keys = hourly_fields.keys(), hourly_fields.values()
daily_cols, daily_json_keys = daily_fields.keys(), daily_fields.values()

# Load in wunderground API key
api_key_file = argv[1]
with open(api_key_file, 'rb') as f:
    api_key = f.read().strip()  # Remove any whitespace/newlines

# Dump hourly data
for short_name, station_id in station_ids.iteritems():
    print 'Fetching data for station ID (%s): %s' % (short_name, station_id)
    # initialize your csv file
    with open(hourly_fname_fmt % short_name, 'wb') as outfile:
        writer = csv.writer(outfile, lineterminator='\n')
        station_tz = None  # infer on first iteration, then print headers

        date = datetime.date(*start_date)
        while date <= end_date:
            # format the date as YYYYMMDD as per wunderground api spec
            date_string = date.strftime('%Y%m%d')
            # build the url
            url = url_fmt % (api_key, date_string, station_id)
            # make the request and parse json (and time it)
            print 'Loading %s @ %s' % (short_name, date_string)
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
                    station_tz_short = tz_short[station_tz]
                    date_cols_local = ['_'.join((ss, station_tz_short))
                                       for ss in datetime_fields]
                    headers = date_cols_utc + date_cols_local + hourly_cols
                    writer.writerow(headers)
                assert(station_tz is not None)
                assert(station_tz == history['date']['tzname'])

                row = []
                for col in datetime_fields:
                    row.append(cleanup(history['utcdate'][col], as_type=int))
                for col in datetime_fields:
                    row.append(cleanup(history['date'][col], as_type=int))
                for col in hourly_json_keys:
                    row.append(cleanup(history[col]))
                writer.writerow(row)
            # increment the day by one
            date += datetime.timedelta(days=1)

print 'Repeat for daily data (with caching)'
for short_name, station_id in station_ids.iteritems():
    # initialize your csv file
    with open(daily_fname_fmt % short_name, 'wb') as outfile:
        writer = csv.writer(outfile, lineterminator='\n')
        # Setup fields
        headers = date_fields + daily_cols
        writer.writerow(headers)

        date = datetime.date(*start_date)
        while date <= end_date:
            # format the date as YYYYMMDD as per wunderground api spec
            date_string = date.strftime('%Y%m%d')
            # build the url
            url = url_fmt % (api_key, date_string, station_id)
            # make the request and parse json (and time it)
            print 'Loading %s @ %s' % (short_name, date_string)
            t = time()
            data = requests.get(url).json()
            print (time() - t), 's'

            if print_response:
                pp.pprint(data)

            # build row for daily data, unpack singleton
            history, = data['history']['dailysummary']
            row = []
            for col in date_fields:
                row.append(cleanup(history['date'][col], as_type=int))
            for col in daily_json_keys:
                row.append(cleanup(history[col]))
            writer.writerow(row)
            # increment the day by one
            date += datetime.timedelta(days=1)
print 'Done'
