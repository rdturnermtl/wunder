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

print_response = True

weather_fields = {'temperature', 'wind speed'}

# This could be moved to a config file
station_ids = {"SF": "CA/San_Francisco",
               "whistler": "airport/CVOC"}
start_date = (2018, 03, 01)
end_date = (2018, 03, 10)

url_fmt = "http://api.wunderground.com/api/%s/history_%s/q/%s.json"

# Load in wunderground API key
api_key_file = argv[1]
with open(api_key_file, 'rb') as f:
    api_key = f.read().strip()  # Remove any whitespace/newlines

# TODO check key valid

pp = pprint.PrettyPrinter(indent=4)
end_date = datetime.date(*end_date)  # Make datetime obj

for short_name, station_id in station_ids.iteritems():
    print "Fetching data for station ID (%s): %s" % (short_name, station_id)
    # initialise your csv file
    with open('%s.csv' % short_name, 'wb') as outfile:
        writer = csv.writer(outfile, lineterminator='\n')
        # TODO make fields a dict as well
        headers = ['date', 'temperature', 'wind speed']
        writer.writerow(headers)

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

            # build your row
            for history in data['history']['observations']:
                row = []
                row.append(str(history['date']['pretty']))
                row.append(str(history['tempm']))
                row.append(str(history['wspdm']))
                writer.writerow(row)
            # increment the day by one
            date += datetime.timedelta(days=1)
print "Done"
