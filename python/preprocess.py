"""
Takes the csv format from GTechna and standardizes it
"""
import argparse
import csv
import glob
import json
import logging
import os
import pickle

import requests
from retrying import retry

CACHED_GEO = {}
GEO_LOOKUP = ("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?"
              "singleLine={}&"
              "f=json&"
              "outFields=Match_addr,Addr_type")

URL = ("https://maps.googleapis.com/maps/api/geocode/json")
OUTFILE = 'outfile.csv'
ANON_OUT_FILE = 'outfile_anon.csv'
INFILE = 'infile.csv'
PICKLE_FILE = 'geo.pickle'

logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')


def retry_if_connection_error(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    return isinstance(exception, requests.exceptions.ConnectionError)


@retry(stop_max_attempt_number=10,
       wait_exponential_multiplier=1000,
       wait_exponential_max=300000,
       retry_on_exception=retry_if_connection_error)
def get_geo(street_address):
    """
    Pulls the latitude and longitude of an address, either from the internet, or the cached version
    """
    if not CACHED_GEO.get(street_address):
        logging.info("Get address %s", street_address)
        req = requests.get(GEO_LOOKUP.format(street_address))
        try:
            data = req.json()
        except json.decoder.JSONDecodeError:
            logging.error("JSON ERROR: %s", req)

        latitude = data['candidates'][0]['location']['y']
        longitude = data['candidates'][0]['location']['x']
        CACHED_GEO[street_address] = (latitude, longitude)
        return latitude, longitude
    return CACHED_GEO.get(street_address)



def process_file(raw_csv, outfile = OUTFILE):
    if os.path.isfile(PICKLE_FILE):
        CACHED_GEO = pickle.load(PICKLE_FILE)
    else:
        CACHED_GEO = {}

    with open(OUTFILE, 'w') as out_file:
        print("Opening {}".format(raw_csv))
        with open(raw_csv, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            first = True
            for in_row in csv_reader:
                if first:
                    # Read the other header and then add the extra fields
                    csv_header = in_row
                    csv_header.remove('Civic #')
                    csv_header.remove('Direction')
                    csv_header.remove('Street')
                    csv_header.remove('Infraction Date')
                    csv_header.remove('Creation Time')
                    csv_header.append('Infraction Datetime')
                    csv_header.append('Street Address')
                    csv_header.append('Latitude')
                    csv_header.append('Longitude')
                    writer = csv.DictWriter(out_file, fieldnames=csv_header)
                    writer.writeheader()
                    first = False
                    continue

                # minimal data checks
                assert len(in_row) == 20
                float(in_row[12])
                int(in_row[13])

                street_addr = "{num} {dir}{street}".format(num=in_row[13] if in_row[13] != '0' else 1,
                                                           dir="{} ".format(in_row[14]) if in_row[14] else "",
                                                           street=in_row[15])
                address = "{},baltimore,md".format(street_addr)
                lat, lng = get_geo(address)

                # Pull out the fragmented up street name, and date/time fields, and put them back combined
                in_row.append("{} {}".format(in_row[4], in_row[5]))  # Combine date and time fields
                in_row.append(street_addr)  # Combined Civic #, Direction, Street
                in_row.append(lat)  # Latitude
                in_row.append(lng)  # Longitude
                in_row.pop(15)  # Street
                in_row.pop(14)  # Direction
                in_row.pop(13)  # Civic #
                in_row.pop(5)  # Creation time
                in_row.pop(4)  # Infraction date

                out_row = {}
                for head_row, data_row in zip(csv_header, in_row):
                    out_row[head_row] = data_row
                writer.writerow(out_row)

    pickle.dump(CACHED_GEO, PICKLE_FILE)

def anonymize_file(raw_file, outfile=ANON_OUT_FILE):
    """
    We only need the datetime, address, infraction type, and lat/long. The rest is unnecessary, may contain PII and
    can be stripped first

    :param raw_file: (str) path to the file that should be anonymized (either relative or absolute)
    """
    with open(outfile, 'w') as out_file:
        with open(raw_file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            first = True
            for in_row in csv_reader:
                if first:
                    writer = csv.DictWriter(out_file, fieldnames=['Infraction Datetime',
                                                                  'violation Code',
                                                                  'Infraction Text',
                                                                  'Street Address',
                                                                  'Latitude',
                                                                  'Longitude'])
                    writer.writeheader()
                    first = False
                    continue
                writer.writerow({'Infraction Datetime':in_row[15],
                                 'violation Code': in_row[8],
                                 'Infraction Text':in_row[9],
                                 'Street Address': in_row[16],
                                 'Latitude': in_row[17],
                                 'Longitude': in_row[18]
                                })

def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(description='Parking citation data processor')
    parser.add_argument('-p', '--parkingdata',
                        nargs=2,
                        metavar=('infile', 'outfile'),
                        help='Process the specified csv file, to normalize it for the ParkingStat app')
    parser.add_argument('-a', '--anonymize',
                        nargs=2,
                        metavar=('infile', 'outfile'),
                        help='Anonymize a file that has already been processed with -p')

    args = parser.parse_args()


    if args.parkingdata:
        in_file, out_file = args.parkingdata
        process_file(in_file, out_file)
    if args.anonymize:
        in_file, out_file = args.anonymize
        anonymize_file(in_file, out_file)


if __name__ == '__main__':
    main()
