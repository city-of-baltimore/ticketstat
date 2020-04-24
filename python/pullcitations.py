"""
Takes the data from GTechna and standardizes it
"""
import argparse
import datetime
import json
import logging
import math
import os
import pickle
from retrying import retry
import requests

import pyodbc
import creds
from gtechna import Gtechna

logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')


def retry_if_connection_error(exception):
    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
    return isinstance(exception, requests.exceptions.ConnectionError)


class CitationData(Gtechna):
    """Pulls citation data from Gtechna and puts it in the database"""
    def __init__(self, username, password):
        super().__init__(username, password)

    @staticmethod
    @retry(stop_max_attempt_number=10,
           wait_exponential_multiplier=1000,
           wait_exponential_max=300000,
           retry_on_exception=retry_if_connection_error)
    def get_geo(street_address, cached_geo):
        """
        Pulls the latitude and longitude of an address, either from the internet, or the cached version
        """
        geo_lookup = ("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?"
                      "singleLine={}&"
                      "f=json&"
                      "outFields=Match_addr,Addr_type")

        if not cached_geo.get(street_address):
            logging.info("Get address %s", street_address)
            req = requests.get(geo_lookup.format(street_address))
            try:
                data = req.json()
            except json.decoder.JSONDecodeError:
                logging.error("JSON ERROR: %s", req)

            latitude = data['candidates'][0]['location']['y']
            longitude = data['candidates'][0]['location']['x']
            cached_geo[street_address] = (latitude, longitude)
            return latitude, longitude
        return cached_geo.get(street_address)

    def enrich_data(self, pickle_file='geo.pickle'):
        """
        Formats the data for the database by combining the address fields, date time fields and getting the lat/long
        :param pickle_file: (string) the pickle file with previous address lookups (optional)
        :return: None
        """
        def get_block(num):
            """Rounds a street address number to the block number"""
            if int(num) < 100 or num == '':
                return 1
            return int(math.floor(int(num)/100) * 100)

        cached_geo = pickle.load(open(pickle_file, "rb")) if os.path.isfile(pickle_file) else {}

        processed_data = []
        for row in self.data:
            streetnum = get_block(row.pop('Civic #', 1))
            direction = row.pop('Direction', '')
            street = row.pop('Street', '')
            infraction_datetime = "{} {}".format(row.pop('Infraction Date', ''), row.pop('Creation Time', ''))

            street_addr = "{num} {dir}{street}".format(num=streetnum if streetnum != '0' else 1,
                                                       dir="{} ".format(direction) if direction else "",
                                                       street=street)
            address = "{},baltimore,md".format(street_addr)
            lat, lng = self.get_geo(address, cached_geo)

            row['Infraction Datetime'] = infraction_datetime
            row['Street Address'] = address
            row['Latitude'] = lat
            row['Longitude'] = lng

            processed_data.append(row)

        self.data = processed_data
        pickle.dump(cached_geo, open(pickle_file, "wb"))

    def insert_data(self, searchdate, createtable=False):
        """
        Insert the dictionary of values into the database

        :param searchdate: (datetime.date) Date to query from Gtechna and insert into the database
        :param createtable: (bool) Create parkingstat database if true
        """
        conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
        cursor = conn.cursor()

        if createtable:
            cursor.execute("""CREATE TABLE [dbo].[parkingstat](
            [Ticket_No] [varchar](100) NULL,
            [Status] [varchar](4) NULL,
            [Plate] [varchar](20) NULL,
            [State] [varchar](4) NULL,
            [Officer_Badge_No] [varchar](max) NULL,
            [Officer_Name] [varchar](100) NULL,
            [Squad] [varchar](max) NULL,
            [Post] [varchar](max) NULL,
            [Violation_Code] [smallint] NULL,
            [Infraction_Text] [varchar](80) NULL,
            [Fine] [real] NULL,
            [ClientId] [varchar](20) NULL,
            [Server] [varchar](max) NULL,
            [Software] [varchar](24) NULL,
            [Export_Date] [datetime] NULL,
            [Infraction_Datetime] [datetime] NULL,
            [Street_Address] [varchar](100) NULL,
            [Latitude] [real] NULL,
            [Longitude] [real] NULL
            )""")

            cursor.commit()

        self.get_results_by_date(searchdate)
        self.enrich_data()

        insert_data = []
        for row in self.data:
            insert_data.append((row['Ticket #'], row['Status'], row['Plate'], row['State'], row['Officer Badge No'],
                                row['Officer Name'], row['Squad'], row['Post'], row['violation Code'],
                                row['Infraction Text'], row['Fine'], row['Client Id'], row['Server'], row['Software'],
                                row['Export Date'], row['Infraction Datetime'], row['Street Address'], row['Latitude'],
                                row['Longitude']))

        if len(insert_data) == 0:
            print("No results for {}".format(searchdate))
            return

        cursor.executemany("""
        MERGE parkingstat USING (
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ) AS vals (Ticket_No, Status, Plate, State, Officer_Badge_No, Officer_Name, Squad, Post, Violation_Code,
        Infraction_Text, Fine, ClientId, Server, Software, Export_Date, Infraction_Datetime, Street_Address,
        Latitude, Longitude)
        ON (parkingstat.Ticket_No = vals.Ticket_No AND
            parkingstat.Violation_Code = vals.Violation_Code)
        WHEN NOT MATCHED THEN
            INSERT (Ticket_No, Status, Plate, State, Officer_Badge_No, Officer_Name, Squad, Post,
                Violation_Code, Infraction_Text, Fine, ClientId, Server, Software, Export_Date,
                Infraction_Datetime, Street_Address, Latitude, Longitude)
            VALUES (Ticket_No, Status, Plate, State, Officer_Badge_No, Officer_Name, Squad, Post,
                Violation_Code, Infraction_Text, Fine, ClientId, Server, Software, Export_Date,
                Infraction_Datetime, Street_Address, Latitude, Longitude);
        """, insert_data)

        cursor.commit()


def start_from_cmd_line():
    """
    Main function
    """
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    parser = argparse.ArgumentParser(description='Circulator ridership aggregator')
    parser.add_argument('-m', '--month', type=int, default=yesterday.month,
                        help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Defaults to all '
                              'days if not specified'))
    parser.add_argument('-d', '--day', type=int, default=yesterday.day,
                        help=('Optional: Day of date we should start searching on (IE: 5). Defaults to all days if '
                              'not specified'))
    parser.add_argument('-y', '--year', type=int, default=yesterday.year,
                        help=('Optional: Year of date we should start searching on (IE: 2020). Defaults to all days '
                              'if not specified'))
    parser.add_argument('-n', '--numofdays', default=1, type=int,
                        help='Optional: Number of days to search, including the start date.')

    args = parser.parse_args()
    create_table = False

    citations = CitationData(creds.USERNAME, creds.PASSWORD)
    for i in range(args.numofdays):
        insert_date = datetime.date(args.year, args.month, args.day) + datetime.timedelta(days=i)
        print("Processing {}".format(insert_date))
        citations.insert_data(insert_date, create_table)
        create_table = False


if __name__ == '__main__':
    start_from_cmd_line()
