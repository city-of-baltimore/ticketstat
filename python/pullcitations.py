"""
Takes the data from Gtechna and standardizes it
"""
import argparse
import datetime
import logging
import math
import os
import pickle
import requests

import googlemaps
import creds
import pyodbc
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
        self.data = None

    @staticmethod
    def geocode(street_address, cached_geo) -> dict:
        """
        Pulls the latitude and longitude of an address, either from the internet, or the cached version
        :param street_address:
        :param cached_geo:
        :return: Dictionary with the keys 'Latitude', 'Longitude', 'Street Address', 'Street Num', 'Street Name',
        'Neighborhood'
        """
        gmaps = googlemaps.Client(key=creds.API_KEY)

        if not cached_geo.get(street_address):
            logging.info("Get address %s", street_address)
            geocode_result = gmaps.geocode(street_address)

            if len(geocode_result) > 1:
                logging.warning("Got more than one result for %s: %s", street_address, geocode_result)

            geocode_result = geocode_result[0]

            ret = {'Latitude': geocode_result["geometry"]["location"]["lat"],
                   'Longitude': geocode_result["geometry"]["location"]["lng"],
                   'Street Address': geocode_result["formatted_address"]}

            for component in geocode_result["address_components"]:
                if "street_no" in component["types"]:
                    ret["Street Num"] = component["long_name"]
                elif "route" in component["types"]:
                    ret["Street Name"] = component["long_name"]
                if "neighborhood" in component["types"]:
                    ret["Neighborhood"] = component["long_name"]

            cached_geo[street_address] = ret
            return ret
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
            return int(math.floor(int(num) / 100) * 100)

        cached_geo = pickle.load(open(pickle_file, "rb")) if os.path.isfile(pickle_file) else {}

        processed_data = []
        for row in self.data:
            streetnum = get_block(row.pop('Civic #', 1))
            direction = row.pop('Direction', '')
            street = row.pop('Street', '')
            infraction_datetime = "{} {}".format(row.pop('Infraction Date', ''), row.pop('Creation Time', ''))
            row['Infraction Datetime'] = infraction_datetime

            street_addr = "{num} {dir}{street}".format(num=streetnum if streetnum != '0' else 1,
                                                       dir="{} ".format(direction) if direction else "",
                                                       street=street)
            # The Jones Falls Expressway lots show up as 400 lot jfa or 500 lot jfb, which doesn't geocode
            if '00 lot jf' in street_addr.lower():
                geo = self.geocode('400 Saratoga St, Baltimore, Maryland', cached_geo)
            else:
                address = "{}, Baltimore, Maryland".format(street_addr)
                geo = self.geocode(address, cached_geo)

            if geo['Latitude'] < 39.1 or geo['Latitude'] > 39.4 or geo['Longitude'] > -76.5 or geo['Longitude'] < -76.8:
                logging.warning("Got lat/long from outside Baltimore City: %s/%s", geo['Latitude'], geo['Longitude'])
                geo['Latitude'] = None
                geo['Longitude'] = None

            row.update(geo)

            processed_data.append(row)

        self.data = processed_data
        pickle.dump(cached_geo, open(pickle_file, "wb"))

    def insert_data(self, search_date, create_table=False):
        """
        Insert the dictionary of values into the database

        :param search_date: (datetime.date) Date to query from Gtechna and insert into the database
        :param create_table: (bool) Create ticketstat database if true
        """
        conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
        cursor = conn.cursor()

        if create_table:
            cursor.execute("""CREATE TABLE [dbo].[ticketstat](
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
            [Street_No] [varchar](20) NULL,
            [Street_Name] [varchar](50) NULL,
            [Neighborhood] [varchar](max) NULL,
            [Street_Address] [varchar](100) NULL,
            [Latitude] [real] NULL,
            [Longitude] [real] NULL
            )""")

            cursor.commit()

        self.get_results_by_date(search_date)
        self.enrich_data()

        insert_data = []
        for row in self.data:
            insert_data.append((row['Ticket #'], row['Status'], row['Plate'], row['State'], row['Officer Badge No'],
                                row['Officer Name'], row['Squad'], row['Post'], row['violation Code'],
                                row['Infraction Text'], row['Fine'], row['Client Id'], row['Server'], row['Software'],
                                row['Export Date'], row['Infraction Datetime'], row['Street Num'],
                                row['Street Name'], row['Neighborhood'], row['Street Address'], row['Latitude'],
                                row['Longitude']))

        if len(insert_data) == 0:
            print("No results for {}".format(search_date))
            return

        cursor.executemany("""
        MERGE ticketstat USING (
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ) AS vals (Ticket_No, Status, Plate, State, Officer_Badge_No, Officer_Name, Squad, Post, Violation_Code,
        Infraction_Text, Fine, ClientId, Server, Software, Export_Date, Infraction_Datetime, Street_Address,
        Latitude, Longitude)
        ON (ticketstat.Ticket_No = vals.Ticket_No AND
            ticketstat.Violation_Code = vals.Violation_Code)
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
    parser = argparse.ArgumentParser(description='Pulls parking citation data from GTechna, geocodes, and puts it in '
                                                 'our internal database')
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
    parser.add_argument('-c', '--create_table', action='store_true',
                        help='Creates the database table. Only needed on first run')

    args = parser.parse_args()

    citations = CitationData(creds.USERNAME, creds.PASSWORD)
    for i in range(args.numofdays):
        insert_date = datetime.date(args.year, args.month, args.day) + datetime.timedelta(days=i)
        print("Processing {}".format(insert_date))
        citations.insert_data(insert_date, args.create_table)


if __name__ == '__main__':
    start_from_cmd_line()
