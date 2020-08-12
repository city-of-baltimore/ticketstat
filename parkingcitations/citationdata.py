"""
Takes the data from Gtechna and standardizes it
"""
import logging
import math
import os
import pickle

import googlemaps
import pyodbc

from .creds import GMAPS_API_KEY
from .gtechna import Gtechna

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class CitationData(Gtechna):
    """Pulls citation data from Gtechna and puts it in the database"""

    def __init__(self, username, password, pickle_filename='geo.pickle'):
        super().__init__(username, password)
        self.data = None
        self.pickle_filename = pickle_filename
        self.cached_geo = {}

    def __enter__(self):
        if os.path.exists(self.pickle_filename):
            with open(self.pickle_filename, 'rb') as pkl:
                self.cached_geo = pickle.load(pkl)

    def __exit__(self, *a):
        with open(self.pickle_filename, 'wb') as proc_files:
            pickle.dump(self.cached_geo, proc_files)

    def geocode(self, street_address) -> dict:
        """
        Pulls the latitude and longitude of an address, either from the internet, or the cached version
        :param street_address: Address to search. Can be anything that would be searched on google maps.
        :return: Dictionary with the keys 'Latitude', 'Longitude', 'Street Address', 'Street Num', 'Street Name',
        'Neighborhood'
        """
        gmaps = googlemaps.Client(key=GMAPS_API_KEY)

        if not self.cached_geo.get(street_address):
            logging.info("Get address %s", street_address)
            geocode_result = gmaps.geocode(street_address)

            if len(geocode_result) > 1:
                logging.warning("Got more than one result for %s: %s", street_address, geocode_result)

            geocode_result = geocode_result[0]

            ret = {'Latitude': geocode_result["geometry"]["location"]["lat"],
                   'Longitude': geocode_result["geometry"]["location"]["lng"],
                   'Street Address': geocode_result["formatted_address"], "Street Num": "", "Street Name": "",
                   "Neighborhood": ""}

            for component in geocode_result["address_components"]:
                if "street_number" in component["types"]:
                    ret["Street Num"] = component["short_name"]
                elif "route" in component["types"]:
                    ret["Street Name"] = component["short_name"]
                if "neighborhood" in component["types"]:
                    ret["Neighborhood"] = component["short_name"]

            self.cached_geo[street_address] = ret
            return ret
        return self.cached_geo.get(street_address)

    def enrich_data(self):
        """
        Formats the data for the database by combining the address fields, date time fields and getting the lat/long
        :return: None
        """

        def get_block(num):
            """Rounds a street address number to the block number"""
            if int(num) < 100 or num == '':
                return 1
            return int(math.floor(int(num) / 100) * 100)

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
            address = "{}, Baltimore, Maryland".format(street_addr)
            geo = self.geocode(address)

            if geo['Latitude'] < 39.1 or geo['Latitude'] > 39.4 or geo['Longitude'] > -76.5 or geo['Longitude'] < -76.8:
                logging.warning("Got lat/long from outside Baltimore City: %s/%s", geo['Latitude'], geo['Longitude'])
                geo['Latitude'] = None
                geo['Longitude'] = None

            row.update(geo)

            processed_data.append(row)

        self.data = processed_data

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
            [Export_Date] [datetime2] NULL,
            [Infraction_Datetime] [datetime2] NULL,
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
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ) AS vals (Ticket_No, Status, Plate, State, Officer_Badge_No, Officer_Name, Squad, Post, Violation_Code,
            Infraction_Text, Fine, ClientId, Server, Software, Export_Date, Infraction_Datetime, Street_No,
            Street_Name, Neighborhood, Street_Address, Latitude, Longitude)
            ON (ticketstat.Ticket_No = vals.Ticket_No AND
                ticketstat.Violation_Code = vals.Violation_Code)
            WHEN NOT MATCHED THEN
                INSERT (Ticket_No, Status, Plate, State, Officer_Badge_No, Officer_Name, Squad, Post,
                    Violation_Code, Infraction_Text, Fine, ClientId, Server, Software, Export_Date,
                    Infraction_Datetime, Street_No, Street_Name, Neighborhood, Street_Address, Latitude, Longitude)
                VALUES (Ticket_No, Status, Plate, State, Officer_Badge_No, Officer_Name, Squad, Post,
                    Violation_Code, Infraction_Text, Fine, ClientId, Server, Software, Export_Date,
                    Infraction_Datetime, Street_No, Street_Name, Neighborhood, Street_Address, Latitude, Longitude);
            """, insert_data)

        cursor.commit()
