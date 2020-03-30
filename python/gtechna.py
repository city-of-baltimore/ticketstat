"""
Pulls citation data from the Gtechna site

NOTE: This has only been lightly tested. If this would be used for anything extensive, there is a lot of fleshing
out that would need to be done.
"""
import argparse
import csv
import io
import pyodbc
import requests


class Gtechna:
    def __init__(self, base_url="https://baltimore.gtechna.net", username='bseel', password='3Q4J7VgrBZ'):
        self.base_url = base_url
        self.session = requests.Session()
        self._login(username, password)

    def _login(self, username, password):
        payload = {
            'bypassSso': 'true',
            'login': username,
            'password': password,
            'language': 'en'
        }

        # Post the payload to the site to log in
        self.session.post("{}/officercc/security/login.jsp".format(self.base_url), data=payload)

    def search(self, filetype='csv', *args):
        """
        :param filetype: Type of file we should get
        :param args: One or more of the search queries as tuples. Should be in the format
        (TICKETTYPE, OPERATOR, VAL!, VAL2)

        For example
        ('TICKETVIEW.TICKETCREATIONTIME', '3/1/2020', '3/7/2020'), ('TICKETVIEW.TICKETINFRACTIONCODE', '46')

        TICKET TYPE
        TICKETVIEW.TICKETNO
        TICKETVIEW.TICKETSTATUS
        TICKETVIEW.TICKETPLATE
        TICKETVIEW.TICKETPLATEPROVINCE
        TICKETVIEW.INFRACTIONDATE
        TICKETVIEW.TICKETCREATIONTIME
        TICKETVIEW.TICKETATTESTAGENTBADGENO
        TICKETVIEW.TICKETAGENTNAME
        TICKETVIEW.TICKETAGENTUNIT
        TICKETVIEW.LOCATIONDISTRICTNAME
        TICKETVIEW.TICKETINFRACTIONCODE
        TICKETVIEW.INFRACTIONTEXTEN
        TICKETVIEW.LOCATIONCIVICNO
        TICKETVIEW.LOCATIONDIRECTIONNAMEEN
        TICKETVIEW.LOCATIONSTREETNAME
        TICKETVIEW.CLIENTID
        TICKETVIEW.ISSERVEROWNER
        TICKETVIEW.CLIENTSOFTWARE
        TICKETVIEW.TICKETEXPORTDATE

        OPERATOR
        1 - IS
        3 - CONTAINS
        5 - <=
        6 - >=
        7 - <
        8 - >
        9 - IS BETWEEN
        101 - IS NOT
        104 - IS NOT NULL
        102- NOT CONTAINS
        201- IS NULL
        :return:
        """
        datatype = []
        operators = []
        var1 = []
        var2 = []

        for arg in args:
            datatype.append(arg[0])
            operators.append(arg[1])
            var1.append(arg[2:3] or '')
            var2.append(arg[3:4] or '')
        data = {
             'ticketList-f': datatype,
             'ticketList-fop': operators,
             'ticketList-v1': var1,
             'ticketList-v2': var2,
             'co': ['TICKETNO', 'TICKETSTATUS', 'TICKETPLATEPROVINCE', 'TICKETPLATEPROVINCE', 'INFRACTIONDATE',
                    'TICKETCREATIONTIME', 'TICKETATTESTAGENTBADGENO', 'TICKETAGENTNAME', 'TICKETAGENTUNIT',
                    'LOCATIONDISTRICTNAME', 'TICKETINFRACTIONCODE', 'INFRACTIONTEXTEN', 'TICKETFINE', 'LOCATIONCIVICNO',
                    'LOCATIONDIRECTIONNAMEEN', 'LOCATIONSTREETNAME', 'CLIENTID', 'ISSERVEROWNER', 'CLIENTSOFTWARE',
                    'TICKETEXPORTDATE'],
             'ticketList-oo': 'DESC'
        }
        filetype = filetype.lower()
        result_lookup = {'csv': self.get_results_csv,
                         'xls': self.get_results_xls,
                         'xml': self.get_results_xml,
                         'pdf': self.get_results_pdf}
        assert filetype in result_lookup.keys(), "Valid file types are {}".format(result_lookup.keys())

        return result_lookup[filetype](data)

    def get_results_csv(self, data):
        data['d-2698956-e'] = 1  # csv mode
        csv_text = self._get_results(data)
        return csv.DictReader(io.StringIO(csv_text.text))

    def get_results_xls(self, data):
        data['d-2698956-e'] = 2  # xls mode
        return self._get_results(data) # TODO: should parse this data

    def get_results_xml(self, data):
        data['d-2698956-e'] = 3  # xml mode
        return self._get_results(data) # TODO: should parse this data

    def get_results_pdf(self, data):
        data['d-2698956-e'] = 5  # pdf mode
        return self._get_results(data) # TODO: We should do something with this data

    def _get_results(self, data):
        return self.session.post("https://baltimore.gtechna.net/officercc/seci/ticketList.jsp", data=data)


def start_from_cmd_line():
    parser = argparse.ArgumentParser(description='Insert citation data into database')
    parser.add_argument('-m', '--month', type=int,
                        help=('Optional: Month of date we should scrape  (IE: 10 for Oct). Defaults to all '
                              'days if not specified'))
    parser.add_argument('-d', '--day', type=int,
                        help=('Optional: Day of date we should scrape  (IE: 5). Defaults to all days if '
                              'not specified'))
    parser.add_argument('-y', '--year', type=int,
                        help=('Optional: Year of date we should scrape (IE: 2020). Defaults to all days '
                               'if not specified'))
    parser.add_argument('-n', '--numofdays', default=1, type=int,
                        help='Optional: Number of days to scrape going backwards, including the start date.')

    args = parser.parse_args()

    conn = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE [dbo].[parkingstat1](
    [Ticket No] [varchar](50) NULL,
    [Status] [varchar](4) NULL,
    [Plate] [varchar](20) NULL,
    [State] [varchar](4) NULL,
    [Officer Badge No] [varchar](max) NULL,
    [Officer Name] [varchar](36) NULL,
    [Squad] [varchar](max) NULL,
    [Post] [varchar](max) NULL,
    [Violation Code] [smallint] NULL,
    [Infraction Text] [varchar](80) NULL,
    [Fine] [real] NULL,
    [ClientId] [varchar](20) NULL,
    [Server] [varchar](max) NULL,
    [Software] [varchar](24) NULL,
    [Export Date] [datetime] NULL,
    [Infraction Datetime] [datetime] NULL,
    [Street Address] [varchar](46) NULL,
    [Latitude] [real] NULL,
    [Longitude] [real] NULL
    )""")
    cursor.commit()

td = Gtechna()
x = td.search('csv',
              ('TICKETVIEW.INFRACTIONDATE', '9', '3/1/2020', '3/7/2020'),
              ('TICKETVIEW.TICKETINFRACTIONCODE', '1', '46'))
assert(sum(1 for _ in x) == 177)
