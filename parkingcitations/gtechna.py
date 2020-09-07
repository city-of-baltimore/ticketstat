"""
Pulls citation data from the Gtechna site

NOTE: This has only been lightly tested. If this would be used for anything extensive, there is a lot of fleshing
out that would need to be done.
"""
import csv
import io
import requests
from retrying import retry


class Gtechna:
    """ Interacts with gtechna to get citation information """
    def __init__(self, username, password, base_url="https://baltimore.gtechna.net"):
        self.base_url = base_url
        self.session = requests.Session()
        self._login(username, password)
        self.data = None

    def _login(self, username, password):
        payload = {
            'bypassSso': 'true',
            'login': username,
            'password': password,
            'language': 'en'
        }

        # Post the payload to the site to log in
        resp = self.session.post("{}/officercc/security/login.jsp".format(self.base_url), data=payload)
        if "Wrong User Name or Password" in resp.text:
            raise Exception("Invalid username or password")

    def search(self, *args, filetype='csv'):
        """
        :param filetype: Type of file we should get
        :param args: One or more of the search queries as tuples. Should be in the format
        (TICKETTYPE, OPERATOR, VAL1, VAL2)

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
        :return: No return, but it sets self.data to the returned data
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
        result_lookup = {'csv': self._get_results_csv}
        assert filetype in result_lookup.keys(), "Valid file types are {}".format(result_lookup.keys())

        try:
            self.data = result_lookup[filetype](data)
        except (StopIteration, KeyError):
            # We have the retry, but sometimes there are just no results
            pass

    def get_results_by_date(self, date):
        """
        Get all citation information by a single date, and store it in self.data
        :param date: (datetime.date) Date to search
        :return: None
        """
        self.search('csv', ('TICKETVIEW.INFRACTIONDATE', '1', date.strftime("%m/%d/%Y")))

    @retry(stop_max_attempt_number=7,
           wait_exponential_multiplier=1000,
           wait_exponential_max=10000)
    def _get_results_csv(self, data: dict) -> csv.DictReader:
        data['d-2698956-e'] = 1  # csv mode
        csv_text = self._get_results(data)

        # Validates that the response is valid by checking the first element for the ticket number. Otherwise, a
        # keyerror is thrown and the retrying logic kicks in
        next(csv.DictReader(io.StringIO(csv_text.text)))['Ticket #']

        return csv.DictReader(io.StringIO(csv_text.text))

    def _get_results(self, data):
        return self.session.post("https://baltimore.gtechna.net/officercc/seci/ticketList.jsp", data=data)
