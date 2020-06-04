"""Wrapper for the CiteWeb website to get red light and over height ticket information"""
from bs4 import BeautifulSoup
import requests


class CiteWeb:
    """Interface for CiteWeb that handles authentication and scraping"""
    def __init__(self, username, password):
        """
        Interface to work with
        :param username:
        :param password:
        """
        self.session = requests.Session()
        self._state_vals = {}

        self._login(username, password)

    def _login(self, username, password):
        payload = {
            'txtUser': username,
            'txtPassword': password,
            'btnLogin': 'Sign In',
            'forgotpwd': 0
        }

        resp = self.session.get('https://cw3.cite-web.com/loginhub/Main.aspx')
        self._get_state_values(resp)

        payload.update(self._state_vals)

        resp = self.session.post('https://cw3.cite-web.com/loginhub/Main.aspx', data=payload)
        self._get_state_values(resp)

    def _login_otp(self, otp):
        payload = {
            'txtUser': '',
            'txtPassword': '',
            'txtOTP': otp,
            'btnOTP': 'Submit',
            'forgotpwd': 0
        }

        # Post the payload to the site to log in
        payload.update(self._state_vals)
        resp = self.session.post('https://cw3.cite-web.com/loginhub/Main.aspx', data=payload)
        self._get_state_values(resp)

    def _get_state_values(self, resp):
        """
        Gets the ASP.net state values from the hidden fields and populates them in self._state_vals
        :param resp: Response object
        :return: None. Values are set in self._state_vals
        """
        soup = BeautifulSoup(resp.text, "html.parser")
        hidden_tags = soup.find_all("input", type="hidden")
        tags = {}
        for tag in hidden_tags:
            tags[tag['name']] = tag['value']

        # Post the payload to the site to log in
        self._state_vals['__VIEWSTATE'] = tags['__VIEWSTATE']
        self._state_vals['__VIEWSTATEGENERATOR'] = tags['__VIEWSTATEGENERATOR']
        self._state_vals['__EVENTVALIDATION'] = tags['__EVENTVALIDATION']

    def get_deployment_by_month(self, month, year):
        """
        Gets the values of the red light camera deployments, with the number of accepted and rejected tickets

        :param month: (str) Month to request, completely spelled out (IE January)
        :param year: (int) Four digit year to request
        :return:
        """
        assert (month in ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December'])

        # Setup the cookies with these requests
        self.session.get('https://cw3.cite-web.com/citeweb3/Default.asp?ID=1488671')
        self.session.get('https://cw3.cite-web.com/citeweb3/citmenu.asp?DB=BaltimoreRL&Site=Maryland')

        resp = self.session.get(
            'https://cw3.cite-web.com/citeweb3/DeplByMonth_BaltimoreRL.asp?Month={}&Year={}'.format(month, year))
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"class": "detail"}, border=1)

        results = []
        for row in table.find_all("tr"):
            elements = row.find_all("td")
            if len(elements) != 8:
                print("Skipping {}".format(elements))
                continue

            row_dict = {
                'id': "" if not row.find_all('td')[0].p else row.find_all('td')[0].a.text,
                'start_time': "" if not row.find_all('td')[1].p else row.find_all('td')[1].p.text,
                'end_time': "" if not row.find_all('td')[2].p else row.find_all('td')[2].p.text,
                'location': "" if not row.find_all('td')[3].p else row.find_all('td')[3].p.text,
                'officer': "" if not row.find_all('td')[4].p else row.find_all('td')[4].p.text,
                'equip_type': "" if not row.find_all('td')[5].p else row.find_all('td')[5].p.text,
                'issued': "" if not row.find_all('td')[6].p else row.find_all('td')[6].p.text,
                'rejected': "" if not row.find_all('td')[7].p else row.find_all('td')[7].p.text
            }

            results.append(row_dict)

        return results

    def get_ticket_information(self):
        """Get ticket information from cameras"""
