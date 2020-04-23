import os
import csv
import datetime
import asyncio
import math
import whoosh
import time
import pycountry
from whoosh.fields import Schema, TEXT
from whoosh.index import create_in, FileIndex
from whoosh.qparser import QueryParser
from tabulate import tabulate
from mautrix.types import TextMessageEventContent, MessageType
from mautrix.client import MembershipEventDispatcher, InternalEventType
from mautrix.errors.request import MLimitExceeded

OFFLOOP_CASES_URL = 'http://offloop.net/covid19h/unconfirmed.csv'
OFFLOOP_GROUPS_URL = 'https://offloop.net/covid19h/groups.txt'
NHS_URL = 'https://www.arcgis.com/sharing/rest/content/items/ca796627a2294c51926865748c4a56e8/data'
UK_URL = 'https://www.arcgis.com/sharing/rest/content/items/b684319181f94875a6879bbc833ca3a6/data'
FINLAND_URL = 'https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData/v2'
UK_COUNTRIES_URL = 'https://raw.githubusercontent.com/tomwhite/covid-19-uk-data/master/data/covid-19-indicators-uk.csv'
UK_SCO_REGIONS_URL = 'https://raw.githubusercontent.com/tomwhite/covid-19-uk-data/master/data/covid-19-cases-uk.csv'

COUNTRY_RENAMES = {
    'US': 'United States',
    'DRC': 'Democratic Republic of the Congo',
    'UAE': 'United Arab Emirates',
    "U.S. Virgin Islands": "United States Virgin Islands"
}

# UK constituent countries with their nominal data update times
UK_COUNTRIES = {"Wales": "1200 GMT", "Scotland": "1400 GMT",
                "England": "1800 GMT", "Northern Ireland": "1400 GMT"}

SCHEMA = Schema(country=TEXT(stored=True), area=TEXT(
    stored=True), location=TEXT(stored=True))


class DataSource:
    def __init__(self, log, http):
        # TODO create our own logger
        self.log, self.http = log, http
        self.cases = {}
        self.groups = {}

    async def _get_offloop_groups(self):
        groups = {}

        self.log.debug("Fetching %s.", OFFLOOP_GROUPS_URL)
        async with self.http.get(OFFLOOP_GROUPS_URL) as r:
            t = await r.text()

            # group;country_1;country_2 ...
            cr = csv.reader(t.splitlines(), delimiter=';')
            for group, *areas in cr:
                groups[group] = areas

        return groups

    async def _get_nhs(self):
        regions = {}

        self.log.debug("Fetching %s.", NHS_URL)
        async with self.http.get(NHS_URL) as r:
            t = await r.text()
            l = t.splitlines()

        # GSS_CD, NHSRNm, TotalCases
        cr = csv.DictReader(l)
        for row in cr:
            regions[row['NHSRNm']] = int(row['TotalCases'].replace(',', ''))

        return regions

    async def _get_uk(self):
        regions = {}

        self.log.debug("Fetching %s.", UK_URL)
        async with self.http.get(UK_URL) as r:
            t = await r.text()
            l = t.splitlines()

        # GSS_CD, GSS_NM, TotalCases
        cr = csv.DictReader(l)
        for row in cr:
            regions[row['GSS_NM']] = int(row['TotalCases'].replace(',', ''))

        return regions

    async def _get_uk_countries(self) -> dict:
        """Get UK constituent countries: WAL/SCO/ENG/NI

        Data is processed by _process_uk_countries()
        before being returned as a dict
        """

        async def _process_uk_countries(uk_countries_data: list) -> dict:
            """Process to covbot format:

                {Country1: {data1}, Country2: {data2}, ...}
            """
            # GB/UK data is processed elsewhere
            countries_data = {}
            for country, update_time in UK_COUNTRIES.items():
                # Filter data to country (ie Wales/Scotland/England/NI)
                country_data = [r for r in uk_countries_data
                                if r["Country"] == country]
                # Find latest (= maximum) date and use that
                maxidate = max([r["Date"] for r in country_data])
                latest_country_data = [r for r in country_data
                                       if r["Date"] == maxidate]
                latest_data_d = {}
                # Pivot data to covbot format
                for r in latest_country_data:
                    self.log.debug(f"r:\t{r}")
                    latest_data_d[r["Indicator"].lower()] = int(r["Value"])
                    # Rename confirmedcases → cases
                    if "confirmedcases" in latest_data_d:
                        latest_data_d["cases"] = int(latest_data_d.pop(
                            "confirmedcases"))
                        latest_data_d["last_update"] = datetime.datetime.\
                            strptime(f"{maxidate} {update_time}",
                                     "%Y-%m-%d %H%M %Z")
                        countries_data[country] = latest_data_d

            return countries_data

        async with self.http.get(UK_COUNTRIES_URL) as r:
            t = await r.text()
            lines = t.splitlines()

        cr = list(csv.DictReader(lines))

        uk_country_data = await _process_uk_countries(cr)

        return uk_country_data

    async def _get_scottish_regions(self) -> dict:
        """Return dict of Scottish region data"""
        async def _process_scottish_regions(regions_data: list) -> dict:
            """Filter region data and process to covbot format

            NB: this could be expanded for other regions if desired
            """
            # Filter to Scotland
            scot = [r for r in regions_data if r["Country"] == "Scotland"]
            # Get latest data
            maxidate = max([r["Date"] for r in scot])
            scot_data = [r for r in scot if r["Date"] == maxidate]
            scot_region_data = {}
            for r in scot_data:
                # Fix for GJNH data 2020-04-22 (blank)
                if r["TotalCases"] == '':
                    r["TotalCases"] = 0
                scot_region_data[r["Area"]] = {
                    "cases": int(r["TotalCases"]), "last_update":
                    datetime.datetime.strptime(
                        f"{maxidate} {UK_COUNTRIES['Scotland']}",
                        "%Y-%m-%d %H%M %Z")}

            return scot_region_data

        async with self.http.get(UK_SCO_REGIONS_URL) as r:
            t = await r.text()
            lines = t.splitlines()

        cr = list(csv.DictReader(lines))

        scottish_region_data = await _process_scottish_regions(cr)

        return scottish_region_data
    async def _get_offloop_cases(self):
        countries = {}
        now = time.time() * 1000  # millis to match the data

        self.log.debug("Fetching %s.", OFFLOOP_CASES_URL)
        async with self.http.get(OFFLOOP_CASES_URL) as r:
            t = await r.text()
            l = t.splitlines()

        # Country;Province;Confirmed;Deaths;Recovered;LastUpdated
        cr = csv.DictReader(l, delimiter=';')
        for row in cr:
            country = row['Country']
            if country in COUNTRY_RENAMES:
                country = COUNTRY_RENAMES[country]

            if not country in countries:
                countries[country] = {'areas': {}}
#
            # handle missing data
            cases = 0 if row['Confirmed'] == '' else int(row['Confirmed'])
            deaths = 0 if row['Deaths'] == '' else int(row['Deaths'])
            recoveries = 0 if row['Recovered'] == '' else int(row['Recovered'])
            ts_msec = now if row['LastUpdated'] == '' else int(
                row['LastUpdated'])

            ts = ts_msec // 1000
            last_update = datetime.datetime.utcfromtimestamp(ts)

            area = row['Province']
            # Do we have a total?
            # area for totals can be either blank or matching the country
            if area == '' or area.lower() == country.lower():
                if 'totals' in countries[country]:
                    self.log.warning('Duplicate totals for %s.', country)

                d = {'cases': cases, 'deaths': deaths,
                     'recoveries': recoveries, 'last_update': last_update}
                # TODO take the max for each value
                countries[country]['totals'] = d
            else:  # or an area?
                d = {'cases': cases, 'deaths': deaths,
                     'recoveries': recoveries, 'last_update': last_update}
                countries[country]['areas'][area] = d

        return countries

    async def _get_finland(self):
        districts = {}

        self.log.debug("Fetching %s.", UK_URL)
        async with self.http.get(FINLAND_URL) as r:
            j = await r.json()

        for case in j['confirmed']:
            d = case['healthCareDistrict']
            if d == None or d == '': # skip missing data
                continue

            if d not in districts:
                districts[d] = 1
            else:
                districts[d] += 1

        return districts

    def _update_index(self):
        # create a new index
        d = '/tmp/covbotindex'
        self.log.debug('Updating index in %s.', d)
        if not os.path.exists(d):
            os.mkdir(d)

        self.index = create_in(d, SCHEMA)
        idx_w = self.index.writer()

        # add all the documents
        for c, c_data in self.cases.items():
            # TODO should this be conditional on a record existing?
            idx_w.add_document(country=c, location=c)
            for a in c_data['areas']:
                l = f'{a}, {c}'
                idx_w.add_document(country=c, area=a, location=l)

        idx_w.commit()

    async def update(self):
        now = datetime.datetime.utcfromtimestamp(int(time.time()))

        self.log.info('Updating data.')
        # offloop, nhs, uk, finland = await asyncio.gather(self._get_offloop_cases(), self._get_nhs(), self._get_uk(), self._get_finland())
        offloop, finland, uk_countries, scottish_regions = await asyncio.gather(self._get_offloop_cases(), self._get_finland(), self._get_uk_countries(), self._get_scottish_regions())

        # TODO take the max value
        # for area, cases in nhs.items():
        #     offloop['United Kingdom']['areas'][area] = {
        #         'cases': cases, 'last_update': now}
        # for area, cases in uk.items():
        #     offloop['United Kingdom']['areas'][area] = {
        #         'cases': cases, 'last_update': now}
        for area, cases in finland.items():
            offloop['Finland']['areas'][area] = {
                'cases': cases, 'last_update': now}

        for r, ukdata in uk_countries.items():
            offloop['United Kingdom']['areas'][r] = ukdata

        for r, scodata in scottish_regions.items():
            offloop['United Kingdom']['areas'][r] = scodata

        self.cases = offloop
        await asyncio.get_running_loop().run_in_executor(None, self._update_index)

    def _exact_country_code_match(self, query: str) -> list:
        self.log.debug('Trying an exact country code match on %s.', query)
        cc = query.upper()

        # TODO generalise.
        # Handle UK alias.
        if cc == 'UK':
            cc = 'GB'

        c = pycountry.countries.get(
            alpha_2=cc) or pycountry.countries.get(alpha_3=cc)
        if c != None:
            self.log.debug('Country code %s is %s.', cc, c.name)

            if c.name not in self.cases:
                self.log.warn('No data for %s.', c.name)
                return None

            d = self.cases[c.name]

            if not 'totals' in d:
                self.log.debug('No totals found for %s.', c.name)
                return None

            return [(c.name, d['totals'])]

        return None

    def _exact_country_match(self, query: str) -> list:
        self.log.debug('Trying an exact country match on %s.', query)
        for country in self.cases:
            if country.lower() == query.lower():
                self.log.debug('Got an exact country match on %s.', query)

                if 'totals' not in self.cases[country]:
                    self.log.debug('No totals found for %s.', country)
                    return None

                return [(country, self.cases[country]['totals'])]

        return None

    def _exact_region_match(self, query: str) -> list:
        self.log.debug('Trying an exact region match on %s.', query)
        regions = []
        for country, data in self.cases.items():
            for area, data in data['areas'].items():
                if area.lower() == query.lower():
                    regions.append((f'{area}, {country}', data))

        if len(regions) > 0:
            self.log.debug(
                'Got exact region matches on %s: %s.', query, regions)

        return regions

    def _wildcard_location_match(self, query: str) -> list:
        self.log.debug('Trying a wildcard location match on %s.', query)
        with self.index.searcher() as s:
            qs = f'*{query}*'
            q = QueryParser("location", SCHEMA).parse(qs)
            matches = s.search(q, limit=None)

            locs = []
            for m in matches:
                c, l = m['country'], m['location']

                if 'area' in m:
                    d = self.cases[c]['areas'][m['area']]
                else:
                    d = self.cases[c]['totals']

                locs.append((l, d))

            if len(locs) > 0:
                self.log.debug(
                    'Found wildcard location matches on %s: %s.', query, locs)

            return locs

    def get(self, query: str) -> list:
        self.log.info('Looking up data for %s.', query)

        m = self._exact_country_code_match(query)
        if m != None:
            return m

        m = self._exact_country_match(query)
        if m != None:
            return m

        areas = self._exact_region_match(query)
        if len(areas) > 0:
            return areas

        locs = self._wildcard_location_match(query)
        if len(locs) > 0:
            return locs

        return []

    def get_mult(self, *queries: list) -> list:
        return [self.get(q) for q in queries]

    @classmethod
    def get_sources(cls) -> str:
        return f"{OFFLOOP_CASES_URL}, {NHS_URL}, {UK_URL} and {FINLAND_URL}"
