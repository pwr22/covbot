from mautrix.types import EventType
from maubot import Plugin, MessageEvent
from maubot.handlers import event, command

import os
import csv
import requests
import pprint
import datetime
import asyncio
import whoosh
import time
import pycountry
from whoosh.fields import Schema, TEXT
from whoosh.index import create_in, FileIndex
from whoosh.qparser import QueryParser

CASE_DATA_URL = 'http://offloop.net/covid19h/unconfirmed.csv'
GROUPS_URL = 'https://offloop.net/covid19h/groups.txt'

COUNTRY_RENAMES = {
    'US': 'United States',
    'DRC': 'Democratic Republic of the Congo',
    'UAE': 'United Arab Emirates',
    "U.S. Virgin Islands": "United States Virgin Islands"
}


class CovBot(Plugin):
    groups = {}
    cases = {}
    next_update_at: datetime.datetime = None
    schema: Schema = Schema(country=TEXT(stored=True), area=TEXT(
        stored=True), location=TEXT(stored=True))
    index: FileIndex = None

    async def _get_country_groups(self):
        groups = {}

        self.log.debug("Fetching %s.", GROUPS_URL)
        async with self.http.get(GROUPS_URL) as r:
            t = await r.text()

            # group;country_1;country_2 ...
            cr = csv.reader(t.splitlines(), delimiter=';')
            for group, *areas in cr:
                groups[group] = areas

        return groups

    async def _get_case_data(self):
        countries = {}
        now = time.time() * 1000  # millis to match the data

        self.log.debug("Fetching %s.", CASE_DATA_URL)
        l = None
        async with self.http.get(CASE_DATA_URL) as r:
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

            cases = 0 if row['Confirmed'] == '' else int(row['Confirmed'])
            deaths = 0 if row['Deaths'] == '' else int(row['Deaths'])
            recoveries = 0 if row['Recovered'] == '' else int(row['Recovered'])

            ts_msec = now if row['LastUpdated'] == '' else int(
                row['LastUpdated'])
            ts = ts_msec // 1000
            last_update = datetime.datetime.utcfromtimestamp(ts)

            area = row['Province']
            if area == '' or area.lower() == country.lower():
                if 'totals' in countries[country]:
                    self.log.warning('Duplicate totals for %s', country)

                d = {'cases': cases, 'deaths': deaths,
                     'recoveries': recoveries, 'last_update': last_update}
                countries[country]['totals'] = d
            else:
                d = {'cases': cases, 'deaths': deaths,
                     'recoveries': recoveries, 'last_update': last_update}
                countries[country]['areas'][area] = d

        return countries

    def _update_index(self):
        # create a new index
        d = '/tmp/covbotindex'
        self.log.debug('Updating index in %s', d)
        if not os.path.exists(d):
            os.mkdir(d)

        self.index = create_in(d, self.schema)
        idx_w = self.index.writer()

        # add all the documents
        for c, c_data in self.cases.items():
            # TODO should this be conditional on a record existing?
            idx_w.add_document(country=c, location=c)
            for a in c_data['areas']:
                l = f'{a}, {c}'
                idx_w.add_document(country=c, area=a, location=l)

        idx_w.commit()

    async def _update_data(self):
        now = datetime.datetime.utcnow()

        if self.next_update_at == None or now >= self.next_update_at:
            self.log.info('Updating data.')
            self.groups, self.cases = await asyncio.gather(self._get_country_groups(), self._get_case_data())
            await asyncio.get_running_loop().run_in_executor(None, self._update_index)
            self.next_update_at = now + datetime.timedelta(minutes=15)
        else:
            self.log.info('Too early to update - using cached data.')

    def _exact_country_code_match(self, query: str) -> list:
        self.log.info('Trying an exact country code match on %s', query)
        cc = query.upper()
        # Handle UK alias.
        if cc == 'UK': 
            cc = 'GB'

        c = pycountry.countries.get(alpha_2=cc) or pycountry.countries.get(alpha_3=cc)
        if c != None:
            self.log.info('%s is %s', cc, c.name)

            if c.name not in self.cases:
                self.log.warn('No data for %s', c.name)
                return None

            d = self.cases[c.name]

            if not 'totals' in d:
                self.log.debug('No totals found for %s', c.name)
                return None

            return ((c.name, d['totals']),)

        return None

    def _exact_country_match(self, query: str) -> list:
        self.log.info('Trying an exact country match on %s', query)
        for country in self.cases:
            if country.lower() == query.lower():
                self.log.debug('Got an exact country match on %s', query)

                if 'totals' not in self.cases[country]:
                    self.log.debug('No totals found for %s', country)
                    return None

                return ((country, self.cases[country]['totals']),)

        return None

    def _exact_region_match(self, query: str) -> list:
        self.log.info('Trying an exact region match on %s', query)
        regions = []
        for country, data in self.cases.items():
            for area, data in data['areas'].items():
                if area.lower() == query.lower():
                    regions.append((f'{area}, {country}', data))

        if len(regions) > 0:
            self.log.debug(
                'Got exact region matches on %s: %s', query, regions)

        return regions

    def _wildcard_location_match(self, query: str) -> list:
        self.log.info('Trying a wildcard location match on %s', query)
        with self.index.searcher() as s:
            qs = f'*{query}*'
            q = QueryParser("location", self.schema).parse(qs)
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
                    'Found wildcard location matches on %s: %s', query, locs)

            return locs

    def _get_data_for(self, query: str) -> list:
        self.log.info('Looking up data for %s', query)

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

    @command.new('cases', help='Get current information on cases.')
    @command.argument("location", pass_raw=True, required=False)
    async def cases_handler(self, event: MessageEvent, location: str) -> None:
        if location == "":
            location = "World"

        self.log.info('Responding to cases request for %s.', location)

        try:
            await self._update_data()
        except Exception as e:
            self.log.warn('Failed to update data: %s.', e)
            await event.respond('Something went wrong fetching the latest data so stats may be outdated.')

        matches = await asyncio.get_running_loop().run_in_executor(None, self._get_data_for, location)
        # matches = self._get_data_for(location)

        if len(matches) == 0:
            await event.respond(
                f'I have searched my data but cannot find a match for {location}.'
                ' It might be under a different name or there may be no cases!'
                ' If I am wrong let @pwr22:shortestpath.dev know.'
            )
            return
        elif len(matches) > 1:
            ms = " - ".join(m[0] for m in matches)
            await event.respond(f"Which of these did you mean? {ms}")
            return

        m_loc, data = matches[0]
        cases, recoveries, deaths, last_update = data['cases'], data[
            'recoveries'], data['deaths'], data['last_update']
        sick = cases - recoveries - deaths

        per_rec = 0 if cases == 0 else int(recoveries) / int(cases) * 100
        per_dead = 0 if cases == 0 else int(deaths) / int(cases) * 100
        per_sick = 100 - per_rec - per_dead

        await event.respond(
            f'In {m_loc} there have been a total of {cases:,} cases as of {last_update} UTC.'
            f' Of these {sick:,} ({per_sick:.1f}%) are still sick or may have recovered without being recorded,'
            f' {recoveries:,} ({per_rec:.1f}%) have definitely recovered'
            f' and {deaths:,} ({per_dead:.1f}%) have died.'
        )

    @command.new('table', help="Show case information in a table. "
                 "Multiple locations can be separated using ;"
                 "(semicolon) as a delimiter.")
    @command.argument("location", pass_raw=True, required=False)
    async def table_handler(self, event: MessageEvent, location: str) -> None:
        self.log.info("Handling table request")
        if location == "":
            location = "World"

        try:
            await self._update_data()
        except Exception as e:
            self.log.warn('Failed to update data: %s.', e)
            await event.respond("Something went wrong fetching "
                                "the latest data so stats may be outdated.")

        results = {}

        if ";" in location:
            locs = location.split(";")
            for loc in locs:
                self.log.info(f"Looking up {loc}")
                matches = await asyncio.get_running_loop().run_in_executor(
                    None, self._get_data_for, loc)
                if len(matches) == 0:
                    await event.respond(f"I cannot find a match for {loc}")
                elif len(matches) > 1:
                    ms = " - ".join(m[0] for m in matches)
                    await event.respond(f"Multiple results for {loc}: {ms}. "
                                        "Please provide one.")
                    return
                else:
                    # {"Elbonia": {}}
                    results[matches[0][0]] = matches[0][1]
        else:
            matches = await asyncio.get_running_loop().run_in_executor(
                None, self._get_data_for, location)
            if len(matches) == 0:
                await event.respond(f"I cannot find a match for {location}")
            elif len(matches) > 1:
                ms = " - ".join(m[0] for m in matches)
                await event.respond(f"Multiple results for {location}: {ms}. "
                                    "Please provide one.")
                return
            else:
                results[matches[0][0]] = matches[0][1]

        tablehead = ("<thead><tr><th>Location</th><th>Cases</th>"
                     "<th>Still Sick</th><th>%</th>"
                     "<th>Recoveries</th><th>%</th>"
                     "<th>Deaths</th><th>%</th></tr></thead>")
        tabledata = ""
        total_cases = total_sick = total_recoveries = total_deaths = 0
        for location, data in results.items():

            sick = data['cases'] - data['recoveries'] - data['deaths']
            per_rec = 0 if data['cases'] == 0 else \
                int(data['recoveries']) / int(data['cases']) * 100
            per_dead = 0 if data['cases'] == 0 else \
                int(data['deaths']) / int(data['cases']) * 100
            per_sick = 100 - per_rec - per_dead

            total_cases += data['cases']
            total_sick += sick
            total_recoveries += data['recoveries']
            total_deaths += data['deaths']

            tabledata += (f"<tr><td>{location}</td> <td>{data['cases']}</td> "
                          f"<td>{sick}</td><td>{per_sick:.1f}</td>"
                          f"<td>{data['recoveries']}</td>"
                          f"<td>{per_rec:.1f}</td>"
                          f"<td>{data['deaths']}</td>"
                          f"<td>{per_dead:.1f}</td></tr>")

        per_total_rec = 0 if total_cases == 0 else \
            int(total_recoveries) / int(total_cases) * 100
        per_total_dead = 0 if total_cases == 0 else \
            int(total_deaths) / int(total_cases) * 100
        per_total_sick = 100 - per_total_rec - per_total_dead

        tablefoot = (f"<tr></tr><tr><tfoot><td><em>Total</em></td>"
                     f"<td><em>{total_cases}</em></td>"
                     f"<td><em>{total_sick}</em></td>"
                     f"<td><em>{per_total_sick:.1f}</em></td>"
                     f"<td><em>{total_recoveries}</td>"
                     f"<td><em>{per_total_rec:.1f}</em></td>"
                     f"<td><em>{total_deaths}</td>"
                     f"<td><em>{per_total_dead:.1f}</em></td>"
                     "</tfoot></tr>")

        if results:
            await event.respond(f"<table>{tablehead}{tabledata}{tablefoot}"
                                "</table>", allow_html=True)


    @command.new('source', help='Get my source code and the data I use.')
    async def source_handler(self, event: MessageEvent) -> None:
        self.log.info('Responding to source request.')
        await event.respond(
            'I am MIT licensed on Github at https://github.com/pwr22/covbot.'
            f' I fetch new data every 15 minutes from {CASE_DATA_URL}.'
        )

    # TODO make less clever and one line per command
    @command.new('help', help='Get usage help using me.')
    async def help_handler(self, event: MessageEvent) -> None:
        self.log.info('Responding to help request.')
        for h in self.cases_handler, self.source_handler, self.help_handler, \
                self.table_handler:
            s = h.__mb_full_help__ + ' - ' + h.__mb_help__
            await event.respond(s)
