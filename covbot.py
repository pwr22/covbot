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
from whoosh.fields import Schema, TEXT
from whoosh.index import create_in, FileIndex
from whoosh.qparser import QueryParser

CASE_DATA_URL = 'http://offloop.net/covid19h/unconfirmed.csv'
GROUPS_URL = 'https://offloop.net/covid19h/groups.txt'


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

            if not country in countries:
                countries[country] = {'totals': {
                    'cases': 0, 'recoveries': 0, 'deaths': 0}, 'areas': {}}

            cases, deaths, recoveries = (0 if n == '' else int(n) for n in (
                row[k] for k in ('Confirmed', 'Deaths', 'Recovered')))

            ts = int(int(now if row['LastUpdated']
                         == '' else row['LastUpdated']) / 1000)
            last_update = datetime.datetime.utcfromtimestamp(ts)

            area = row['Province']
            if area == '':
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

    def _exact_country_match(self, query: str) -> list:
        self.log.info('Trying an exact country match on %s', query)
        for country in self.cases:
            if country.lower() == query.lower():
                self.log.debug('Got an exact country match on %s', query)
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
            self.log.debug('Got exact region matches on %s: %s', query, regions)

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
                self.log.debug('Found wildcard location matches on %s: %s', query, locs)

            return locs

    def _get_data_for(self, query: str) -> list:
        self.log.info('Looking up data for %s', query)

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
        for h in self.cases_handler, self.source_handler, self.help_handler:
            s = h.__mb_full_help__ + ' - ' + h.__mb_help__
            await event.respond(s)
