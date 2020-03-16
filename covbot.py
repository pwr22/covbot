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

        self.log.debug("fetching %s", GROUPS_URL)
        async with self.http.get(GROUPS_URL) as r:
            t = await r.text()

            # group;country_1;country_2 ...
            cr = csv.reader(t.splitlines(), delimiter=';')
            for group, *areas in cr:
                groups[group] = areas

        return groups

    async def _get_case_data(self):
        countries = {}

        self.log.debug("fetching %s", CASE_DATA_URL)
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

            epoch = int(int(row['LastUpdated']) / 1000)
            last_update = datetime.datetime.utcfromtimestamp(epoch)

            area = row['Province']
            if area == '':
                countries[country]['totals'] = {
                    'cases': cases, 'deaths': deaths, 'recoveries': recoveries, 'last_update': last_update}
            else:
                countries[country]['areas'][area] = {
                    'cases': cases, 'deaths': deaths, 'recoveries': recoveries, 'last_update': last_update}

        return countries

    def _update_index(self):
        # create a new index
        d = '/tmp/covbotindex'
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
            self.log.info('updating data')
            self.groups, self.cases = await asyncio.gather(self._get_country_groups(), self._get_case_data())

            # one we've got the data we can update the index
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: self._update_index())

            self.next_update_at = now + datetime.timedelta(minutes=15)
        else:
            self.log.info('too early to update - using cached data')

    def _exact_country_match(self, query: str) -> list:
        for country in self.cases:
            if country.lower() == query.lower():
                return ((country, self.cases[country]['totals']),)

        return None

    def _exact_region_match(self, query: str) -> list:
        regions = []
        for country, data in self.cases.items():
            for area, data in data['areas'].items():
                if area.lower() == query.lower():
                    regions.append((f'{area}, {country}', data))

        return regions

    def _get_data_for(self, query: str) -> list:
        m = self._exact_country_match(query)
        if m != None:
            return m

        areas = self._exact_region_match(query)
        if len(areas) > 0:
            return areas

        # try wildcard location match
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
                return locs

        return []

    @command.new('cases', help='Get current information on cases.')
    @command.argument("location", pass_raw=True, required=False)
    async def cases_handler(self, event: MessageEvent, location: str) -> None:
        if location == "":
            location = "World"

        try:
            await self._update_data()
        except Exception as e:
            self.log.error('failed to update data: %s', e)
            await event.respond('Something went wrong fetching the latest data so stats may be outdated.')

        matches = await asyncio.get_running_loop().run_in_executor(None, self._get_data_for, location)

        if len(matches) == 0:
            await event.respond(f'I have searched my data but cannot find a match for {location}. It might be under a different name or there may be no cases! If I am wrong let @pwr22:shortestpath.dev know.')
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

        s = f'In {m_loc} there have been a total of {cases:,} cases as of {last_update} UTC.'
        s += f' Of these {sick:,} ({per_sick:.1f}%) are still sick or may have recovered without being recorded,'
        s += f' {recoveries:,} ({per_rec:.1f}%) have definitely recovered'
        s += f' and {deaths:,} ({per_dead:.1f}%) have died.'
        # TODO put data source info somewhere else - auto-expansion of URLs can make these messages consume a lot of space
        # s += f' Check out https://offloop.net/covid19/ for graphs!'

        await event.respond(s)

    @command.new('source', help='Get my source code and the data I use.')
    async def source_handler(self, event: MessageEvent) -> None:
        s = 'I am MIT licensed on Github at https://github.com/pwr22/covbot.'
        s += f' I fetch new data every 15 minutes from {CASE_DATA_URL}.'
        await event.respond(s)

    # TODO make less clever and one line per command
    @command.new('help', help='Get usage help using me.')
    async def help_handler(self, event: MessageEvent) -> None:
        for h in self.cases_handler, self.source_handler, self.help_handler:
            s = h.__mb_full_help__ + ' - ' + h.__mb_help__
            await event.respond(s)
