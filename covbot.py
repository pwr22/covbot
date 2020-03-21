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
from mautrix.types import TextMessageEventContent, MessageType
from mautrix.client import MembershipEventDispatcher, InternalEventType

CASE_DATA_URL = 'http://offloop.net/covid19h/unconfirmed.csv'
GROUPS_URL = 'https://offloop.net/covid19h/groups.txt'

COUNTRY_RENAMES = {
    'US': 'United States',
    'DRC': 'Democratic Republic of the Congo',
    'UAE': 'United Arab Emirates',
    "U.S. Virgin Islands": "United States Virgin Islands"
}

# command: ( usage, description )
HELP = {
    'cases': ('!cases [location]', 'Get current information on cases. Location is optional and can be a country, country code, state, county, region or city.'),
    'source': ('!source', 'Where are my source code and data sources?'),
    'help': ('!help', 'Get help using me!'),
}


class CovBot(Plugin):
    groups = {}
    cases = {}
    next_update_at: datetime.datetime = None
    schema: Schema = Schema(country=TEXT(stored=True), area=TEXT(
        stored=True), location=TEXT(stored=True))
    index: FileIndex = None

    async def _prune_dead_rooms(self):
        while True:
            rooms = await self.client.get_joined_rooms()
            self.log.debug('I am in %s rooms.', len(rooms))

            for r in rooms:
                members = await self.client.get_joined_members(r)

                if len(members) == 1:
                    self.log.debug('Leaving room %s since it is empty.', r)
                    await self.client.leave_room(r)
                    await asyncio.sleep(5)  # avoid throttling - no rush!

            await asyncio.sleep(300)

    async def start(self):
        await super().start()
        # So we can get room join events.
        self.client.add_dispatcher(MembershipEventDispatcher)
        self._room_prune_task = asyncio.create_task(self._prune_dead_rooms())

    async def stop(self):
        await super().start()
        self.client.remove_dispatcher(MembershipEventDispatcher)
        self._room_prune_task.cancel()

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

        c = pycountry.countries.get(
            alpha_2=cc) or pycountry.countries.get(alpha_3=cc)
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

    @staticmethod
    async def _respond(e: MessageEvent, m: str) -> None:
        c = TextMessageEventContent(msgtype=MessageType.TEXT, body=m)
        await e.respond(c)

    @command.new('cases', help=HELP['cases'][1])
    @command.argument("location", pass_raw=True, required=False)
    async def cases_handler(self, event: MessageEvent, location: str) -> None:
        if location == "":
            location = "World"

        self.log.info('Responding to cases request for %s.', location)

        try:
            await self._update_data()
        except Exception as e:
            self.log.warn('Failed to update data: %s.', e)
            await self._respond(event, 'Something went wrong fetching the latest data so stats may be outdated.')

        matches = await asyncio.get_running_loop().run_in_executor(None, self._get_data_for, location)
        # matches = self._get_data_for(location)

        if len(matches) == 0:
            await self._respond(
                event,
                f'I have searched my data but cannot find a match for {location}.'
                ' It might be under a different name or there may be no cases!'
                ' If I am wrong let @pwr22:shortestpath.dev know.'
            )
            return
        elif len(matches) > 1:
            ms = " - ".join(m[0] for m in matches)
            await self._respond(event, f"Which of these did you mean? {ms}")
            return

        m_loc, data = matches[0]
        cases, recoveries, deaths, last_update = data['cases'], data[
            'recoveries'], data['deaths'], data['last_update']
        sick = cases - recoveries - deaths

        per_rec = 0 if cases == 0 else int(recoveries) / int(cases) * 100
        per_dead = 0 if cases == 0 else int(deaths) / int(cases) * 100
        per_sick = 100 - per_rec - per_dead

        await self._respond(
            event,
            f'In {m_loc} there have been a total of {cases:,} cases as of {last_update} UTC.'
            f' Of these {sick:,} ({per_sick:.1f}%) are still sick or may have recovered without being recorded,'
            f' {recoveries:,} ({per_rec:.1f}%) have definitely recovered'
            f' and {deaths:,} ({per_dead:.1f}%) have died.'
        )

    @command.new('source', help=HELP['source'][1])
    async def source_handler(self, event: MessageEvent) -> None:
        self.log.info('Responding to source request.')
        await self._respond(
            event,
            'I am MIT licensed on Github at https://github.com/pwr22/covbot.'
            f' I fetch new data every 15 minutes from {CASE_DATA_URL}.'
        )

    async def _send_help(self, event: MessageEvent) -> None:
        await self._respond(event, 'You can send me any of these commands:')
        for (usage, desc) in HELP.values():
            await self._respond(event, f'{usage} - {desc}')

    @command.new('help', help=HELP['help'][1])
    async def help_handler(self, event: MessageEvent) -> None:
        self.log.info('Responding to help request.')

        await self._send_help(event)

    async def _message(self, room_id, m: str) -> None:
        c = TextMessageEventContent(msgtype=MessageType.TEXT, body=m)
        await self.client.send_message(room_id=room_id, content=c)

    _rooms_joined = {}

    @event.on(InternalEventType.JOIN)
    async def join_handler(self, event: InternalEventType.JOIN) -> None:
        me = await self.client.whoami()

        # Ignore all joins but mine.
        if event.sender != me:
            return

        if event.room_id in self._rooms_joined:
            self.log.warning('Duplicate join event for room %s.', event.room_id)
            return

        # work around duplicate joins
        self._rooms_joined[event.room_id] = True
        self.log.info(
            'Sending unsolicited help on join to room %s', event.room_id)

        await self._message(event.room_id, 'Hi, I am a bot that tracks SARS-COV-2 infection statistics.')
        await self._message(event.room_id, 'You can send me any of these commands:')
        for (usage, desc) in HELP.values():
            await self._message(event.room_id, f'{usage} - {desc}')
