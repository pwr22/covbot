from mautrix.types import EventType
from maubot import Plugin, MessageEvent
from maubot.handlers import event, command

import os
import csv
import requests
import pprint
import datetime
import whoosh
from whoosh.fields import Schema, TEXT
from whoosh.index import create_in, FileIndex
from whoosh.qparser import QueryParser


class CovBot(Plugin):
    groups = {}
    cases = {}
    next_update_at: datetime.datetime = None
    schema: Schema = Schema(country=TEXT(stored=True), area=TEXT(stored=True))
    index: FileIndex = None

    @staticmethod
    def _get_country_groups():
        groups = {}

        r = requests.get('https://offloop.net/covid19h/groups.txt')
        # group;country_1;country_2 ...
        cr = csv.reader(r.content.decode('utf-8').splitlines(), delimiter=';')
        for group, *areas in cr:
            groups[group] = areas

        return groups

    def _get_case_data(self):
        countries = {}

        d = '/tmp/covbotindex'
        if not os.path.exists(d):
            os.mkdir(d)
        self.index = create_in(d, self.schema)
        i_writer = self.index.writer()

        r = requests.get('http://offloop.net/covid19h/unconfirmed.csv')
        # Country;Province;Confirmed;Deaths;Recovered;LastUpdated
        cr = csv.DictReader(r.content.decode(
            'utf-8').splitlines(), delimiter=';')
        for row in cr:
            country = row['Country']
            if not country in countries:
                countries[country] = {'totals': {
                    'cases': 0, 'recoveries': 0, 'deaths': 0}, 'areas': {}}

            cases, deaths, recoveries = (int(n) for n in (
                row[k] for k in ('Confirmed', 'Deaths', 'Recovered')))

            epoch = int(int(row['LastUpdated']) / 1000)
            last_update = datetime.datetime.utcfromtimestamp(epoch)

            area = row['Province']
            if area == '':
                countries[country]['totals'] = {
                    'cases': cases, 'deaths': deaths, 'recoveries': recoveries, 'last_update': last_update}
                i_writer.add_document(country=country)
            else:
                countries[country]['areas'][area] = {
                    'cases': cases, 'deaths': deaths, 'recoveries': recoveries, 'last_update': last_update}
                i_writer.add_document(country=country, area=area)

        i_writer.commit()
        return countries

    def _update_data(self):
        now = datetime.datetime.utcnow()

        if self.next_update_at == None or now >= self.next_update_at:
            self.log.info('updating data')
            self.groups = self._get_country_groups()
            self.cases = self._get_case_data()
            self.next_update_at = now + datetime.timedelta(minutes=15)
        else:
            self.log.info('too early to update - using cached data')

    def _get_data_for(self, location: str) -> (str, dict):
        lc_loc = location.lower()

        # try exact country match
        for country in self.cases:
            if country.lower() == lc_loc:
                return ((country, self.cases[country]['totals']),)

        # try exact area match
        areas = []
        for country, d in self.cases.items():
            for area, d in d['areas'].items():
                if area.lower() == lc_loc:
                    areas.append((f'{area}, {country}', d))

        if len(areas) > 0:
            return areas

        # try wildcard country match
        with self.index.searcher() as s:
            qs = f'*{location}*'
            q = QueryParser("country", self.schema).parse(qs)
            matches = s.search(q)

            countries = tuple((m['country'], self.cases[m['country']]['totals']) for m in matches)

            if len(countries) > 0:
                return countries

        # try wildcard area match
        with self.index.searcher() as s:
            qs = f'*{location}*'
            q = QueryParser("area", self.schema).parse(qs)
            matches = s.search(q)

            countries = tuple((m['area'], self.cases[m['country']]['areas'][m['area']]) for m in matches)

            if len(countries) > 0:
                return countries

        return ()

    @command.new('cases', help='Get information on cases')
    @command.argument("location", pass_raw=True, required=False)
    async def cases_handler(self, event: MessageEvent, location: str) -> None:
        if location == "":
            location = "World"

        self._update_data()
        matches = self._get_data_for(location)

        if len(matches) == 0:
            await event.respond(f'I have no data on {location} or there are no cases. If you can try a less specific location and if you are sure I am wrong then pester @pwr22:shortestpath.dev! (fuzzy matching is pretty bad at the moment - will be improved soon)')
            return
        elif len(matches) > 1:
            ms = " - ".join(m[0] for m in matches)
            await event.respond(f"Which of these did you mean? {ms}")
            return

        m_loc, data = matches[0]
        cases, recoveries, deaths, last_update = data['cases'], data[
            'recoveries'], data['deaths'], data['last_update']
        recovered = 0 if cases == 0 else int(recoveries) / int(cases) * 100
        dead = 0 if cases == 0 else int(deaths) / int(cases) * 100
        sick = 100 - recovered - dead
        s = f'In {m_loc} there have been a total of {cases} cases as of {last_update} UTC.'
        s += f' Of these {sick:.1f}% are still sick or may have recovered without being recorded, {recovered:.1f}% have definitely recovered and {dead:.1f}% have died.'
        # TODO put data source info somewhere else - auto-expansion of URLs can make these messages consume a lot of space
        # s += f' Check out https://offloop.net/covid19/ for graphs!'

        await event.respond(s)
