from mautrix.types import EventType
from maubot import Plugin, MessageEvent
from maubot.handlers import event, command

import csv
import requests
import pprint
import datetime


class CovBot(Plugin):
    groups = {}
    cases = {}
    next_update_at: datetime.datetime = None

    @staticmethod
    def _get_country_groups():
        groups = {}

        r = requests.get('https://offloop.net/covid19h/groups.txt')
        # group;country_1;country_2 ...
        cr = csv.reader(r.content.decode('utf-8').splitlines(), delimiter=';')
        for group, *areas in cr:
            groups[group] = areas

        return groups

    @staticmethod
    def _get_case_data():
        countries = {}

        r = requests.get('http://offloop.net/covid19h/unconfirmed.csv')
        # Country;Province;Confirmed;Deaths;Recovered;LastUpdated
        cr = csv.DictReader(r.content.decode(
            'utf-8').splitlines(), delimiter=';')
        for row in cr:
            country = row['Country']
            if not country in countries:
                countries[country] = {'areas': {}}

            cases, deaths, recoveries = (int(n) for n in (row[k] for k in ('Confirmed', 'Deaths', 'Recovered')))

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
        # first check for an exact match on the country name
        for country, data in self.cases.items():
            if country.lower() == location.lower():
                return country, data['totals']

        # then check for an exact match on the area
        for country, data in self.cases.items():
            for area, data in data['areas'].items():
                if area.lower() == location.lower():
                    return f'{area}, {country}', data

        # then do a substring match on country
        for country, data in self.cases.items():
            if location.lower() in country.lower():
                return country, data['totals']

        # then do a substring match on area
        for country, data in self.cases.items():
            for area, data in data['areas'].items():
                if location.lower() in area.lower():
                    return f'{area}, {country}', data

        # then do a substring match on area + country
        for country, data in self.cases.items():
            for area, data in data['areas'].items():
                s = f'{area}, {country}'

                if location.lower() in s.lower():
                    return s, data

        return '', None

    @command.new('cases', help='Get information on cases')
    @command.argument("location", pass_raw=True, required=False)
    async def cases_handler(self, event: MessageEvent, location: str) -> None:
        if location == "":
            location = "World"

        self._update_data()
        match, data = self._get_data_for(location)

        if data == None:
            await event.respond(f'There are no cases in {location} - pester @pwr22:shortestpath.dev if you think this is wrong!')
            return

        cases, recoveries, deaths, last_update = data['cases'], data[
            'recoveries'], data['deaths'], data['last_update']
        recovered = 0 if cases == 0 else int(recoveries) / int(cases) * 100
        dead = 0 if cases == 0 else int(deaths) / int(cases) * 100
        sick = 100 - recovered - dead
        s = f'In {match} there have been a total of {cases} cases as of {last_update} UTC.'
        s += f' Of these {sick:.1f}% are still sick, {recovered:.1f}% have recovered and {dead:.1f}% have died.'
        # TODO put data source info somewhere else - auto-expansion of URLs can make these messages consume a lot of space
        # s += f' Check out https://offloop.net/covid19/ for graphs!'

        await event.respond(s)
