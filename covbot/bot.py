from mautrix.types import EventType
from maubot import Plugin, MessageEvent
from maubot.matrix import parse_formatted
from maubot.handlers import event, command
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

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

from .data import DataSource

RATE_LIMIT_BACKOFF_SECONDS = 10

# command: ( usage, description )
HELP = {
    'cases': (
        '!cases location',
        'Get up to date info on cases, optionally in a specific location.'
        ' You can give a country code, country, state, county, region or city.'
        ' E.g. !cases china'
    ),
    'compare': (
        '!compare locations',
        'Compare up to date info on cases in multiple locations.'
        'If it looks bad on mobile try rotating into landscape mode. '
        ' Separate the locations with semicolons (;).'
        ' You can give a country codes, countries, states, counties, regions or cities.'
        ' E.g. !compare cn;us;uk;it;de'
    ),
    'risk': ('!risk age', 'For a person of the given age, what is is the risk to them if they become sick with COVID-19?'),
    'source': ('!source', 'Find out about my data sources and developers.'),
    'help': ('!help', 'Get a reminder what I can do for you.'),
}


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("admins")


class CovBot(Plugin):
    next_update_at: datetime.datetime = None
    _rooms_joined = {}

    async def _handle_rate_limit(self, api_call_wrapper):
        while True:
            try:
                return await api_call_wrapper()
            except MLimitExceeded:
                self.log.warning(
                    'API rate limit exceepted so backing off for %s seconds.', RATE_LIMIT_BACKOFF_SECONDS)
                await asyncio.sleep(RATE_LIMIT_BACKOFF_SECONDS)
            except Exception as e:  # ignore other errors but give up
                self.log.warning('%s', e)
                return

    async def _prune_dead_rooms(self):
        while True:
            users = set()
            rooms = await self._handle_rate_limit(lambda: self.client.get_joined_rooms())
            self.log.info('I am in %s rooms.', len(rooms))

            for r in rooms:
                members = await self._handle_rate_limit(lambda: self.client.get_joined_members(r))

                if len(members) == 1:
                    self.log.debug('Leaving room %s since it is empty.', r)
                    await self._handle_rate_limit(lambda: self.client.leave_room(r))
                else:
                    for m in members:
                        users.add(m)

            self.log.info('I talk to %s unique users.', len(users))
            await asyncio.sleep(60 * 60 * 24)  # once per day

    @classmethod
    def get_config_class(cls) -> BaseProxyConfig:
        return Config

    async def start(self):
        await super().start()
        self.config.load_and_update()
        # So we can get room join events.
        self.client.add_dispatcher(MembershipEventDispatcher)
        self._room_prune_task = asyncio.create_task(self._prune_dead_rooms())
        self.data = DataSource(self.log, self.http)
        await self.data.update()

    async def stop(self):
        await super().stop()
        self.client.remove_dispatcher(MembershipEventDispatcher)
        self._room_prune_task.cancel()

    def _short_location(self, location: str, length=int(12)) -> str:
        """Returns a shortened location name.

        If exactly matches a country code, return that. (1)

        If shorter/equal than length, return intact. (2)

        Logic done in that order so that if someone passes a list
        of countries, they get the codes back, rather than a mix
        of codes and country names.

        If longer, split on commas and replace the final part with
        a country code if that matches. (3)

        If still too long, strip out 'middle' to get desired length.

        TODO: - consider stripping out ", City of,"
              - consider simple truncation

        Example (length=12):
            United States → US
            Manchester, GB → Manch..r ,GB
        """

        # Exact country case (1)
        try:
            return pycountry.countries.lookup(location).alpha_2
        except LookupError:
            pass

        # It fits already (2)
        if len(location) <= length:
            return location

        # If there's commas, try to replace the last bit with a
        # country code (3)
        if "," in location:
            loc_parts = [s.strip() for s in location.split(",")]
            if pycountry.countries.lookup(loc_parts[-1]):
                loc_parts[-1] = pycountry.countries.lookup(
                    loc_parts[-1]).alpha_2
            location = " ,".join(loc_parts)

        # If what we have is still longer, cut out the middle (4)
        if len(location) <= length:
            return location
        else:
            return "..".join([
                location[:int((length-2)/2)],
                location[-int((length-2)/2):]
            ])

    async def _locations_table(self, event: MessageEvent, data: dict,
                               tabletype=str("text"),
                               length=str("long")) -> str:
        """Build a table of locations to respond to.

        Uses tabulate module to tabulate data.

        Can be:
            - tabletype: text (default) or html
            - length: long (default), short or tiny

        Missing data (eg PHE) is handled and replaced
        by '---'; although this throws off tabulate's
        auto-alignment of numerical data.

        Tables by default report in following columns:
            - Location
            - Cases
            - Sick (%)
            - Recovered (%)
            - Deaths (%)

        Short table limits 'Location' to <= 12 chars
        and renames 'Recovered' to "matchesRec'd".

        Tiny table only outputs Loction and Cases columns.

        Table includes a 'Total' row, even where this makes
        no meaningful sense (eg countries + world data).
        """
        MISSINGDATA = "---"

        try:
            await self.data.update()
        except Exception as e:
            self.log.warn('Failed to update data: %s.', e)
            await event.respond("Something went wrong fetching "
                                "the latest data so stats may be outdated.")

        columns = ["Location", "Cases"]

        if [v for v in data.values() if "recoveries" in v]:
            # At least one of the results has recovery data
            columns.extend(["Recovered", "%"])

        if [v for v in data.values() if "deaths" in v]:
            # At least one of the results has deaths data
            columns.extend(["Deaths", "%"])

        if "Recovered" in columns and "Deaths" in columns:
            # L - C - S - R - D
            columns.insert(2, "Sick")
            columns.insert(3, "%")

        # TODO: sort by cases descending
        tabledata = []
        for location, data in data.items():
            rowdata = []
            cases = data['cases']

            # Location
            if length == "short":
                rowdata.extend([self._short_location(location)])
            else:
                rowdata.extend([location])

            # Cases
            rowdata.extend([f'{cases:,}'])

            # TODO: decide if eliding % columns
            if "recoveries" in data:
                recs = data['recoveries']
                per_rec = 0 if cases == 0 else \
                    int(recs) / int(cases) * 100

                rowdata.extend([f'{recs:,}', f"{per_rec:.1f}"])
            else:
                rowdata.extend([MISSINGDATA, MISSINGDATA])

            if "deaths" in data:
                deaths = data['deaths']
                per_dead = 0 if cases == 0 else \
                    int(deaths) / int(cases) * 100

                rowdata.extend([f'{deaths:,}', f"{per_dead:.1f}"])
            else:
                rowdata.extend([MISSINGDATA, MISSINGDATA])

            if "recoveries" in data and "deaths" in data:
                sick = cases - int(data['recoveries']) - data['deaths']
                per_sick = 100 - per_rec - per_dead

                rowdata.insert(2, f'{sick:,}')
                rowdata.insert(3, f"{per_sick:.1f}")
            else:
                rowdata.extend([MISSINGDATA, MISSINGDATA])

            # Trim data for which there are no columns
            rowdata = rowdata[:len(columns)]

            tabledata.append(rowdata)

        # Shorten columns if needed
        if length == "short":
            columns = [w.replace("Recovered", "Rec'd") for w in columns]
        # Minimal- cases only:
        if length == "tiny":
            columns = columns[:2]
            tabledata = [row[:2] for row in tabledata]

        # Build table
        if tabletype == "html":
            tablefmt = "html"
        else:
            tablefmt = "presto"

        table = tabulate(tabledata, headers=columns,
                         tablefmt=tablefmt, floatfmt=".1f")

        if data:
            return table

    async def _respond(self, e: MessageEvent, m: str) -> None:
        c = TextMessageEventContent(msgtype=MessageType.TEXT, body=m)
        await self._handle_rate_limit(lambda: e.respond(c))

    @staticmethod
    async def _respond_formatted(e: MessageEvent, m: str) -> None:
        """Respond with formatted message in m.text matrix format,
        not m.notice.

        This is needed as mobile clients (Riot 0.9.10, RiotX) currently
        do not seem to render markdown / HTML in m.notice events
        which are conventionally send by bots.

        Desktop/web Riot.im does render MD/HTML in m.notice, however.
        """
        c = TextMessageEventContent(
            msgtype=MessageType.TEXT, formatted_body=m, format="org.matrix.custom.html")
        c.body, c.formatted_body = parse_formatted(m, allow_html=True)
        await e.respond(c, markdown=True, allow_html=True)

    # source : https://www.desmos.com/calculator/v0zif7tflm
    @command.new('risk', help=HELP['risk'][1])
    @command.argument("age", pass_raw=True, required=True)
    async def risks_handler(self, event: MessageEvent, age: str) -> None:
        try:
            age = int(age)
        except ValueError:
            await self._respond(event, f'{age} does not look like a number to me.')
            return

        if age < 0 or age > 110:
            await self._respond(event, "The risk model only handles ages between 0 and 110.")
            return

        # Maths that Peter doesn't really understand!
        death_rate = max(0, -0.00186807 + 0.00000351867 *
                         age ** 2 + (2.7595 * 10 ** -15) * age ** 7)
        ic_rate = max(0, -0.0572602 - -0.0027617 * age)
        h_rate = max(0, -0.0730827 - age * -0.00628289)
        survival_rate = 1 - death_rate

        s = (
            f"I estimate a {age} year old patient sick with COVID-19 has a {survival_rate:.1%} chance of survival,"
            f" a {h_rate:.1%} likelihood of needing to go to hospital, a {ic_rate:.1%} risk of needing intensive care there"
            f" and a {death_rate:.1%} chance of death."
        )

        await self._respond(event, s)

    @command.new('cases', help=HELP['cases'][1])
    @command.argument("location", pass_raw=True, required=False)
    async def cases_handler(self, event: MessageEvent, location: str) -> None:
        if location == "":
            location = "World"

        self.log.info('Responding to cases request for %s.', location)

        try:
            await self.data.update()
        except Exception as e:
            self.log.warn('Failed to update data: %s.', e)
            await self._respond(event, 'Something went wrong fetching the latest data so stats may be outdated.')

        matches = self.data.get(location)

        if len(matches) == 0:
            await self._respond(
                event,
                f'My data doesn\'t seem to include {location}.'
                ' It might be under a different name, data on it might not be available or there could even be no cases.'
                ' You may have more luck if you try a less specific location, like the country it\'s in.'
                f' \n\nIf you think I should have data on it you can open an issue at https://github.com/pwr22/covbot/issues and Peter will take a look.'
            )
            return
        elif len(matches) > 5:
            await self._respond(event, f'I found a lot of matches for {location}. Please could you be more specific?')
            return
        elif len(matches) > 1:
            ms = "\n".join(m[0] for m in matches)
            await self._respond(event, f"Which of these did you mean?\n\n{ms}")
            return

        m_loc, data = matches[0]
        cases, last_update = data['cases'], data['last_update']
        s = f'In {m_loc} there have been a total of {cases:,} cases as of {last_update} UTC.'

        # some data is more detailed
        if 'recoveries' in data and 'deaths' in data:
            recoveries, deaths = data['recoveries'], data['deaths']
            sick = cases - recoveries - deaths

            per_rec = 0 if cases == 0 else int(recoveries) / int(cases) * 100
            per_dead = 0 if cases == 0 else int(deaths) / int(cases) * 100
            per_sick = 100 - per_rec - per_dead

            s += (
                f' Of these {sick:,} ({per_sick:.1f}%) are still sick or may have recovered without being recorded,'
                f' {recoveries:,} ({per_rec:.1f}%) have definitely recovered'
                f' and {deaths:,} ({per_dead:.1f}%) have died.'
            )

        await self._respond(
            event,
            s
        )

    @command.new('compare', help=HELP["compare"][1])
    @command.argument("locations", pass_raw=True, required=True)
    async def table_handler(self, event: MessageEvent, locations: str) -> None:
        self.log.info("Handling compare request")
        
        results = {}
        for loc in locations.split(";"):
            matches = self.data.get(loc)

            if len(matches) == 0:
                await self._respond(event,
                                    f"I cannot find a match for {loc}")
                return
            elif len(matches) > 5:
                await self._respond(event, f'I found a lot of matches for {loc}. Please could you be more specific?')
                return {}
            elif len(matches) > 1:
                ms = " - ".join(m[0] for m in matches)
                await self._respond(event,
                                    f"Multiple results for {loc}: {ms}. "
                                    "Please provide one.")
                return {}

            m = matches.pop()  # there's only one
            loc, data = m
            results[loc] = data

        
        t = await self._locations_table(event, data=results,
                                        tabletype="text",
                                        length="long")
        if t:
            await self._respond_formatted(event, f'<pre><code>{t}</code></pre>')

    @command.new('source', help=HELP['source'][1])
    async def source_handler(self, event: MessageEvent) -> None:
        self.log.info('Responding to source request.')
        await self._respond(
            event,
            'I was created by Peter Roberts and MIT licensed on Github at https://github.com/pwr22/covbot.'
            f' I fetch new data every 15 minutes from {CASE_DATA_URL}, {UK_NHS_REGIONS_URL} and {UK_REGIONS_URL}.'
            f' Risk estimates are based on the model at https://www.desmos.com/calculator/v0zif7tflm.'
        )

    @command.new('help', help=HELP['help'][1])
    async def help_handler(self, event: MessageEvent) -> None:
        self.log.info('Responding to help request.')

        s = 'You can message me any of these commands:\n\n'
        s += '\n\n'.join(f'{usage} - {desc}' for (usage,
                                                  desc) in HELP.values())
        await self._message(event.room_id, s)

    async def _message(self, room_id, m: str) -> None:
        c = TextMessageEventContent(msgtype=MessageType.TEXT, body=m)
        await self._handle_rate_limit(lambda: self.client.send_message(room_id=room_id, content=c))

    @command.new('announce', help='Send broadcast a message to all rooms.')
    @command.argument("message", pass_raw=True, required=True)
    async def accounce(self, event: MessageEvent, message: str) -> None:

        if event.sender not in self.config['admins']:
            self.log.warn(
                'User %s tried to send an announcement but only admins are authorised to do so.'
                ' They tried to send %s.',
                event.sender, message
            )
            await self._respond(event, 'You do not have permission to !announce.')
            return None

        rooms = await self._handle_rate_limit(lambda: self.client.get_joined_rooms())
        self.log.info('Sending announcement %s to all %s rooms',
                      message, len(rooms))

        for r in rooms:
            await self._message(r, message)

    @event.on(InternalEventType.JOIN)
    async def join_handler(self, event: InternalEventType.JOIN) -> None:
        me = await self._handle_rate_limit(lambda: self.client.whoami())

        # Ignore all joins but mine.
        if event.sender != me:
            return

        if event.room_id in self._rooms_joined:
            self.log.warning(
                'Duplicate join event for room %s.', event.room_id)
            return

        # work around duplicate joins
        self._rooms_joined[event.room_id] = True
        self.log.info(
            'Sending unsolicited help on join to room %s', event.room_id)

        s = 'Hi, I am a bot that tracks SARS-COV-2 infection statistics for you. You can message me any of these commands:\n\n'
        s += '\n'.join(f'{usage} - {desc}' for (usage, desc) in HELP.values())
        await self._message(event.room_id, s)
