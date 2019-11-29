import logging
import pytz
import re
import requests
import requests_futures.sessions

from common.db_service import DbService
from datetime import datetime, timedelta
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Any, Dict, List, Optional

from traffic_violations.constants import L10N, regexps as regexp_constants, twitter as twitter_constants
from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.services.open_data_service import OpenDataService
from traffic_violations.services.tweet_detection_service import TweetDetectionService


class TrafficViolationsAggregator:

    MYSQL_TIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'

    def __init__(self, logger):
        self.logger = logger
        self.db_service = DbService(logger)
        self.tweet_detection_service = TweetDetectionService(logger=self.logger)

    def detect_campaign_hashtags(self, string_tokens):
        # Instantiate connection.
        conn = self.db_service.get_connection()

        # Look for campaign hashtags in the message's text.
        campaigns_present = conn.execute(""" select id, hashtag from campaigns where hashtag in (%s) """ % ','.join(
            ['%s'] * len(string_tokens)), [regexp_constants.HASHTAG_PATTERN.sub('', string) for string in string_tokens])
        result = [tuple(i) for i in campaigns_present.cursor]

        # Close the connection
        conn.close()

        return result

    def detect_plate_types(self, plate_types_input):
        plate_types_pattern = re.compile(regexp_constants.REGISTRATION_TYPES_REGEX)

        if ',' in plate_types_input:
            parts = plate_types_input.upper().split(',')
            return any([plate_types_pattern.search(part) != None for part in parts])
        else:
            return plate_types_pattern.search(plate_types_input.upper()) != None

    def detect_state(self, state_input):
        # or state_full_pattern.search(state_input.upper()) != None
        if state_input is not None:
            return regexp_constants.STATE_ABBR_PATTERN.search(state_input.upper()) != None

        return False

    def find_potential_vehicles(self, list_of_strings):

        # Use new logic of '<state>:<plate>'
        plate_tuples = [[part.strip() for part in match.split(':')] for match in re.findall(regexp_constants.PLATE_FORMAT_REGEX, ' '.join(
            list_of_strings)) if all(substr not in match.lower() for substr in ['://', 'state:', 'plate:'])]

        return self.infer_plate_and_state_data(plate_tuples)

    def find_potential_vehicles_using_legacy_logic(self, list_of_strings):

        # Find potential plates

        # Use old logic of 'state:<state> plate:<plate>'
        potential_vehicles = []
        legacy_plate_data = dict([[piece.strip() for piece in match.split(':')] for match in [part.lower(
        ) for part in list_of_strings if ('state:' in part.lower() or 'plate:' in part.lower() or 'types:' in part.lower())]])

        if legacy_plate_data:
            if self.detect_state(legacy_plate_data.get('state')) and legacy_plate_data.get('plate'):
                legacy_plate_data['valid_plate'] = True
            else:
                legacy_plate_data['valid_plate'] = False

            potential_vehicles.append(legacy_plate_data)

        return potential_vehicles

    def form_campaign_lookup_response_parts(self, query_result, username):

        campaign_chunks = []
        campaign_string = ""

        for campaign in query_result['included_campaigns']:
            num_vehicles = campaign['campaign_vehicles']
            num_tickets = campaign['campaign_tickets']

            next_string_part = (
                f"{num_vehicles} {'vehicle with' if num_vehicles == 1 else 'vehicles with a total of'} "
                f"{num_tickets} ticket{L10N.pluralize(num_tickets)} {'has' if num_vehicles == 1 else 'have'} "
                f"been tagged with { campaign['campaign_hashtag']}.\n\n")

            # how long would it be
            potential_response_length = len(
                username + ' ' + campaign_string + next_string_part)

            if (potential_response_length <= twitter_constants.MAX_TWITTER_STATUS_LENGTH):
                campaign_string += next_string_part
            else:
                campaign_chunks.append(campaign_string)
                campaign_string = next_string_part

        # Get any part of string left over
        campaign_chunks.append(campaign_string)

        return campaign_chunks

    def form_plate_lookup_response_parts(
          self,
          frequency: int,
          plate: str,
          plate_types: List[str],
          state: str,
          username: str,
          violations: Dict[str, Any],
          borough_data: Optional[Dict[str, Any]] = None,
          camera_streak_data: Optional[Dict[str, Any]] = None,
          fine_data: Optional[Dict[str, Any]] = None,
          previous_lookup: Optional[Dict[str, Any]] = None,
          year_data: Optional[Dict[str, Any]] = None):

        # response_chunks holds tweet-length-sized parts of the response
        # to be tweeted out or appended into a single direct message.
        response_chunks: List[str] = []
        violations_string: str = ""

        # Get total violations
        total_violations: int = sum([s['count']
                                for s in violations])
        self.logger.debug("total_violations: %s", total_violations)

        # Append to initially blank string to build tweet.
        violations_string += L10N.LOOKUP_SUMMARY_STRING.format(
            L10N.VEHICLE_HASHTAG.format(state, plate),
            L10N.get_plate_types_string(plate_types),
            frequency,
            L10N.pluralize(int(frequency)))

        # If this vehicle has been queried before...
        if previous_lookup:
            previous_num_violations: int = previous_lookup.num_tickets

            violations_string += self._create_repeat_lookup_string(
                new_violations=(total_violations - previous_num_violations),
                plate=plate,
                previous_lookup=previous_lookup,
                state=state,
                username=username)


        response_chunks += self.handle_response_part_formation(
            collection=violations,
            continued_format_string=L10N.LOOKUP_TICKETS_STRING_CONTD.format(L10N.VEHICLE_HASHTAG.format(state, plate)),
            count='count',
            cur_string=violations_string,
            description='title',
            default_description='No Year Available',
            prefix_format_string=L10N.LOOKUP_TICKETS_STRING.format(total_violations),
            result_format_string=L10N.LOOKUP_RESULTS_DETAIL_STRING,
            username=username)

        if year_data:
            response_chunks += self.handle_response_part_formation(
                collection=year_data,
                continued_format_string=L10N.LOOKUP_YEAR_STRING_CONTD.format(L10N.VEHICLE_HASHTAG.format(state, plate)),
                count='count',
                description='title',
                default_description='No Year Available',
                prefix_format_string=L10N.LOOKUP_YEAR_STRING.format(L10N.VEHICLE_HASHTAG.format(state, plate)),
                result_format_string=L10N.LOOKUP_RESULTS_DETAIL_STRING,
                username=username)

        if borough_data:
            response_chunks += self.handle_response_part_formation(
                collection=borough_data,
                continued_format_string=L10N.LOOKUP_BOROUGH_STRING_CONTD.format(L10N.VEHICLE_HASHTAG.format(state, plate)),
                count='count',
                description='title',
                default_description='No Borough Available',
                prefix_format_string=L10N.LOOKUP_BOROUGH_STRING.format(L10N.VEHICLE_HASHTAG.format(state, plate)),
                result_format_string=L10N.LOOKUP_RESULTS_DETAIL_STRING,
                username=username)

        if fine_data and any([k[1] != 0 for k in fine_data]):

            cur_string = f"Known fines for {L10N.VEHICLE_HASHTAG.format(state, plate)}:\n\n"

            max_count_length = len('${:,.2f}'.format(max(t[1] for t in fine_data)))
            spaces_needed = (max_count_length * 2) + 1

            for fine_type, amount in fine_data:

                currency_string = '${:,.2f}'.format(amount)
                count_length = len(str(currency_string))

                # e.g., if spaces_needed is 5, and count_length is 2, we need
                # to pad to 3.
                left_justify_amount = spaces_needed - count_length

                # formulate next string part
                next_part = (
                    f"{currency_string.ljust(left_justify_amount)}| "
                    f"{fine_type.replace('_', ' ').title()}\n")

                # determine current string length if necessary
                potential_response_length = len(
                    username + ' ' + cur_string + next_part)

                # If username, space, violation string so far and new part are less or
                # equal than 280 characters, append to existing tweet string.
                if (potential_response_length <= twitter_constants.MAX_TWITTER_STATUS_LENGTH):
                    cur_string += next_part
                else:
                    response_chunks.append(cur_string)

                    cur_string = "Known fines for #{}_{}, cont'd:\n\n"
                    cur_string += next_part

            # add to container
            response_chunks.append(cur_string)

        if camera_streak_data:

            if camera_streak_data.get('max_streak') and camera_streak_data['max_streak'] >= 5:

                # formulate streak string
                streak_string = (
                    f"Under @bradlander's proposed legislation, "
                    f"this vehicle could have been booted or impounded "
                    f"due to its {camera_streak_data['max_streak']} camera violations "
                    f"(>= 5/year) from {camera_streak_data['min_streak_date']}"
                    f" to {camera_streak_data['max_streak_date']}.\n")

                # add to container
                response_chunks.append(streak_string)

        # Send it back!
        return response_chunks

    def form_summary_string(self, summary, username):
        return [
            f"The {summary['vehicles']} vehicles you queried have collectively received "
            f"{summary['tickets']} ticket{L10N.pluralize(summary['tickets'])} with at "
            f"least {'${:,.2f}'.format(summary['fines']['fined'] - summary['fines']['reduced'])} "
            f"in fines, of which {'${:,.2f}'.format(summary['fines']['paid'])} has been paid.\n\n"]

    def get_plate_lookup(self, args: List):
        # Grab plate and plate from args.

        created_at: Optional[str] = datetime.strptime(args['created_at'], twitter_constants.TWITTER_TIME_FORMAT).strftime(self.MYSQL_TIME_FORMAT
            ) if 'created_at' in args else None
        message_id: Optional[str] = args['message_id'] if 'message_id' in args else None
        message_type: str = args['message_type']

        plate: str = regexp_constants.PLATE_PATTERN.sub('', args['plate'].strip().upper())
        state: str = args['state'].strip().upper()

        plate_types: str = ','.join(sorted([type for type in args['plate_types'].split(
            ',')])) if args.get('plate_types') is not None else None

        username: str = re.sub('@', '', args['username'])

        self.logger.debug('Listing args... plate: %s, state: %s, message_id: %s, created_at: %s',
                          plate, state, str(message_id), str(created_at))

        return PlateLookup(created_at=created_at,
          message_id=message_id,
          message_type=message_type,
          plate=plate,
          plate_types=plate_types,
          state=state,
          username=username)

    def handle_response_part_formation(self,
          count: str,
          collection: Dict[str, Any],
          continued_format_string: str,
          description: str,
          default_description: str,
          prefix_format_string: str,
          result_format_string: str,
          username: str,
          cur_string: str = None):

        # collect the responses
        response_container = []

        cur_string = cur_string if cur_string else ''

        if prefix_format_string:
            cur_string += prefix_format_string

        max_count_length = len(
            str(max(item[count] for item in collection)))
        spaces_needed = (max_count_length * 2) + 1

        # Grab item
        for item in collection:

            # Titleize for readability.
            violation_description = item[description].title()

            # Use a default description if need be
            if len(violation_description) == 0:
                violation_description = default_description

            violation_count = item[count]
            count_length = len(str(violation_count))

            # e.g., if spaces_needed is 5, and count_length is 2, we need to
            # pad to 3.
            left_justify_amount = spaces_needed - count_length

            # formulate next string part
            next_part = result_format_string.format(
                str(violation_count).ljust(left_justify_amount), violation_description)

            # determine current string length
            potential_response_length = len(f'{username} {cur_string}{next_part}')

            # If username, space, violation string so far and new part are less or
            # equal than 280 characters, append to existing tweet string.
            if (potential_response_length <= twitter_constants.MAX_TWITTER_STATUS_LENGTH):
                cur_string += next_part
            else:
                response_container.append(cur_string)
                if continued_format_string:
                    cur_string = continued_format_string
                else:
                    cur_string = ''

                cur_string += next_part

        # If we finish the list with a non-empty string,
        # append that string to response parts
        if len(cur_string) != 0:
            # Append ready string into parts for response.
            response_container.append(cur_string)

        # Return parts
        return response_container

    def infer_plate_and_state_data(self, list_of_vehicle_tuples):
        plate_data = []

        for vehicle_tuple in list_of_vehicle_tuples:
            this_plate = {'original_string': ':'.join(
                vehicle_tuple), 'valid_plate': False}

            if len(vehicle_tuple) in range(2, 4):
                state_bools = [self.detect_state(
                    part) for part in vehicle_tuple]
                try:
                    state_index = state_bools.index(True)
                except ValueError:
                    state_index = None

                plate_types_bools = [self.detect_plate_types(
                    part) for part in vehicle_tuple]
                try:
                    plate_types_index = plate_types_bools.index(True)
                except ValueError:
                    plate_types_index = None

                have_valid_plate = (len(vehicle_tuple) == 2 and state_index is not None) or (
                    len(vehicle_tuple) == 3 and None not in [plate_types_index, state_index])

                if have_valid_plate:
                    non_state_plate_types_parts = [x for x in list(range(0, len(vehicle_tuple))) if x not in [
                        plate_types_index, state_index]]

                    plate_index = None

                    # We have a tuple with state and plate, and possibly plate
                    # types
                    if non_state_plate_types_parts:

                        plate_index = non_state_plate_types_parts[0]

                    # We don't seem to have a plate, which means the plate
                    # types might be the plate
                    elif plate_types_index is not None:

                        alphanumeric_only = re.match(
                            '^[\w-]+$', vehicle_tuple[plate_types_index]) is not None

                        if alphanumeric_only:
                            plate_index = plate_types_index
                            plate_types_index = None

                    # Put plate data together
                    if plate_index is not None and vehicle_tuple[plate_index] != '':
                        this_plate['plate'] = vehicle_tuple[plate_index]
                        this_plate['state'] = vehicle_tuple[state_index]

                        if plate_types_index is not None:
                            this_plate['types'] = vehicle_tuple[
                                plate_types_index]

                        this_plate['valid_plate'] = True

            plate_data.append(this_plate)

        return plate_data

    def initiate_reply(self, lookup_request):
        self.logger.info('\n')
        self.logger.info('Calling initiate_reply')

        if lookup_request.requires_response():
            return self.create_response(lookup_request)

    def perform_campaign_lookup(self, included_campaigns):

        self.logger.debug('Performing lookup for campaigns.')

        # Instantiate connection.
        conn = self.db_service.get_connection()

        result = {'included_campaigns': []}

        for campaign in included_campaigns:
            # get new total for tickets
            campaign_tickets_query_string = """
              select count(id) as campaign_vehicles,
                     ifnull(sum(num_tickets), 0) as campaign_tickets
                from plate_lookups t1
               where (plate, state)
                  in
                (select plate, state
                  from campaigns_plate_lookups cpl
                  join plate_lookups t2
                    on t2.id = cpl.plate_lookup_id
                 where campaign_id = %s)
                 and t1.created_at =
                  (select MAX(t3.created_at)
                     from plate_lookups t3
                    where t3.plate = t1.plate
                      and t3.state = t1.state
                      and count_towards_frequency = 1);
            """

            campaign_tickets_result = conn.execute(
                campaign_tickets_query_string.replace('\n', ''), (campaign[0])).fetchone()
            # return data
            # result['included_campaigns'].append((campaign[1], int(campaign_tickets), int(num_vehicles)))
            result['included_campaigns'].append({'campaign_hashtag': campaign[1], 'campaign_tickets': int(
                campaign_tickets_result[1]), 'campaign_vehicles': int(campaign_tickets_result[0])})

        # Close the connection
        conn.close()

        return result

    def perform_plate_lookup(self, args):

        self.logger.debug('Performing lookup for plate.')

        plate_lookup: PlateLookup = self.get_plate_lookup(args)

        nyc_open_data_service = OpenDataService(logger=self.logger)
        result = nyc_open_data_service.lookup_vehicle(plate_lookup=plate_lookup)

        self.logger.debug(f'Violation data: {result}')

        previous_lookup: Optional[PlateLookup] = self.query_for_previous_lookup(plate_lookup=plate_lookup)

        # if we have a previous lookup, add it to the return data.
        if previous_lookup:
            result['previous_result'] = previous_lookup

            self.logger.debug('Previous lookups for this vehicle exists: %s', previous_lookup)


        current_frequency = self.query_for_lookup_frequency(plate_lookup=plate_lookup)

        # how many times have we searched for this plate from a tweet
        result['frequency'] = current_frequency + 1

        self.save_plate_lookup(campaigns=args['included_campaigns'],
            plate_lookup=plate_lookup, result=result)

        self.logger.debug('Returned_result: %s', result)

        return result

    def query_for_lookup_frequency(self, plate_lookup) -> int:
        # Instantiate connection.
        conn = self.db_service.get_connection()

        # Find the number of times we have seen this vehicle before.
        if plate_lookup.plate_types:
            current_frequency = conn.execute(
                """ select count(*) as lookup_frequency from plate_lookups where plate = %s and state = %s and plate_types = %s and count_towards_frequency = %s """, (plate_lookup.plate, plate_lookup.state, plate_lookup.plate_types, True)).fetchone()[0]
        else:
            current_frequency = conn.execute(
                """ select count(*) as lookup_frequency from plate_lookups where plate = %s and state = %s and plate_types IS NULL and count_towards_frequency = %s """, (plate_lookup.plate, plate_lookup.state, True)).fetchone()[0]

        conn.close()

        return current_frequency

    def query_for_previous_lookup(self, plate_lookup) -> Optional[PlateLookup]:
        # See if we've seen this vehicle before.

        conn = self.db_service.get_connection()

        previous_lookup = None

        if plate_lookup.plate_types:
            previous_lookup = conn.execute(
                """ select created_at, external_username as username, lookup_source as message_type, message_id, num_tickets from plate_lookups where plate = %s and state = %s and plate_types = %s and count_towards_frequency = %s order by created_at desc limit 1 """, (plate_lookup.plate, plate_lookup.state, plate_lookup.plate_types, True))
        else:
            previous_lookup = conn.execute(
                """ select created_at, external_username as username, lookup_source as message_type, message_id, num_tickets from plate_lookups where plate = %s and state = %s and plate_types IS NULL and count_towards_frequency = %s order by created_at desc limit 1 """, (plate_lookup.plate, plate_lookup.state, True))

        # Turn data into list of dicts with attribute keys
        previous_data = [dict(zip(tuple(previous_lookup.keys()), i))
                         for i in previous_lookup.cursor][0]

        conn.close()

        if previous_data:
            return PlateLookup(created_at=previous_data['created_at'],
                message_id=previous_data['message_id'],
                message_type=previous_data['message_type'],
                num_tickets=previous_data['num_tickets'],
                username=previous_data['username'])
        else:
            return None

    def save_plate_lookup(self, campaigns, plate_lookup, result) -> None:
        conn = self.db_service.get_connection()

        camera_streak_data = result.get('camera_streak_data', {})

        # Default to counting everything.
        count_towards_frequency = 1

        # Calculate the number of violations.
        total_violations = result.get('num_violations')

        # If this came from message, add it to the plate_lookups table.
        if plate_lookup.message_type and plate_lookup.message_id and plate_lookup.created_at:
            # Insert plate lookupresult
            insert_lookup = conn.execute(""" insert into plate_lookups (plate, state, plate_types, observed, message_id, lookup_source, created_at, external_username, count_towards_frequency, num_tickets, boot_eligible, responded_to) values (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, 1) """, (
                plate_lookup.plate, plate_lookup.state, plate_lookup.plate_types, plate_lookup.message_id, plate_lookup.message_type, plate_lookup.created_at, plate_lookup.username, count_towards_frequency, total_violations, camera_streak_data.get('max_streak') >= 5 if camera_streak_data else False))

            # Iterate through included campaigns to tie lookup to each
            for campaign in campaigns:
                # insert join record for campaign lookup
                conn.execute(""" insert into campaigns_plate_lookups (campaign_id, plate_lookup_id) values (%s, %s) """, (campaign[
                             0], insert_lookup.lastrowid))

        conn.close()

    def _create_repeat_lookup_string(
        self,
        new_violations: int,
        plate: str,
        state: str,
        username: str,
        previous_lookup: Optional[Dict[str, Any]] = None):

        violations_string = ''

        if new_violations > 0:

            # assume we can't link
            can_link_tweet = False

            # Where did this come from?
            if previous_lookup.message_type == 'status':
                # Determine if tweet is still visible:
                username_for_url: str = re.sub('@', '', username)
                if self.tweet_detection_service.tweet_exists(id=previous_lookup.message_id,
                                                        username=username_for_url):
                    can_link_tweet = True

            # Determine when the last lookup was...
            previous_time = previous_lookup.created_at
            now = datetime.now()
            utc = pytz.timezone('UTC')
            eastern = pytz.timezone('US/Eastern')

            adjusted_time = utc.localize(previous_time)
            adjusted_now = utc.localize(now)

            # If at least five minutes have passed...
            if adjusted_now - timedelta(minutes=5) > adjusted_time:

                # Add the new ticket info and previous lookup time to the string.
                violations_string += L10N.LAST_QUERIED_STRING.format(
                    adjusted_time.astimezone(eastern).strftime('%B %-d, %Y'),
                    adjusted_time.astimezone(eastern).strftime('%I:%M%p'))

                if can_link_tweet:
                    violations_string += L10N.PREVIOUS_LOOKUP_STATUS_STRING.format(
                        previous_lookup.username,
                        previous_lookup.username,
                        previous_lookup.message_id)
                else:
                    violations_string += '.'

                violations_string += L10N.REPEAT_LOOKUP_STRING.format(
                    L10N.VEHICLE_HASHTAG.format(state, plate),
                    new_violations,
                    L10N.pluralize(new_violations))

        return violations_string

    def create_response(self, request_object):

        self.logger.info('\n')
        self.logger.info("Calling create_response")

        # Print args
        self.logger.info('args:')
        self.logger.info('request_object: %s', request_object)

        # Grab string parts
        self.logger.debug('string_tokens: %s', request_object.string_tokens())

        # Collect response parts here.
        response_parts = []
        successful_lookup = False
        error_on_lookup = False

        # Wrap in try/catch block
        try:
            # Find potential plates
            potential_vehicles = self.find_potential_vehicles(
                request_object.string_tokens())
            self.logger.debug('potential_vehicles: %s', potential_vehicles)

            # Find included campaign hashtags
            included_campaigns = self.detect_campaign_hashtags(
                request_object.string_tokens())
            self.logger.debug('included_campaigns: %s', included_campaigns)

            # Grab legacy string parts
            self.logger.debug('legacy_string_tokens: %s',
                              request_object.legacy_string_tokens())

            potential_vehicles += self.find_potential_vehicles_using_legacy_logic(
                request_object.legacy_string_tokens())
            self.logger.debug('potential_vehicles: %s', potential_vehicles)

            # Grab user info
            self.logger.debug('username: %s',
                              request_object.username())
            self.logger.debug('mentioned_users: %s',
                              request_object.mentioned_users())

            # Grab tweet details for reply.
            self.logger.debug("message id: %s",
                              request_object.external_id())
            self.logger.debug('message created at: %s',
                              request_object.created_at())
            self.logger.debug('message_source: %s',
                              request_object.message_source())
            self.logger.debug('message_type: %s',
                              request_object.message_type())

            # Split plate and state strings into key/value pairs.
            query_info = {
                'created_at': request_object.created_at(),
                'included_campaigns': included_campaigns,
                'message_id': request_object.external_id(),
                'message_type': request_object.message_type(),
                'username': request_object.username()
            }

            self.logger.debug("lookup info: %s", query_info)

            # for each vehicle, we need to determine if the supplied information amounts to a valid plate
            # then we need to look up each valid plate
            # then we need to respond in a single thread in order with the
            # responses

            summary = {
                'fines': {
                    'fined': 0,
                    'outstanding': 0,
                    'reduced': 0,
                    'paid': 0
                },
                'tickets': 0,
                'vehicles': 0
            }

            for potential_vehicle in potential_vehicles:

                if potential_vehicle.get('valid_plate'):

                    query_info['plate'] = potential_vehicle.get('plate')
                    query_info['state'] = potential_vehicle.get('state')
                    query_info['plate_types'] = potential_vehicle.get(
                        'types').upper() if 'types' in potential_vehicle else None

                    # Do the real work!
                    plate_lookup_response = self.perform_plate_lookup(query_info)

                    # Increment summary vehicle info
                    summary['vehicles'] += 1

                    if plate_lookup_response.get('violations'):

                        # Increment summary ticket info
                        summary['tickets'] += sum(s['count']
                                                  for s in plate_lookup_response['violations'])
                        for k, v in {key[0]: key[1] for key in plate_lookup_response['fines']}.items():
                            summary['fines'][k] += v

                        # Record successful lookup.
                        successful_lookup = True

                        plate_lookup_response_parts: List[Any] = self.form_plate_lookup_response_parts(
                            borough_data=plate_lookup_response.get('boroughs'),
                            camera_streak_data=plate_lookup_response.get('camera_streak_data'),
                            fine_data=plate_lookup_response['fines'],
                            frequency=plate_lookup_response['frequency'],
                            plate=plate_lookup_response['plate'],
                            plate_types=plate_lookup_response['plate_types'],
                            previous_lookup=plate_lookup_response['previous_result'],
                            state=plate_lookup_response['state'],
                            username=request_object.username(),
                            violations=plate_lookup_response['violations'],
                            year_data=plate_lookup_response.get('years'))

                        response_parts.append(plate_lookup_response_parts)
                        # [[campaign_stuff], tickets_0, tickets_1, etc.]

                    elif plate_lookup_response.get('error'):

                        # Record lookup error.
                        error_on_lookup = True

                        response_parts.append([
                            f"Sorry, I received an error when looking up "
                            f"{plate_lookup_response.get('state').upper()}:{plate_lookup_response.get('plate').upper()}"
                            f"{(' (types: ' + potential_vehicle.get('types').upper() + ')') if potential_vehicle.get('types') else ''}. "
                            f"Please try again."])

                    else:

                        # Record successful lookup.
                        successful_lookup = True

                        # Let user know we didn't find anything.
                        response_parts.append([
                            f"I couldn't find any tickets for "
                            f"{potential_vehicle.get('state').upper()}:{potential_vehicle.get('plate').upper()}"
                            f"{(' (types: ' + potential_vehicle.get('types').upper() + ')') if potential_vehicle.get('types') else ''}."])

                else:

                    # Record the failed lookup.

                    # Instantiate a connection.
                    conn = self.db_service.get_connection()

                    # Insert failed lookup
                    conn.execute(""" insert into failed_plate_lookups (external_username, message_id, responded_to) values (%s, %s, 1) """, re.sub(
                        '@', '', request_object.username()), request_object.external_id())

                    # Close the connection
                    conn.close()

                    # Legacy data where state is not a valid abbreviation.
                    if potential_vehicle.get('state'):
                        self.logger.debug("We have a state, but it's invalid.")

                        response_parts.append([
                            f"The state should be two characters, but you supplied '{potential_vehicle.get('state')}'. "
                            f"Please try again."])

                    # '<state>:<plate>' format, but no valid state could be detected.
                    elif potential_vehicle.get('original_string'):
                        self.logger.debug(
                            "We don't have a state, but we have an attempted lookup with the new format.")

                        response_parts.append([
                            f"Sorry, a plate and state could not be inferred from "
                            f"{potential_vehicle.get('original_string')}."])

                    # If we have a plate, but no state.
                    elif potential_vehicle.get('plate'):
                        self.logger.debug("We have a plate, but no state")

                        response_parts.append(
                            ["Sorry, the state appears to be blank."])

            # If we have multiple vehicles, prepend a summary.
            if summary.get('vehicles') > 1 and int(summary.get('fines', {}).get('fined')) > 0:
                response_parts.insert(0, self.form_summary_string(
                    summary, request_object.username()))

            # Look up campaign hashtags after doing the plate lookups and then
            # prepend to response.
            if included_campaigns:
                campaign_lookup = self.perform_campaign_lookup(
                    included_campaigns)
                response_parts.insert(0, self.form_campaign_lookup_response_parts(
                    campaign_lookup, request_object.username()))

                successful_lookup = True

            # If we don't look up a single plate successfully,
            # figure out how we can help the user.
            if not successful_lookup and not error_on_lookup:

                # Record the failed lookup

                # Instantiate a connection.
                conn = self.db_service.get_connection()

                # Insert failed lookup
                conn.execute(""" insert into failed_plate_lookups (external_username, message_id, responded_to) values (%s, %s, 1) """, re.sub(
                    '@', '', request_object.username()), request_object.external_id())

                # Close the connection
                conn.close()

                self.logger.debug('The data seems to be in the wrong format.')

                state_regex = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NU|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QC|RI|SC|SD|SK|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
                numbers_regex = r'[0-9]{4}'

                state_pattern = re.compile(state_regex)
                number_pattern = re.compile(numbers_regex)

                state_matches = [state_pattern.search(
                    s.upper()) != None for s in request_object.string_tokens()]
                number_matches = [number_pattern.search(s.upper()) != None for s in list(filter(lambda part: re.sub(
                    r'\.|@', '', part.lower()) not in set(request_object.mentioned_users()), request_object.string_tokens()))]

                # We have what appears to be a plate and a state abbreviation.
                if all([any(state_matches), any(number_matches)]):
                    self.logger.debug(
                        'There is both plate and state information in this message.')

                    # Let user know plate format
                    response_parts.append(
                        ["I’d be happy to look that up for you!\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"])

                # Maybe we have plate or state. Let's find out.
                else:
                    self.logger.debug(
                        'The tweet is missing either state or plate or both.')

                    state_regex_minus_words = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|IA|ID|IL|KS|KY|LA|MA|MB|MD|MH|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NU|NV|NY|PA|PE|PR|PW|QC|RI|SC|SD|SK|STATE|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
                    state_minus_words_pattern = re.compile(
                        state_regex_minus_words)

                    state_minus_words_matches = [state_minus_words_pattern.search(
                        s.upper()) != None for s in request_object.string_tokens()]

                    number_matches = [number_pattern.search(s.upper()) != None for s in list(filter(lambda part: re.sub(
                        r'\.|@', '', part.lower()) not in set(request_object.mentioned_users()), request_object.string_tokens()))]

                    # We have either plate or state.
                    if any(state_minus_words_matches) or any(number_matches):

                        # Let user know plate format
                        response_parts.append(
                            ["I think you're trying to look up a plate, but can't be sure.\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"])

                    # We have neither plate nor state. Do nothing.
                    else:
                        self.logger.debug(
                            'ignoring message since no plate or state information to respond to.')

        except Exception as e:
            # Set response data
            error_on_lookup = True
            response_parts.append(
                ["Sorry, I encountered an error. Tagging @bdhowald."])

            # Log error
            self.logger.error('Missing necessary information to continue')
            self.logger.error(e)
            self.logger.error(str(e))
            self.logger.error(e.args)
            logging.exception("stack trace")

        # Indicate successful response processing.
        return {
            'error_on_lookup': error_on_lookup,
            'request_object': request_object,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': successful_lookup,
            'username': request_object.username()
        }
