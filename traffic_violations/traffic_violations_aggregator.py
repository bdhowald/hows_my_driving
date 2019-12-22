import logging
import pytz
import re
import requests
import requests_futures.sessions

from datetime import datetime, timedelta
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from sqlalchemy import func
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from traffic_violations.constants import L10N, regexps as regexp_constants, \
    twitter as twitter_constants

from traffic_violations.models.lookup_requests import BaseLookupRequest
from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.campaign import Campaign
from traffic_violations.models.failed_plate_lookup import FailedPlateLookup
from traffic_violations.models.fine_data import FineData
from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse
from traffic_violations.models.response.traffic_violations_aggregator_response \
    import TrafficViolationsAggregatorResponse
from traffic_violations.models.vehicle import Vehicle

from traffic_violations.services.apis.open_data_service import OpenDataService
from traffic_violations.services.apis.tweet_detection_service import \
    TweetDetectionService

LOG = logging.getLogger(__name__)


class TrafficViolationsAggregator:

    MYSQL_TIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'
    REPEAT_LOOKUP_DATE_FORMAT: str = '%B %-d, %Y'
    REPEAT_LOOKUP_TIME_FORMAT: str = '%I:%M%p'

    def __init__(self):
        self.tweet_detection_service = TweetDetectionService()

        self.eastern = pytz.timezone('US/Eastern')
        self.utc = pytz.timezone('UTC')

    def initiate_reply(self, lookup_request: Type[BaseLookupRequest]):
        LOG.info('\n')
        LOG.info('Calling initiate_reply')

        if lookup_request.requires_response():
            return self._create_response(lookup_request)

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
            if previous_lookup.message_type == twitter_constants.TwitterMessageTypes.STATUS.value:
                # Determine if tweet is still visible:
                if self.tweet_detection_service.tweet_exists(id=previous_lookup.message_id,
                                                             username=username):
                    can_link_tweet = True

            # Determine when the last lookup was...
            previous_time = previous_lookup.created_at
            now = datetime.now()

            adjusted_time = self.utc.localize(previous_time)
            adjusted_now = self.utc.localize(now)

            # If at least five minutes have passed...
            if adjusted_now - timedelta(minutes=5) > adjusted_time:

                # Add the new ticket info and previous lookup time to the string.
                violations_string += L10N.LAST_QUERIED_STRING.format(
                    adjusted_time.astimezone(
                        self.eastern).strftime(self.REPEAT_LOOKUP_DATE_FORMAT),
                    adjusted_time.astimezone(self.eastern).strftime(
                        self.REPEAT_LOOKUP_TIME_FORMAT))

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

    def _create_response(self, request_object: Type[BaseLookupRequest]):

        LOG.info('\n')
        LOG.info('Calling create_response')

        # Grab tweet details for reply.
        LOG.debug(f'request_object: {request_object}')

        # Collect response parts here.
        response_parts = []
        successful_lookup = False
        error_on_lookup = False

        # Wrap in try/catch block
        try:

            # Find potential plates
            potential_vehicles: List[Vehicle] = self._find_potential_vehicles(
                request_object.string_tokens())
            LOG.debug(f'potential_vehicles: {potential_vehicles}')

            potential_vehicles += self._find_potential_vehicles_using_legacy_logic(
                request_object.legacy_string_tokens())
            LOG.debug(f'potential_vehicles: {potential_vehicles}')

            # Find included campaign hashtags
            included_campaigns = self._detect_campaign_hashtags(
                request_object.string_tokens())
            LOG.debug(f'included_campaigns: {included_campaigns}')

            # for each vehicle, we need to determine if the supplied information amounts to a valid plate
            # then we need to look up each valid plate
            # then we need to respond in a single thread in order with the responses

            summary: TrafficViolationsAggregatorResponse = TrafficViolationsAggregatorResponse()

            for potential_vehicle in potential_vehicles:

                if potential_vehicle.valid_plate:

                    plate_query: PlateQuery = self._get_plate_query(
                        vehicle=potential_vehicle,
                        request_object=request_object)

                    # Do the real work!
                    open_data_response: OpenDataServiceResponse = self._perform_plate_lookup(
                        campaigns=included_campaigns,
                        plate_query=plate_query)

                    if open_data_response.success:

                        # Record successful lookup.
                        successful_lookup = True

                        plate_lookup: OpenDataServicePlateLookup = open_data_response.data

                        # do we have a previous lookup
                        previous_lookup: Optional[PlateLookup] = self._query_for_previous_lookup(plate_query=plate_query)
                        LOG.debug(f'Previous lookup for this vehicle: {previous_lookup}')

                        # how many times have we searched for this plate from a tweet
                        current_frequency: int = self._query_for_lookup_frequency(plate_query)

                        # self._proceess_lookup_results()

                        # Add lookup to summary
                        summary.plate_lookups.append(plate_lookup)

                        if plate_lookup.violations:

                            plate_lookup_response_parts: List[Any] = self._form_plate_lookup_response_parts(
                                borough_data=plate_lookup.boroughs,
                                camera_streak_data=plate_lookup.camera_streak_data,
                                fine_data=plate_lookup.fines,
                                frequency=(current_frequency + 1),
                                plate=plate_lookup.plate,
                                plate_types=plate_lookup.plate_types,
                                previous_lookup=previous_lookup,
                                state=plate_lookup.state,
                                username=request_object.username(),
                                violations=plate_lookup.violations,
                                year_data=plate_lookup.years)

                            response_parts.append(plate_lookup_response_parts)
                            # [[campaign_stuff], tickets_0, tickets_1, etc.]

                        else:
                            # Let user know we didn't find anything.
                            plate_types_string = (
                                f' (types: {plate_query.plate_types})') if plate_types else ''
                            L10N.NO_TICKETS_FOUND_STRING.format(
                                plate_query.state,
                                plate_query.plate,
                                plate_types_string)
                            response_parts.append(
                                L10N.NO_TICKETS_FOUND_STRING.format(
                                    plate_query.state,
                                    plate_query.plate,
                                    plate_types_string))

                    else:

                        # Record lookup error.
                        error_on_lookup = True

                        response_parts.append([
                            f"Sorry, I received an error when looking up "
                            f"{plate_query.state}:{plate_query.plate}"
                            f"{(' (types: ' + plate_query.plate_types + ')') if plate_query.plate_types else ''}. "
                            f"Please try again."])

                else:

                    # Record the failed lookup.
                    new_failed_lookup = FailedPlateLookup(
                        message_id=request_object.external_id(),
                        username=request_object.username())

                    # Insert plate lookup
                    FailedPlateLookup.query.session.add(new_failed_lookup)
                    FailedPlateLookup.query.session.commit()

                    # Legacy data where state is not a valid abbreviation.
                    if potential_vehicle.get('state'):
                        LOG.debug("We have a state, but it's invalid.")

                        response_parts.append([
                            f"The state should be two characters, but you supplied '{potential_vehicle.get('state')}'. "
                            f"Please try again."])

                    # '<state>:<plate>' format, but no valid state could be detected.
                    elif potential_vehicle.get('original_string'):
                        LOG.debug(
                            "We don't have a state, but we have an attempted lookup with the new format.")

                        response_parts.append([
                            f"Sorry, a plate and state could not be inferred from "
                            f"{potential_vehicle.get('original_string')}."])

                    # If we have a plate, but no state.
                    elif potential_vehicle.get('plate'):
                        LOG.debug("We have a plate, but no state")

                        response_parts.append(
                            ["Sorry, the state appears to be blank."])

            # If we have multiple vehicles, prepend a summary.
            if len(summary.plate_lookups) > 1:

                # # Increment summary ticket info
                # summary.tickets += sum(s['count']
                #                           for s in plate_lookup.violations)

                # fine_data: FineData = plate_lookup.fines
                # for key in ['fined', 'outstanding', 'paid', 'reduced']:
                #     plate_lookup_fines: float = getattr(plate_lookup.fines, key)
                #     current_summary_fines: float = getattr(summary.fines, key)
                #     setattr(summary.fines, key, current_summary_fines + plate_lookup_fines)

                if int(summary.fines.fined) > 0:
                    response_parts.insert(
                        0, self._form_summary_string(summary))

            # Look up campaign hashtags after doing the plate lookups and then
            # prepend to response.
            if included_campaigns:
                campaign_lookups: List[Tuple[str, int, int]] = self._perform_campaign_lookup(
                    included_campaigns)
                response_parts.insert(0, self._form_campaign_lookup_response_parts(
                    campaign_lookups, request_object.username()))

                successful_lookup = True

            # If we don't look up a single plate successfully,
            # figure out how we can help the user.
            if not successful_lookup and not error_on_lookup:

                # Record the failed lookup.
                new_failed_lookup = FailedPlateLookup(
                    message_id=request_object.external_id(),
                    username=request_object.username())

                # Insert plate lookup
                FailedPlateLookup.query.session.add(new_failed_lookup)
                FailedPlateLookup.query.session.commit()

                LOG.debug('The data seems to be in the wrong format.')

                state_matches = [regexp_constants.STATE_ABBR_PATTERN.search(
                    s.upper()) != None for s in request_object.string_tokens()]
                number_matches = [regexp_constants.NUMBER_PATTERN.search(s.upper()) != None for s in list(filter(lambda part: re.sub(
                    r'\.|@', '', part.lower()) not in set(request_object.mentioned_users), request_object.string_tokens()))]

                # We have what appears to be a plate and a state abbreviation.
                if all([any(state_matches), any(number_matches)]):
                    LOG.debug(
                        'There is both plate and state information in this message.')

                    # Let user know plate format
                    response_parts.append(
                        ["I’d be happy to look that up for you!\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"])

                # Maybe we have plate or state. Let's find out.
                else:
                    LOG.debug(
                        'The tweet is missing either state or plate or both.')

                    state_minus_words_matches = [regexp_constants.STATE_MINUS_WORDS_PATTERN.search(
                        s.upper()) != None for s in request_object.string_tokens()]

                    number_matches = [regexp_constants.NUMBER_PATTERN.search(s.upper()) != None for s in list(filter(lambda part: re.sub(
                        r'\.|@', '', part.lower()) not in set(request_object.mentioned_users), request_object.string_tokens()))]

                    # We have either plate or state.
                    if any(state_minus_words_matches) or any(number_matches):

                        # Let user know plate format
                        response_parts.append(
                            ["I think you're trying to look up a plate, but can't be sure.\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"])

                    # We have neither plate nor state. Do nothing.
                    else:
                        LOG.debug(
                            'ignoring message since no plate or state information to respond to.')

        except Exception as e:
            # Set response data
            error_on_lookup = True
            response_parts.append(
                ["Sorry, I encountered an error. Tagging @bdhowald."])

            # Log error
            LOG.error('Missing necessary information to continue')
            LOG.error(e)
            LOG.error(str(e))
            LOG.error(e.args)
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

    def _detect_campaign_hashtags(self, string_tokens):
        """ Look for campaign hashtags in the message's text. """

        return Campaign.get_all_in(hashtag=tuple([regexp_constants.HASHTAG_PATTERN.sub('', string) for string in string_tokens]))

    def _detect_plate_types(self, plate_types_input) -> bool:
        plate_types_pattern = re.compile(
            regexp_constants.REGISTRATION_TYPES_REGEX)

        if ',' in plate_types_input:
            parts = plate_types_input.upper().split(',')
            return any([plate_types_pattern.search(part) != None for part in parts])
        else:
            return plate_types_pattern.search(plate_types_input.upper()) != None

    def _detect_state(self, state_input) -> bool:
        # or state_full_pattern.search(state_input.upper()) != None
        """ Does this input constitute a valid state abbreviation """
        if state_input is not None:
            return regexp_constants.STATE_ABBR_PATTERN.search(state_input.upper()) != None

        return False

    def _find_potential_vehicles(self, list_of_strings: List[str]) -> List[Vehicle]:

        # Use new logic of '<state>:<plate>'
        plate_tuples: Union[Tuple[str, str, str], Tuple[str, str]] = [[part.strip() for part in match.split(':')] for match in re.findall(regexp_constants.PLATE_FORMAT_REGEX, ' '.join(
            list_of_strings)) if all(substr not in match.lower() for substr in ['://', 'state:', 'plate:'])]

        return self._infer_plate_and_state_data(plate_tuples)

    def _find_potential_vehicles_using_legacy_logic(self, list_of_strings: List[str]) -> List[Vehicle]:

        # Find potential plates

        # Use old logic of 'state:<state> plate:<plate>'
        potential_vehicles: List[Vehicle] = []
        legacy_plate_data: Tuple = dict([[piece.strip() for piece in match.split(':')] for match in [part.lower(
        ) for part in list_of_strings if ('state:' in part.lower() or 'plate:' in part.lower() or 'types:' in part.lower())]])

        if legacy_plate_data:
            if self._detect_state(legacy_plate_data.get('state')) and legacy_plate_data.get('plate'):
                legacy_plate_data['valid_plate'] = True
            else:
                legacy_plate_data['valid_plate'] = False

            vehicle: Vehicle = Vehicle(plate=legacy_plate_data.get('plate'),
                                       plate_types=legacy_plate_data.get(
                                           'types'),
                                       state=legacy_plate_data.get('state'),
                                       valid_plate=legacy_plate_data['valid_plate'])

            potential_vehicles.append(vehicle)

        return potential_vehicles

    def _form_campaign_lookup_response_parts(self,
                                             campaign_summaries:
                                             List[Tuple[str, int, int]],
                                             username: str):

        campaign_chunks: List[str] = []
        campaign_string = ""

        for campaign in campaign_summaries:
            campaign_name = campaign[0]
            campaign_vehicles = campaign[1]
            campaign_tickets = campaign[2]

            next_string_part = (
                f"{campaign_vehicles} {'vehicle with' if campaign_vehicles == 1 else 'vehicles with a total of'} "
                f"{campaign_tickets} ticket{L10N.pluralize(campaign_tickets)} {'has' if campaign_vehicles == 1 else 'have'} "
                f"been tagged with { campaign_name}.\n\n")

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

    def _form_plate_lookup_response_parts(
            self,
            borough_data:  List[Tuple[str, int]],
            frequency: int,
            fine_data: FineData,
            plate: str,
            plate_types: List[str],
            state: str,
            username: str,
            violations: List[Tuple[str, int]],
            year_data: List[Tuple[str, int]],
            camera_streak_data: Optional[CameraStreakData] = None,
            previous_lookup: Optional[PlateLookup] = None):

        # response_chunks holds tweet-length-sized parts of the response
        # to be tweeted out or appended into a single direct message.
        response_chunks: List[str] = []
        violations_string: str = ""

        # Get total violations
        total_violations: int = sum([s['count']
                                     for s in violations])
        LOG.debug("total_violations: %s", total_violations)

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

        response_chunks += self._handle_response_part_formation(
            collection=violations,
            continued_format_string=L10N.LOOKUP_TICKETS_STRING_CONTD.format(
                L10N.VEHICLE_HASHTAG.format(state, plate)),
            count='count',
            cur_string=violations_string,
            description='title',
            default_description='No Year Available',
            prefix_format_string=L10N.LOOKUP_TICKETS_STRING.format(
                total_violations),
            result_format_string=L10N.LOOKUP_RESULTS_DETAIL_STRING,
            username=username)

        if year_data:
            response_chunks += self._handle_response_part_formation(
                collection=year_data,
                continued_format_string=L10N.LOOKUP_YEAR_STRING_CONTD.format(
                    L10N.VEHICLE_HASHTAG.format(state, plate)),
                count='count',
                description='title',
                default_description='No Year Available',
                prefix_format_string=L10N.LOOKUP_YEAR_STRING.format(
                    L10N.VEHICLE_HASHTAG.format(state, plate)),
                result_format_string=L10N.LOOKUP_RESULTS_DETAIL_STRING,
                username=username)

        if borough_data:
            response_chunks += self._handle_response_part_formation(
                collection=borough_data,
                continued_format_string=L10N.LOOKUP_BOROUGH_STRING_CONTD.format(
                    L10N.VEHICLE_HASHTAG.format(state, plate)),
                count='count',
                description='title',
                default_description='No Borough Available',
                prefix_format_string=L10N.LOOKUP_BOROUGH_STRING.format(
                    L10N.VEHICLE_HASHTAG.format(state, plate)),
                result_format_string=L10N.LOOKUP_RESULTS_DETAIL_STRING,
                username=username)

        if fine_data and fine_data.fines_assessed():
            # if fine_data and any([k[1] != 0 for k in fine_data]):

            cur_string = f"Known fines for {L10N.VEHICLE_HASHTAG.format(state, plate)}:\n\n"

            max_count_length = len('${:,.2f}'.format(fine_data.max_amount()))
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

            if camera_streak_data.max_streak and camera_streak_data.max_streak >= 5:

                # formulate streak string
                streak_string = (
                    f"Under @bradlander's proposed legislation, "
                    f"this vehicle could have been booted or impounded "
                    f"due to its {camera_streak_data.max_streak} camera violations "
                    f"(>= 5/year) from {camera_streak_data.min_streak_date}"
                    f" to {camera_streak_data.max_streak_date}.\n")

                # add to container
                response_chunks.append(streak_string)

        # Send it back!
        return response_chunks

    def _form_summary_string(self, summary: TrafficViolationsAggregatorResponse) -> str:
        num_vehicles = len(summary.plate_lookups)
        num_tickets = sum(len(lookup.violations)
                          for lookup in summary.plate_lookups)
        aggregate_fines: FineData = FineData(**{field: sum(getattr(lookup.fine_data, field) for lookup in summary.plate_lookups) for field in FineData.FINE_FIELDS})
        return [
            f"The {num_vehicles} vehicles you queried have collectively received "
            f"{num_tickets} ticket{L10N.pluralize(num_tickets)} with at "
            f"least {'${:,.2f}'.format(aggregate_fines.fined - aggregate_fines.reduced)} "
            f"in fines, of which {'${:,.2f}'.format(aggregate_fines.paid)} has been paid.\n\n"]

    def _get_plate_query(self, request_object: Type[BaseLookupRequest], vehicle: Vehicle) -> PlateQuery:
        """Transform a request object into plate query"""

        created_at: str = datetime.strptime(request_object.created_at,
                                            twitter_constants.TWITTER_TIME_FORMAT).strftime(self.MYSQL_TIME_FORMAT)

        message_id: Optional[str] = request_object.external_id()
        message_type: str = request_object.message_type

        plate: str = regexp_constants.PLATE_PATTERN.sub(
            '', vehicle.plate.strip().upper())

        plate_types: Optional[str] = None
        if vehicle.plate_types is not None:
            plate_types = ','.join(
                sorted([type.strip() for type in vehicle.plate_types.upper().split(',')]))

        state: str = vehicle.state.upper()

        username: Optional[str] = request_object.username()

        plate_query: PlateQuery = PlateQuery(created_at=created_at,
                                             message_id=message_id,
                                             message_type=message_type,
                                             plate=plate,
                                             plate_types=plate_types,
                                             state=state,
                                             username=username)

        LOG.debug(f'plate_query: {plate_query}')

        return plate_query

    def _handle_response_part_formation(self,
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

    def _infer_plate_and_state_data(self,
                                    list_of_vehicle_tuples:
                                    Union[Tuple[str, str, str],
                                          Tuple[str, str]]) -> List[Vehicle]:

        potential_vehicles: List[Vehicle] = []

        for vehicle_tuple in list_of_vehicle_tuples:

            original_string = ':'.join(vehicle_tuple)
            plate = None
            plate_types = None
            state = None
            valid_plate = False

            if len(vehicle_tuple) in range(2, 4):
                state_bools: List[bool] = [self._detect_state(
                    part) for part in vehicle_tuple]
                try:
                    state_index = state_bools.index(True)
                except ValueError:
                    state_index = None

                plate_types_bools: List[bool] = [self._detect_plate_types(
                    part) for part in vehicle_tuple]
                try:
                    plate_types_index = plate_types_bools.index(True)
                except ValueError:
                    plate_types_index = None

                have_valid_plate: bool = (len(vehicle_tuple) == 2 and state_index is not None) or (
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
                        plate = vehicle_tuple[plate_index]
                        state = vehicle_tuple[state_index]

                        if plate_types_index is not None:
                            plate_types = vehicle_tuple[
                                plate_types_index]

                        valid_plate = True

            vehicle: Vehicle = Vehicle(original_string=original_string,
                                       plate=plate,
                                       plate_types=plate_types,
                                       state=state,
                                       valid_plate=valid_plate)

            potential_vehicles.append(vehicle)

        return potential_vehicles

    def _perform_campaign_lookup(self,
                                 included_campaigns:
                                 List[Tuple[int, str]]) -> List[
            Tuple[str, int, int]]:

        LOG.debug('Performing lookup for campaigns.')

        result: List[Tuple[str, int, int]] = []

        for campaign_tuple in included_campaigns:
            campaign: Campaign = Campaign.get_by(id=campaign_tuple[0])

            subquery = campaign.plate_lookups.session.query(
                PlateLookup.plate, PlateLookup.state, func.max(PlateLookup.created_at).label(
                    'most_recent_campaign_lookup'),).group_by(
                PlateLookup.plate, PlateLookup.state).subquery('subquery')

            full_query = PlateLookup.query.join(subquery,
                                                (PlateLookup.plate == subquery.c.plate) &
                                                (PlateLookup.state == subquery.c.state) &
                                                (PlateLookup.created_at ==
                                                    subquery.c.most_recent_campaign_lookup)).order_by(subquery.c.most_recent_campaign_lookup.desc(), PlateLookup.created_at.desc())

            campaign_lookups = full_query.all()

            campaign_vehicles: int = len(campaign_lookups)
            campaign_tickets: int = sum(
                [lookup.num_tickets for lookup in campaign_lookups])

            result.append(
                (campaign_tuple[1], campaign_vehicles, campaign_tickets))

        return result

    def _perform_plate_lookup(self,
                              campaigns: List[Campaign],
                              plate_query: PlateQuery) -> OpenDataServiceResponse:

        LOG.debug('Performing lookup for plate.')

        nyc_open_data_service: OpenDataService = OpenDataService()
        open_data_response: OpenDataServiceResponse = nyc_open_data_service.lookup_vehicle(
            plate_query=plate_query)

        LOG.debug(f'Violation data: {open_data_response}')

        if open_data_response.success:

            open_data_plate_lookup: OpenDataServicePlateLookup = open_data_response.data

            camera_streak_data: CameraStreakData = open_data_plate_lookup.camera_streak_data

            # If this came from message, add it to the plate_lookups table.
            if plate_query.message_type and plate_query.message_id and plate_query.created_at:
                new_lookup = PlateLookup(
                    boot_eligible=camera_streak_data.max_streak >= 5 if camera_streak_data else False,
                    created_at=plate_query.created_at,
                    message_id=plate_query.message_id,
                    message_type=plate_query.message_type,
                    num_tickets=open_data_plate_lookup.num_violations,
                    plate=plate_query.plate,
                    plate_types=plate_query.plate_types,
                    state=plate_query.state,
                    username=plate_query.username
                )

                # Iterate through included campaigns to tie lookup to each
                for campaign in campaigns:
                    # insert join record for campaign lookup
                    new_lookup.campaigns.append(campaign)

                # Insert plate lookup
                PlateLookup.query.session.add(new_lookup)
                PlateLookup.query.session.commit()

        else:
            LOG.info(f'open data plate lookup failed')

        return open_data_response

    def _proceess_lookup_results(self, plate_query: PlateLookup):
        pass

    def _query_for_lookup_frequency(self, plate_query: PlateQuery) -> int:
        return PlateLookup.query.filter_by(
            plate=plate_query.plate,
            plate_types=plate_query.plate_types,
            state=plate_query.state).count()

    def _query_for_previous_lookup(self, plate_query: PlateQuery) -> Optional[PlateLookup]:
        """ See if we've seen this vehicle before. """

        return PlateLookup.get_by(
            plate=plate_query.plate,
            state=plate_query.state,
            plate_types=plate_query.plate_types,
            count_towards_frequency=True)
