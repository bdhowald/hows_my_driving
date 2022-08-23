import logging
import pytz
import random
import re
import requests
import requests_futures.sessions
import string

from datetime import datetime, timedelta
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from sqlalchemy import and_, func
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from traffic_violations.constants import (L10N, endpoints, lookup_sources,
    thresholds, twitter as twitter_constants, regexps as regexp_constants)

from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.campaign import Campaign
from traffic_violations.models.failed_plate_lookup import FailedPlateLookup
from traffic_violations.models.fine_data import FineData
from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.lookup_requests import BaseLookupRequest

from traffic_violations.models.response.invalid_vehicle_response \
    import InvalidVehicleResponse
from traffic_violations.models.response.non_vehicle_response \
    import NonVehicleResponse
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse
from traffic_violations.models.response.traffic_violations_aggregator_response \
    import TrafficViolationsAggregatorResponse
from traffic_violations.models.response.valid_vehicle_response \
    import ValidVehicleResponse
from traffic_violations.models.vehicle import Vehicle

from traffic_violations.services.apis.open_data_service import OpenDataService
from traffic_violations.services.apis.tweet_detection_service import \
    TweetDetectionService

LOG = logging.getLogger(__name__)


class TrafficViolationsAggregator:

    CAMERA_THRESHOLDS = {
        'Mixed': thresholds.RECKLESS_DRIVER_ACCOUNTABILITY_ACT_THRESHOLD,
        'Failure to Stop at Red Light':
            thresholds.DANGEROUS_VEHICLE_ABATEMENT_ACT_RED_LIGHT_CAMERA_THRESHOLD,
        'School Zone Speed Camera Violation':
            thresholds.DANGEROUS_VEHICLE_ABATEMENT_ACT_SCHOOL_ZONE_SPEED_CAMERA_THRESHOLD
    }

    CAMERA_VIOLATIONS = ['Bus Lane Violation',
                         'Failure To Stop At Red Light',
                         'Mobile Bus Lane Violation',
                         'School Zone Speed Camera Violation']

    MYSQL_TIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'

    UNIQUE_IDENTIFIER_STRING_LENGTH = 8

    def __init__(self):
        self.tweet_detection_service = TweetDetectionService()

        self.eastern = pytz.timezone('US/Eastern')
        self.utc = pytz.timezone('UTC')

    def initiate_reply(self, lookup_request: Type[BaseLookupRequest]):
        """Look up the plates in a request and return the results."""
        LOG.debug('Calling initiate_reply')

        if lookup_request.requires_response():
            return self._create_response(lookup_request)

    def lookup_has_valid_plates(self, lookup_request: Type[BaseLookupRequest]
        ) -> bool:
        """Does the request have valid plates in it requiring a response."""

        potential_vehicles: List[Vehicle] = self._find_potential_vehicles(
            lookup_request)

        return any(potential_vehicle.valid_plate for potential_vehicle
            in potential_vehicles)

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
            if previous_lookup.message_source == lookup_sources.LookupSource.STATUS.value:
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
                        self.eastern).strftime(L10N.REPEAT_LOOKUP_DATE_FORMAT),
                    adjusted_time.astimezone(self.eastern).strftime(
                        L10N.REPEAT_LOOKUP_TIME_FORMAT))

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

    def _create_response(self, request_object: Type[BaseLookupRequest]) -> dict:
        LOG.debug('Calling create_response')

        # Grab tweet details for reply.
        LOG.debug(f'request_object: {request_object}')

        # Collect response parts here.
        response_parts: List[Any] = []

        # We need to know if any lookups errored out or were successful.
        error_on_any_lookup = False
        success_on_any_lookup = False

        # Find potential plates
        potential_vehicles: List[Vehicle] = self._find_potential_vehicles(
            request_object)
        LOG.debug(f'potential_vehicles: {potential_vehicles}')

        # Find included campaign hashtags
        included_campaigns: List[Campaign] = self._detect_campaigns(
            request_object.string_tokens())
        LOG.debug(f'included_campaigns: {included_campaigns}')

        try:
            # for each vehicle, we determine if the supplied information amounts to a valid plate
            # then we look up each valid plate
            # then we respond in a single thread in order with the responses

            summary: TrafficViolationsAggregatorResponse = TrafficViolationsAggregatorResponse()

            for potential_vehicle in potential_vehicles:

                if potential_vehicle.valid_plate:
                    # If the plate is valid, process it by doing a lookup.
                    vehicle_response: ValidVehicleResponse = self._process_valid_vehicle(
                        campaigns=included_campaigns,
                        request_object=request_object,
                        vehicle=potential_vehicle)

                    # Add lookup to summary
                    if vehicle_response.plate_lookup:
                        summary.plate_lookups.append(vehicle_response.plate_lookup)

                    # Keep track of any error while looking up a plate.
                    error_on_any_lookup = error_on_any_lookup or vehicle_response.error_on_lookup

                    # Note if any lookup was successful
                    success_on_any_lookup = success_on_any_lookup or vehicle_response.success_on_lookup

                else:
                    # If the plate is invalid, process it by gathering response parts.
                    vehicle_response: InvalidVehicleResponse = self._process_invalid_vehicle(
                        request_object=request_object,
                        invalid_vehicle=potential_vehicle)

                # Add response parts.
                response_parts.append(vehicle_response.response_parts)

            # If we have multiple vehicles, prepend a summary.
            if len(summary.plate_lookups) > 1:

                summary_string: Optional[str] = self._form_summary_string(summary)

                if summary_string:
                    # Prepend summary string if more than one vehicle.
                    response_parts.insert(0, summary_string)

            # Look up campaign hashtags after doing the plate lookups and then
            # prepend to response.
            if included_campaigns:
                campaign_lookups: List[Tuple[str, int, int]] = self._perform_campaign_lookup(
                    included_campaigns)

                # Prepend campaign string to response.
                response_parts.insert(0, self._form_campaign_lookup_response_parts(
                    campaign_lookups))

                success_on_any_lookup = True

            # If we don't look up a single plate successfully,
            # figure out how we can help the user.
            if not success_on_any_lookup and not error_on_any_lookup:

                non_vehicle_response: NonVehicleResponse = self._process_lookup_without_detected_vehicles(
                    request_object=request_object)

                response_parts.append(non_vehicle_response.response_parts)

        except Exception as e:
            # Set response data
            error_on_any_lookup = True
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
            'error_on_lookup': error_on_any_lookup,
            'request_object': request_object,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': success_on_any_lookup
        }

    def _detect_campaigns(self, string_tokens) -> List[Campaign]:
        """ Look for campaign hashtags in the message's text
        and return matching campaigns.

        """

        return Campaign.get_all_in(
            hashtag=tuple(
                [regexp_constants.HASHTAG_PATTERN.sub('', string) for string in string_tokens]))

    def _detect_plate_types(self, plate_types_input: str) -> bool:
        if ',' in plate_types_input:
            parts = plate_types_input.upper().split(',')
            return any([regexp_constants.PLATE_TYPES_PATTERN.search(part) != None for part in parts])
        else:
            return regexp_constants.PLATE_TYPES_PATTERN.search(plate_types_input.upper()) != None

    def _detect_state(self, state_input) -> bool:
        # or state_full_pattern.search(state_input.upper()) != None
        """ Does this input constitute a valid state abbreviation """
        if state_input is not None:
            return regexp_constants.STATE_ABBREVIATIONS_PATTERN.search(state_input.upper()) != None

        return False

    def _ensure_unique_plates(self,
                              vehicles: List[Vehicle]
                              ) -> List[Vehicle]:

        vehicle_dict: Dict[str, Vehicle] = {}
        unique_vehicles: List[Vehicle] = []

        for vehicle in vehicles:
            lookup_string = (
                f'{vehicle.state}:'
                f'{vehicle.plate}'
                f"{(':' + vehicle.plate_types) if vehicle.plate_types else ''}")

            if vehicle_dict.get(lookup_string) is None:
                vehicle_dict[lookup_string] = vehicle
                unique_vehicles.append(vehicle)

        return unique_vehicles

    def _find_potential_vehicles(self, request_object: Type[BaseLookupRequest]) -> List[Vehicle]:
        """Parse tweet text for vehicles"""

        potential_vehicles: List[Vehicle] = []

        potential_vehicles += self._find_potential_vehicles_using_combined_fields(
            list_of_strings=request_object.string_tokens())

        potential_vehicles += self._find_potential_vehicles_using_separate_fields(
            list_of_strings=request_object.legacy_string_tokens())

        return self._ensure_unique_plates(
            vehicles=potential_vehicles)

    def _find_potential_vehicles_using_separate_fields(self, list_of_strings: List[str]) -> List[Vehicle]:
        """Parse tweet text for vehicles using old logic of 'state:<state> plate:<plate>'"""

        potential_vehicles: List[Vehicle] = []
        legacy_plate_data: Tuple = dict([[piece.strip() for piece in match.split(':')] for match in [part.lower(
        ) for part in list_of_strings if (('state:' in part.lower() or 'plate:' in part.lower() or 'types:' in part.lower()) and '://' not in part.lower())]])

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

    def _find_potential_vehicles_using_combined_fields(self, list_of_strings: List[str]) -> List[Vehicle]:
        """Parse tweet text for vehicles using new logic of '<state>:<plate>'"""

        potential_plates: Union[Tuple[str, str, str], Tuple[str, str]] = [[part.strip() for part in match.split(':')] for match in re.findall(regexp_constants.PLATE_FORMAT_REGEX, ' '.join(
            list_of_strings)) if all(substr not in match.lower() for substr in ['://', 'state:', 'plate:'])]

        for start_plate in potential_plates:
            for comparison_plate in potential_plates:
                if start_plate == comparison_plate:
                    # Skip on self-comparison
                    continue
                if set(start_plate) < set(comparison_plate):
                    # Remove plate if it is subset of another plate match
                    potential_plates.remove(start_plate)

        return self._infer_plate_and_state_data(potential_plates)


    def _form_campaign_lookup_response_parts(self,
                                             campaign_summaries:
                                             List[Tuple[str, int, int]]):

        campaign_chunks: List[str] = []
        campaign_string = ""

        for campaign in campaign_summaries:
            campaign_name = campaign[0]
            campaign_vehicles = campaign[1]
            campaign_tickets = campaign[2]

            next_string_part = (
                f"{'{:,}'.format(campaign_vehicles)} {'vehicle with' if campaign_vehicles == 1 else 'vehicles with a total of'} "
                f"{'{:,}'.format(campaign_tickets)} ticket{L10N.pluralize(campaign_tickets)} {'has' if campaign_vehicles == 1 else 'have'} "
                f"been tagged with {campaign_name}.\n\n")

            # how long would it be
            potential_response_length = len(campaign_string + next_string_part)

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
            lookup_source: str,
            plate: str,
            plate_types: List[str],
            state: str,
            unique_identifier: str,
            username: str,
            violations: List[Tuple[str, int]],
            year_data: List[Tuple[str, int]],
            camera_streak_data: Dict[str, CameraStreakData] = None,
            previous_lookup: Optional[PlateLookup] = None):

        # response_chunks holds tweet-length-sized parts of the response
        # to be tweeted out or appended into a single direct message.
        response_chunks: List[str] = []
        violations_string: str = ""

        # Get total violations
        total_violations: int = sum([s['count']
                                     for s in violations])
        LOG.debug(f'total_violations: {total_violations}')

        now_in_eastern_time = self.utc.localize(datetime.now()).astimezone(self.eastern)
        time_prefix = now_in_eastern_time.strftime('As of %I:%M:%S %p %Z on %B %-d, %Y:\n\n')

        # Append username to blank string to start to build tweet.
        violations_string += (f'@{username} {time_prefix}' if lookup_source
                                 == lookup_sources.LookupSource.STATUS.value else '')

        # Append summary string.
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

        response_chunks.append(violations_string)

        username_prefix = (f'@{twitter_constants.HMDNY_TWITTER_HANDLE} {time_prefix}' if lookup_source
                               == lookup_sources.LookupSource.STATUS.value else '')

        response_chunks += self._handle_response_part_formation(
            collection=violations,
            continued_format_string=L10N.LOOKUP_TICKETS_STRING_CONTD.format(
                L10N.VEHICLE_HASHTAG.format(state, plate)),
            count='count',
            description='title',
            default_description='No Year Available',
            prefix_format_string=L10N.LOOKUP_TICKETS_STRING.format(
                L10N.VEHICLE_HASHTAG.format(state, plate),
                total_violations),
            result_format_string=L10N.LOOKUP_RESULTS_DETAIL_STRING,
            username_prefix=username_prefix)

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
                username_prefix=username_prefix)

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
                username_prefix=username_prefix)

        if fine_data and fine_data.fines_assessed():

            cur_string = (f'{username_prefix}'
                          f'Known fines for {L10N.VEHICLE_HASHTAG.format(state, plate)}:\n\n')

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
                potential_response_length = len(cur_string + next_part)

                # If violation string so far and new part are less or
                # equal than 280 characters, append to existing tweet string.
                if (potential_response_length <= twitter_constants.MAX_TWITTER_STATUS_LENGTH):
                    cur_string += next_part
                else:
                    response_chunks.append(cur_string)

                    cur_string = "Known fines for #{}_{}, cont'd:\n\n"
                    cur_string += next_part

            # add to container
            response_chunks.append(cur_string)

        for camera_violation_type, threshold in self.CAMERA_THRESHOLDS.items():
            violation_type_data = camera_streak_data[camera_violation_type]

            if violation_type_data:

                if (camera_violation_type == 'Failure to Stop at Red Light' and
                    violation_type_data.max_streak >= threshold):

                    # add to container
                    response_chunks.append(L10N.DANGEROUS_VEHICLE_ABATEMENT_ACT_REPEAT_OFFENDER_STRING.format(
                        username_prefix,
                        violation_type_data.max_streak,
                        'red light',
                        threshold,
                        violation_type_data.min_streak_date,
                        violation_type_data.max_streak_date))

                elif (camera_violation_type == 'School Zone Speed Camera Violation' and
                      violation_type_data.max_streak >= threshold):

                    # add to container
                    response_chunks.append(L10N.DANGEROUS_VEHICLE_ABATEMENT_ACT_REPEAT_OFFENDER_STRING.format(
                        username_prefix,
                        violation_type_data.max_streak,
                        'school zone speed',
                        threshold,
                        violation_type_data.min_streak_date,
                        violation_type_data.max_streak_date))


        unique_link: str = self._get_website_plate_lookup_link(unique_identifier)

        website_link_string = f'View more details at {unique_link}.'
        response_chunks.append(website_link_string)

        # Send it back!
        return response_chunks

    def _form_summary_string(self,
                             summary: TrafficViolationsAggregatorResponse
                             ) -> Optional[str]:

        num_vehicles = len(summary.plate_lookups)
        vehicle_tickets = [sum(
            violation_type['count'] for violation_type in vehicle.violations)
                           for vehicle in summary.plate_lookups]
        total_tickets = sum(vehicle_tickets)

        fines_by_vehicle: List[FineData] = [lookup.fines for lookup in summary.plate_lookups]

        vehicles_with_fines: int = len([
            fine_data for fine_data in fines_by_vehicle if fine_data.fined > 0])

        aggregate_fines: FineData = FineData(**{
            field: sum(getattr(lookup.fines, field) for lookup
                       in summary.plate_lookups) for field in FineData.FINE_FIELDS})

        if aggregate_fines.fined > 0:
            return [
                f"You queried {num_vehicles} vehicles, of which "
                f"{vehicles_with_fines} vehicle{L10N.pluralize(vehicles_with_fines)} "
                f"{'has' if vehicles_with_fines == 1 else 'have collectively'} received {total_tickets} ticket{L10N.pluralize(total_tickets)} "
                f"with at least {'${:,.2f}'.format(aggregate_fines.fined - aggregate_fines.reduced)} "
                f"in fines, of which {'${:,.2f}'.format(aggregate_fines.paid)} has been paid.\n\n"]

    def _generate_unique_identifier(self):
        return ''.join(
            random.SystemRandom().choice(
                string.ascii_lowercase + string.digits) for _ in range(self.UNIQUE_IDENTIFIER_STRING_LENGTH))

    def _get_unique_identifier(self):
        unique_identifier = self._generate_unique_identifier()
        while PlateLookup.query.filter(PlateLookup.unique_identifier == unique_identifier).all():
            unique_identifier = self._generate_unique_identifier()
        return unique_identifier

    def _get_plate_query(self, request_object: Type[BaseLookupRequest], vehicle: Vehicle) -> PlateQuery:
        """Transform a request object into plate query"""

        created_at: str = datetime.strptime(request_object.created_at,
                                            twitter_constants.TWITTER_TIME_FORMAT).strftime(self.MYSQL_TIME_FORMAT)

        message_id: Optional[str] = request_object.external_id()
        message_source: str = request_object.message_source

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
                                             message_source=message_source,
                                             plate=plate,
                                             plate_types=plate_types,
                                             state=state,
                                             username=username)

        LOG.debug(f'plate_query: {plate_query}')

        return plate_query

    def _get_website_plate_lookup_link(self, unique_identifier: str) -> str:
        return f'{endpoints.HOWS_MY_DRIVING_NY_WEBSITE}/{unique_identifier}'

    def _handle_response_part_formation(self,
                                        count: str,
                                        collection: Dict[str, Any],
                                        continued_format_string: str,
                                        description: str,
                                        default_description: str,
                                        prefix_format_string: str,
                                        result_format_string: str,
                                        username_prefix: str):

        # collect the responses
        response_container = []

        # Initialize current string to prefix
        cur_string = username_prefix

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
            potential_response_length = len(cur_string + next_part)

            # If violation string so far and new part are less or
            # equal than 280 characters, append to existing tweet string.
            if (potential_response_length <= twitter_constants.MAX_TWITTER_STATUS_LENGTH):
                cur_string += next_part
            else:
                response_container.append(cur_string)
                if continued_format_string:
                    cur_string = username_prefix + continued_format_string
                else:
                    # Initialize current string to prefix
                    cur_string = username_prefix

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
                                 List[Campaign]) -> List[
            Tuple[str, int, int]]:

        LOG.debug('Performing lookup for campaigns.')

        result: List[Tuple[str, int, int]] = []

        for campaign in included_campaigns:

            subquery = campaign.plate_lookups.session.query(
                PlateLookup.plate, PlateLookup.state, func.max(PlateLookup.created_at).label(
                    'most_recent_campaign_lookup'),).group_by(
                PlateLookup.plate, PlateLookup.state).filter(
                    and_(PlateLookup.campaigns.any(Campaign.id.in_([campaign.id])), PlateLookup.count_towards_frequency == True)).subquery('subquery')

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
                (campaign.hashtag, campaign_vehicles, campaign_tickets))

        return result

    def _perform_plate_lookup(self,
                              campaigns: List[Campaign],
                              plate_query: PlateQuery,
                              unique_identifier: str) -> OpenDataServiceResponse:

        LOG.debug('Performing lookup for plate.')

        nyc_open_data_service: OpenDataService = OpenDataService()
        open_data_response: OpenDataServiceResponse = nyc_open_data_service.look_up_vehicle(
            plate_query=plate_query)

        LOG.debug(f'Violation data: {open_data_response}')

        if open_data_response.success:

            open_data_plate_lookup: OpenDataServicePlateLookup = open_data_response.data

            bus_lane_camera_violations = 0
            mobile_bus_lane_camera_violations = 0
            red_light_camera_violations = 0
            speed_camera_violations = 0

            for violation_type_summary in open_data_plate_lookup.violations:
                if violation_type_summary['title'] in self.CAMERA_VIOLATIONS:
                    violation_count = violation_type_summary['count']

                    if violation_type_summary['title'] == 'Bus Lane Violation':
                        bus_lane_camera_violations = violation_count
                    elif violation_type_summary['title'] == 'Failure To Stop At Red Light':
                        red_light_camera_violations = violation_count
                    elif violation_type_summary['title'] == 'Mobile Bus Lane Violation':
                        mobile_bus_lane_camera_violations = violation_count
                    elif violation_type_summary['title'] == 'School Zone Speed Camera Violation':
                        speed_camera_violations = violation_count

            total_bus_lane_camera_violations = (
                bus_lane_camera_violations + mobile_bus_lane_camera_violations)

            camera_streak_data: CameraStreakData = open_data_plate_lookup.camera_streak_data

            # If this came from message, add it to the plate_lookups table.
            if plate_query.message_source and plate_query.message_id and plate_query.created_at:
                new_lookup = PlateLookup(
                    boot_eligible_under_dvaa_threshold=(
                        camera_streak_data['Failure to Stop at Red Light'].max_streak >=
                        thresholds.DANGEROUS_VEHICLE_ABATEMENT_ACT_RED_LIGHT_CAMERA_THRESHOLD
                        if camera_streak_data['Failure to Stop at Red Light'] else False or
                        camera_streak_data['School Zone Speed Camera Violation'].max_streak >=
                        thresholds.DANGEROUS_VEHICLE_ABATEMENT_ACT_SCHOOL_ZONE_SPEED_CAMERA_THRESHOLD
                        if camera_streak_data['School Zone Speed Camera Violation'] else False),
                    boot_eligible_under_rdaa_threshold=(
                        camera_streak_data['Mixed'].max_streak >=
                        thresholds.RECKLESS_DRIVER_ACCOUNTABILITY_ACT_THRESHOLD
                        if camera_streak_data['Mixed'] else False),
                    bus_lane_camera_violations=total_bus_lane_camera_violations,
                    created_at=plate_query.created_at,
                    message_id=plate_query.message_id,
                    message_source=plate_query.message_source,
                    num_tickets=open_data_plate_lookup.num_violations,
                    plate=plate_query.plate,
                    plate_types=plate_query.plate_types,
                    red_light_camera_violations=red_light_camera_violations,
                    responded_to=True,
                    speed_camera_violations=speed_camera_violations,
                    state=plate_query.state,
                    unique_identifier=unique_identifier,
                    username=plate_query.username)

                # Iterate through included campaigns to tie lookup to each
                for campaign in campaigns:
                    # insert join record for campaign lookup
                    new_lookup.campaigns.append(campaign)

                # Insert plate lookup
                PlateLookup.query.session.add(new_lookup)
                PlateLookup.query.session.commit()

        else:
            LOG.info('open data plate lookup failed')

        return open_data_response

    def _process_invalid_vehicle(self,
                                 request_object: BaseLookupRequest,
                                 invalid_vehicle: Vehicle) -> InvalidVehicleResponse:

        """Process an invalid vehicle by notifying the user which necessary lookup
        elements were missing or incorrect and how to correct the issue in a
        subsequent tweet.s
        """

        plate_lookup_response_parts: List[Any]

        # Record the failed lookup.
        new_failed_lookup = FailedPlateLookup(
            message_id=request_object.external_id(),
            username=request_object.username())

        # Insert plate lookup
        FailedPlateLookup.query.session.add(new_failed_lookup)
        FailedPlateLookup.query.session.commit()

        # Legacy data where state is not a valid abbreviation.
        if invalid_vehicle.state:
            LOG.debug("We have a state, but it's invalid.")

            plate_lookup_response_parts = [
                f"The state should be two characters, but you supplied '{invalid_vehicle.state}'. "
                f"Please try again."]

        # '<state>:<plate>' format, but no valid state could be detected.
        elif invalid_vehicle.original_string:
            LOG.debug(
                "We don't have a state, but we have an attempted lookup with the new format.")

            plate_lookup_response_parts = [
                f"Sorry, a plate and state could not be inferred from "
                f"{invalid_vehicle.original_string}."]

        # If we have a plate, but no state.
        elif invalid_vehicle.plate:
            LOG.debug("We have a plate, but no state")

            plate_lookup_response_parts = [
                "Sorry, the state appears to be blank."]

        return InvalidVehicleResponse(response_parts=plate_lookup_response_parts)

    def _process_lookup_without_detected_vehicles(self,
                                                  request_object: BaseLookupRequest) -> NonVehicleResponse:

        """Process a lookup that had no detected vehicles, either partial or
        complete, by determining if the user was likely trying to submit a
        lookup, and if so, help them do so in a subsequent message.
        """

        non_vehicle_response_parts: List[Any]

        # Record the failed lookup.
        new_failed_lookup = FailedPlateLookup(
            message_id=request_object.external_id(),
            username=request_object.username())

        # Insert plate lookup
        FailedPlateLookup.query.session.add(new_failed_lookup)
        FailedPlateLookup.query.session.commit()

        LOG.debug('The data seems to be in the wrong format.')

        lookup_unique_identifier_matches = [regexp_constants.HMDNY_LOOKUP_PATTERN.search(
            s) != None for s in request_object.string_tokens()]
        state_matches = [regexp_constants.STATE_ABBREVIATIONS_PATTERN.search(
            s.upper()) != None for s in request_object.string_tokens()]
        number_matches = [regexp_constants.NUMBER_PATTERN.search(s.upper()) != None for s in list(filter(lambda part: re.sub(
            r'\.|@', '', part.lower()) not in set(request_object.mentioned_users), request_object.string_tokens()))]

        if any(lookup_unique_identifier_matches):
            # Do nothing here, since someone is probably sharing a website lookup.
            non_vehicle_response_parts = []
            LOG.debug(
                'Ignoring message since user quoting website lookup.')

        elif all([any(state_matches), any(number_matches)]):
            # We have what appears to be a plate and a state abbreviation.
            LOG.debug(
                'There is both plate and state information in this message.')

            # Let user know plate format
            non_vehicle_response_parts = [(
                "I’d be happy to look that up for you!\n\n"
                'Just a reminder, the format is '
                '<state|province|territory>:<plate>, e.g. NY:abc1234')]

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
                non_vehicle_response_parts = [(
                    "I think you're trying to look up a plate, but can't be sure.\n\n"
                    'Just a reminder, the format is '
                    '<state|province|territory>:<plate>, e.g. NY:abc1234')]

            # We have neither plate nor state. Do nothing.
            else:
                non_vehicle_response_parts = []
                LOG.debug(
                    'ignoring message since no plate or state information to respond to.')

        return NonVehicleResponse(response_parts=non_vehicle_response_parts)


    def _process_valid_vehicle(self,
                               campaigns: List[Campaign],
                               request_object: BaseLookupRequest,
                               vehicle: Vehicle) -> ValidVehicleResponse:

        """Process a valid plate by:

        1. searching open data
        2. saving the lookup
        3. returning the results

        """

        error_on_plate_lookup: bool = False
        plate_lookup_response_parts: List[Any]
        success_on_plate_lookup: bool = False
        plate_lookup: Optional[OpenDataServicePlateLookup] = None

        plate_query: PlateQuery = self._get_plate_query(vehicle=vehicle,
                                                        request_object=request_object)

        # do we have a previous lookup
        previous_lookup: Optional[PlateLookup] = self._query_for_previous_lookup(
            plate_query=plate_query)
        LOG.debug(f'Previous lookup for this vehicle: {previous_lookup}')

        # Obtain a unique identifier for the lookup
        unique_identifier: str = self._get_unique_identifier()

        # Do the real work!
        open_data_response: OpenDataServiceResponse = self._perform_plate_lookup(
            campaigns=campaigns,
            plate_query=plate_query,
            unique_identifier=unique_identifier)

        if open_data_response.success:

            # Record successful lookup.
            success_on_plate_lookup = True

            plate_lookup: OpenDataServicePlateLookup = open_data_response.data

            # how many times have we searched for this plate from a tweet
            current_frequency: int = self._query_for_lookup_frequency(plate_query)

            if plate_lookup.violations:

                plate_lookup_response_parts = self._form_plate_lookup_response_parts(
                    borough_data=plate_lookup.boroughs,
                    camera_streak_data=plate_lookup.camera_streak_data,
                    fine_data=plate_lookup.fines,
                    frequency=current_frequency,
                    lookup_source=request_object.message_source,
                    plate=plate_lookup.plate,
                    plate_types=plate_lookup.plate_types,
                    previous_lookup=previous_lookup,
                    state=plate_lookup.state,
                    username=request_object.username(),
                    unique_identifier=unique_identifier,
                    violations=plate_lookup.violations,
                    year_data=plate_lookup.years)

            else:
                # Let user know we didn't find anything.
                plate_types_string = (
                    f' (types: {plate_query.plate_types})') if plate_lookup.plate_types else ''

                plate_lookup_response_parts = L10N.NO_TICKETS_FOUND_STRING.format(
                    plate_query.state,
                    plate_lookup.plate,
                    plate_types_string)

        else:
            # Record lookup error.
            error_on_plate_lookup = True

            plate_lookup_response_parts = [
                f"Sorry, I received an error when looking up "
                f"{plate_query.state}:{plate_query.plate}"
                f"{(' (types: ' + plate_query.plate_types + ')') if plate_query.plate_types else ''}. "
                f"Please try again."]

        return ValidVehicleResponse(error_on_lookup=error_on_plate_lookup,
                                    plate_lookup=plate_lookup,
                                    response_parts=plate_lookup_response_parts,
                                    success_on_lookup=success_on_plate_lookup)

    def _query_for_lookup_frequency(self, plate_query: PlateQuery) -> int:
        """How many times has this plate been queried before?"""
        return len(PlateLookup.get_all_by(
            plate=plate_query.plate,
            plate_types=plate_query.plate_types,
            state=plate_query.state,
            count_towards_frequency=True))

    def _query_for_previous_lookup(self, plate_query: PlateQuery) -> Optional[PlateLookup]:
        """ See if we've seen this vehicle before. """

        lookups_for_vehicle: List[PlateLookup] = PlateLookup.get_all_by(
            plate=plate_query.plate,
            state=plate_query.state,
            plate_types=plate_query.plate_types,
            count_towards_frequency=True)

        if lookups_for_vehicle:
            lookups_for_vehicle.sort(key=lambda x: x.created_at, reverse=True)

            return lookups_for_vehicle[0]

        else:
            return None
