import logging
import os
import re
import requests
import requests_futures.sessions

from collections import Counter
from datetime import datetime
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Any, Dict, List, Optional, Tuple

from traffic_violations import settings
from traffic_violations.constants.borough_codes import BOROUGH_CODES
from traffic_violations.constants.open_data.endpoints import \
    FISCAL_YEAR_DATABASE_ENDPOINTS, MEDALLION_ENDPOINT, \
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT
from traffic_violations.constants.open_data.needed_fields import \
    FISCAL_YEAR_DATABASE_NEEDED_FIELDS, \
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS, \
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_FINE_KEYS
from traffic_violations.constants.open_data.violations import \
    CAMERA_VIOLATIONS, CAMERA_STREAK_DATA_TYPES, \
    HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS, \
    HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS
from traffic_violations.constants.precincts import PRECINCTS_BY_BOROUGH

from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.fine_data import FineData
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_plate_lookup import (
    OpenDataServicePlateLookup)
from traffic_violations.models.response.open_data_service_response import (
    OpenDataServiceResponse)
from traffic_violations.models.special_purpose.covid_19_camera_offender import (
    Covid19CameraOffender)

from traffic_violations.services.constants.exceptions import \
    APIFailureException
from traffic_violations.services.apis.location_service import LocationService

LOG = logging.getLogger(__name__)


class OpenDataService:

    MAX_RESULTS = 10_000

    MEDALLION_PATTERN = re.compile(r'^[0-9][A-Z][0-9]{2}$')
    MEDALLION_PLATE_KEY = 'dmv_license_plate_number'

    OPEN_DATA_TOKEN = os.getenv('NYC_OPEN_DATA_TOKEN')

    OUTPUT_FINE_KEYS = ['fined', 'paid', 'reduced', 'outstanding']

    TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

    def __init__(self):
        # Set up retry ability
        s_req = requests_futures.sessions.FuturesSession(max_workers=9)

        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[403, 500, 502, 503, 504],
                        raise_on_status=False)

        s_req.mount('https://', HTTPAdapter(max_retries=retries))

        self.api = s_req

        self.location_service = LocationService()

    def lookup_covid_19_camera_violations(self) -> List[Dict[str, str]]:
        """Search for all camera violations for all vehicles between two
        timestamps.
        """

        # Ugly hardcoded query, but necessary since
        # Open Parking and Camera Violations has a string 'issue_date',
        # meaning that queries must use LIKE operators
        open_parking_and_camera_violations_query_string: str = (
            f'{OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT}?'
             '$select=plate,state,count(summons_number)%20as%20total_camera_violations,'
             'sum(case%20when%20violation='
             '%27PHTO%20SCHOOL%20ZN%20SPEED%20VIOLATION%27'
             '%20then%201%20else%200%20end)%20as%20speed_camera_count,'
             'sum(case%20when%20violation='
             '%27FAILURE%20TO%20STOP%20AT%20RED%20LIGHT%27'
             '%20then%201%20else%200%20end)%20as%20red_light_camera_count&'
             '$where=violation%20in%20('
             '%27PHTO%20SCHOOL%20ZN%20SPEED%20VIOLATION%27,'
             '%27FAILURE%20TO%20STOP%20AT%20RED%20LIGHT%27'
             ')%20and%20('
             'issue_date%20LIKE%20%2703/1_/2020%27%20or%20'
             'issue_date%20LIKE%20%2703/2_/2020%27%20or%20'
             'issue_date%20LIKE%20%2703/3_/2020%27%20or%20'
             'issue_date%20LIKE%20%2704/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2705/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2706/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2707/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2708/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2709/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2710/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/__/2020%27%20or%20'
             'issue_date%20LIKE%20%270_/__/2021%27%20or%20'
             'issue_date%20LIKE%20%2710/__/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/0_/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/1_/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/20/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/21/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/22/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/23/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/24/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/25/2020%27%20or%20'
             'issue_date%20LIKE%20%2711/26/2020%27%20or%20'
             ')&'
             '$group=plate,state&$order=count%20desc')

        open_parking_and_camera_violations_response: Dict[str, str] = self._perform_query(
            query_string=open_parking_and_camera_violations_query_string)

        return open_parking_and_camera_violations_response['data']



    def look_up_vehicle(self,
                       plate_query: PlateQuery,
                       since: datetime = None,
                       until: datetime = None) -> OpenDataServiceResponse:
        try:
            lookup_result: OpenDataServicePlateLookup = self._perform_all_queries(
                plate_query=plate_query,
                since=since,
                until=until)

            return OpenDataServiceResponse(
                data=lookup_result,
                success=True)

        except APIFailureException as exc:
            LOG.error(str(exc))

            return OpenDataServiceResponse(
                message=str(exc),
                success=False)

    def _add_fine_data_for_open_parking_and_camera_violations_summons(self, summons) -> Dict[str, Any]:
        for output_key in self.OUTPUT_FINE_KEYS:
            summons[output_key] = 0

        for fine_key in OPEN_PARKING_AND_CAMERA_VIOLATIONS_FINE_KEYS:
            if fine_key in summons:
                try:
                    amount = float(summons[fine_key])

                    if fine_key in ['fine_amount', 'interest_amount', 'penalty_amount']:
                        summons['fined'] += amount

                    elif fine_key == 'reduction_amount':
                        summons['reduced'] += amount

                    elif fine_key == 'amount_due':
                        summons['outstanding'] += amount

                    elif fine_key == 'payment_amount':
                        summons['paid'] += amount

                except ValueError as ve:

                    LOG.error('Error parsing value into float')
                    LOG.error(e)
                    LOG.error(str(e))
                    LOG.error(e.args)
                    logging.exception("stack trace")

                    pass

        return summons

    def _add_query_limit_and_token(self, url: str) -> str:
        return f'{url}&$limit={self.MAX_RESULTS}&$$app_token={self.OPEN_DATA_TOKEN}'

    def _calculate_aggregate_data(self, plate_query: PlateQuery, violations) -> OpenDataServicePlateLookup:
        # Marshal all ticket data into form.
        fines: FineData = FineData(
            fined=round(sum(v['fined']
                      for v in violations.values() if v.get('fined')), 2),
            reduced=round(sum(v['reduced']
                        for v in violations.values() if v.get('reduced')), 2),
            paid=round(sum(v['paid'] for v in violations.values() if v.get('paid')), 2),
            outstanding=round(sum(v['outstanding']
                            for v in violations.values() if v.get('outstanding')), 2)
        )

        tickets: List[Tuple[str, int]] = Counter([v['violation'].title() for v in violations.values(
        ) if v.get('violation') is not None]).most_common()

        years: List[Tuple[str, int]] = Counter([datetime.strptime(v['issue_date'], self.TIME_FORMAT).strftime('%Y') if v.get(
            'has_date') else 'No Year Available' for v in violations.values()]).most_common()

        boroughs: List[Tuple[str, int]] = Counter([v['borough'].title() for v in violations.values(
        ) if v.get('borough')]).most_common()

        camera_violations_by_type: Dict[str, CameraStreakData] = { violation_type: self._find_max_camera_violations_streak(
            sorted([datetime.strptime(v['issue_date'], self.TIME_FORMAT) for v in violations.values(
            ) if v.get('violation') == violation_type])) for violation_type in CAMERA_VIOLATIONS }

        camera_violations_by_type['Mixed'] = self._find_max_camera_violations_streak(
            sorted([datetime.strptime(v['issue_date'], self.TIME_FORMAT) for v in violations.values(
            ) if v.get('violation') and v['violation'] in CAMERA_VIOLATIONS]))

        return OpenDataServicePlateLookup(
            boroughs=[{'count': v, 'title': k.title()} for k, v in boroughs],
            camera_streak_data=camera_violations_by_type,
            fines=fines,
            num_violations=len(violations),
            plate=plate_query.plate,
            plate_types=plate_query.plate_types,
            state=plate_query.state,
            violations=[{'count': v, 'title': k.title()} for k, v in tickets],
            years=sorted([{'count': v, 'title': k.title()}
                          for k, v in years], key=lambda k: k['title'])
        )

    def _find_max_camera_violations_streak(self,
                                           list_of_violation_times: List[datetime]) -> Optional[CameraStreakData]:

        if list_of_violation_times:
            max_streak = 0
            min_streak_date = None
            max_streak_date = None

            for date in list_of_violation_times:

                LOG.debug(f'date: {date}')

                year_later = date + \
                    (datetime(date.year + 1, 1, 1) - datetime(date.year, 1, 1))
                LOG.debug(f'year_later: {year_later}')

                year_long_tickets = [
                    comp_date for comp_date in list_of_violation_times if date <= comp_date < year_later]
                this_streak = len(year_long_tickets)

                if this_streak > max_streak:

                    max_streak = this_streak
                    min_streak_date = year_long_tickets[0]
                    max_streak_date = year_long_tickets[-1]

            return CameraStreakData(
                min_streak_date=min_streak_date.strftime('%B %-d, %Y'),
                max_streak=max_streak,
                max_streak_date=max_streak_date.strftime('%B %-d, %Y'))

        return None

    def _merge_violations(self,
                          original_dict: Dict[str, Any],
                          overwrite_dict: Dict[str, Any]) -> Dict[str, Any]:

        merged_dict: Dict[str, Any] = {**original_dict, **overwrite_dict}

        for key, value in merged_dict.items():
            if key in original_dict and key in overwrite_dict:
                merged_dict[key] = {**original_dict[key], **overwrite_dict[key]}

                if merged_dict[key].get('violation') is None:
                    merged_dict[key]['violation'] = "No Violation Description Available"

                if merged_dict[key].get('borough') is None:
                    merged_dict[key]['borough'] = 'No Borough Available'

        return merged_dict

    def _normalize_fiscal_year_database_summons(self, summons) -> Dict[str, Any]:
        # get human readable ticket type name
        if summons.get('violation_description') is None:
            if summons.get('violation_code') and HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(summons['violation_code']):
                summons['violation'] = HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(
                    summons['violation_code'])
        else:
            if HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(summons['violation_description']):
                summons['violation'] = HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(
                    summons['violation_description'])
            else:
                summons['violation'] = re.sub(
                    '[0-9]*-', '', summons['violation_description'])

        if summons.get('issue_date') is None:
            summons['has_date'] = False
        else:
            try:
                summons['issue_date'] = datetime.strptime(
                    summons['issue_date'], self.TIME_FORMAT).strftime(self.TIME_FORMAT)
                summons['has_date'] = True
            except ValueError as ve:
                summons['has_date'] = False

        if summons.get('violation_precinct') is not None:
            boros = [boro for boro, precincts in PRECINCTS_BY_BOROUGH.items() if int(
                summons['violation_precinct']) in precincts]
            if boros:
                summons['borough'] = boros[0]
            else:
                if summons.get('violation_county') is not None:
                    boros = [name for name, codes in BOROUGH_CODES.items(
                    ) if summons.get('violation_county') in codes]
                    if boros:
                        summons['borough'] = boros[0]
                else:
                    if summons.get('street_name') is not None:
                        street_name = summons.get('street_name')
                        intersecting_street = summons.get(
                            'intersecting_street') or ''

                        geocoded_borough = self.location_service.get_borough_from_location_strings(
                            [street_name, intersecting_street])
                        if geocoded_borough:
                            summons['borough'] = geocoded_borough.lower()

        return summons

    def _normalize_open_parking_and_camera_violations_summons(self, summons) -> Dict[str, Any]:
        # get human readable ticket type name
        if summons.get('violation') is not None:
            summons['violation'] = HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS[
                summons['violation']]

        # normalize the date
        if summons.get('issue_date') is None:
            summons['has_date'] = False

        else:
            try:
                summons['issue_date'] = datetime.strptime(
                    summons['issue_date'], '%m/%d/%Y').strftime(self.TIME_FORMAT)
                summons['has_date'] = True
            except ValueError as ve:
                summons['has_date'] = False

        if summons.get('precinct') is not None:
            boros = [boro for boro, precincts in PRECINCTS_BY_BOROUGH.items() if int(
                summons['precinct']) in precincts]
            if boros:
                summons['borough'] = boros[0]
            else:
                if summons.get('county') is not None:
                    boros = [name for name, codes in BOROUGH_CODES.items(
                    ) if summons.get('county') in codes]
                    if boros:
                        summons['borough'] = boros[0]

        summons = self._add_fine_data_for_open_parking_and_camera_violations_summons(
            summons=summons)

        return summons

    def _perform_all_queries(self,
                             plate_query: PlateQuery,
                             since: datetime,
                             until: datetime) -> OpenDataServicePlateLookup:

        if self.MEDALLION_PATTERN.search(plate_query.plate) is not None:
            plate_query = self._perform_medallion_query(
                plate_query=plate_query)

        opacv_result: Dict[str, Any] = self._perform_open_parking_and_camera_violations_query(
            plate_query=plate_query, since=since, until=until)
        fiscal_year_result: Dict[str, Any] = self._perform_fiscal_year_database_queries(
            plate_query=plate_query, since=since, until=until)

        violations: Dict[str, Any] = self._merge_violations(opacv_result, fiscal_year_result)

        return self._calculate_aggregate_data(plate_query=plate_query,
                                              violations=violations)


    def _perform_fiscal_year_database_queries(self,
                                              plate_query: PlateQuery,
                                              since: datetime,
                                              until: datetime) -> Dict[str, Any]:
        """Grab data from each of the fiscal year violation datasets"""

        violations: Dict[str, Any] = {}

        # iterate through the endpoints
        for year, endpoint in FISCAL_YEAR_DATABASE_ENDPOINTS.items():

            fiscal_year_database_query_string: str = (
                f"{endpoint}?"
                f"plate_id={plate_query.plate}&"
                f"registration_state={plate_query.state}"
                f"{'&$where=plate_type%20in(' + ','.join(['%27' + type + '%27' for type in plate_query.plate_types.split(',')]) + ')' if plate_query.plate_types is not None else ''}")

            fiscal_year_database_response: Dict[str, Any] = self._perform_query(
                query_string=fiscal_year_database_query_string)

            fiscal_year_database_data: Dict[str, str] = \
                fiscal_year_database_response['data']

            LOG.debug(
                f'Fiscal year data for {plate_query.state}:{plate_query.plate}'
                f'{":" + plate_query.plate_types if plate_query.plate_types else ""} for {year}: '
                f'{fiscal_year_database_data}')

            for record in fiscal_year_database_data:
                record = self._normalize_fiscal_year_database_summons(
                    summons=record)

                # structure response and only use the data we need
                new_data: Dict[str, Any] = {
                    needed_field: record.get(needed_field) for needed_field in FISCAL_YEAR_DATABASE_NEEDED_FIELDS}

                if new_data.get('has_date'):
                    try:
                        issue_date: datetime = datetime.strptime(new_data['issue_date'], self.TIME_FORMAT)

                        if (since is None or issue_date >= since) and (until is None or issue_date <= until):
                            violations[new_data['summons_number']] = new_data

                        else:
                            LOG.debug(
                                f"record {new_data['summons_number']} ({new_data['issue_date']}) "
                                f"is not within time range "
                                f"({since.strftime(self.TIME_FORMAT) if since else 'any'}<->"
                                f"{until.strftime(self.TIME_FORMAT) if until else 'any'}")

                        continue

                    except ValueError as ve:
                        LOG.info(f"Issue time could not be determined for {new_data['summons_number']}")

                violations[new_data['summons_number']] = new_data

        return violations

    def _perform_medallion_query(self, plate_query: PlateQuery
                                 ) -> PlateQuery:
        medallion_query_string: str = (
            f'{MEDALLION_ENDPOINT}?'
            f'license_number={plate_query.plate}')

        LOG.debug(
            f'Querying medallion data from {medallion_query_string}')

        medallion_response: Dict[str, Dict[str, Any]] = self._perform_query(query_string=medallion_query_string)

        medallion_data: Dict[str, Any] = medallion_response['data']

        LOG.debug(
            f'Medallion data for {plate_query.state}:{plate_query.plate} – '
            f'{medallion_data}')

        if medallion_data:
            sorted_list: Dict[str, Any] = sorted(
                set([res[self.MEDALLION_PLATE_KEY] for res in medallion_data]))

            return PlateQuery(
                created_at=plate_query.created_at,
                message_source=plate_query.message_source,
                message_id=plate_query.message_id,
                plate=sorted_list[-1],
                plate_types=plate_query.plate_types,
                state=plate_query.state,
                username=plate_query.username)

        return plate_query

    def _perform_open_parking_and_camera_violations_query(self,
                                                          plate_query: PlateQuery,
                                                          since: datetime,
                                                          until: datetime) -> Dict[str, Any]:
        """Grab data from 'Open Parking and Camera Violations'"""

        violations: Dict[str, Any] = {}

        # response from city open data portal
        open_parking_and_camera_violations_query_string: str = (
            f'{OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT}?'
            f'plate={plate_query.plate}&'
            f'state={plate_query.state}'
            f"{'&$where=license_type%20in(' + ','.join(['%27' + type + '%27' for type in plate_query.plate_types.split(',')]) + ')' if plate_query.plate_types is not None else ''}")

        open_parking_and_camera_violations_response: Dict[str, Any] = self._perform_query(
            query_string=open_parking_and_camera_violations_query_string)

        open_parking_and_camera_violations_data: List[Dict[str, Any]] = \
            open_parking_and_camera_violations_response['data']

        LOG.debug(
            f'Open Parking and Camera Violations data for {plate_query.state}:{plate_query.plate}'
            f'{":" + plate_query.plate_types if plate_query.plate_types else ""}: '
            f'{open_parking_and_camera_violations_data}')

        # only data we're looking for
        opacv_desired_keys = OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS

        # add violation if it's missing
        for record in open_parking_and_camera_violations_data:
            record = self._normalize_open_parking_and_camera_violations_summons(
                summons=record)

            new_data = {needed_field: record.get(needed_field) for needed_field in opacv_desired_keys}

            if new_data.get('has_date'):
                try:
                    issue_date: datetime = datetime.strptime(new_data['issue_date'], self.TIME_FORMAT)

                    if (since is None or issue_date >= since) and (until is None or issue_date <= until):
                        violations[new_data['summons_number']] = new_data

                    else:
                        LOG.debug(
                            f"record {new_data['summons_number']} ({new_data['issue_date']}) "
                            f"is not within time range "
                            f"({since.strftime(self.TIME_FORMAT) if since else 'any'}<->"
                            f"{until.strftime(self.TIME_FORMAT) if until else 'any'}")

                    continue

                except ValueError as ve:
                    LOG.info(f"Issue time could not be determined for {new_data['summons_number']}")

            violations[new_data['summons_number']] = new_data

        return violations

    def _perform_query(self, query_string: str) -> Dict[str, Any]:
        full_url: str = f'{self._add_query_limit_and_token(query_string)}'
        response = self.api.get(full_url)

        result = response.result()

        if result.status_code in range(200, 300):
            # Only attempt to read json on a successful response.
            return {'data': result.json()}
        elif result.status_code in range(300, 400):
            raise APIFailureException(
                f'redirect error when accessing {query_string}')
        elif result.status_code in range(400, 500):
            raise APIFailureException(
                f'user error when accessing {query_string}')
        elif result.status_code in range(500, 600):
            raise APIFailureException(
                f'server error when accessing {query_string}')
        else:
            raise APIFailureException(
                f'unknown error when accessing {query_string}')
