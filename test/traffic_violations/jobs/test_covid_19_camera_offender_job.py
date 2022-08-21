import datetime
import ddt
import mock
import unittest

from typing import Dict
from unittest.mock import call, MagicMock

from traffic_violations.jobs.covid_19_camera_offender_job import (
    Covid19CameraOffenderJob)

from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse


@ddt.ddt
class TestCovid19CameraOffenderJob(unittest.TestCase):

    DVAL_SPEED_CAMERA_THRESHOLD = 15

    @ddt.data(
        {
            'dry_run': True,
            'open_data_results': [{
                'plate': 'ABC1234',
                'red_light_camera_count': 13,
                'speed_camera_count': 78,
                'total_camera_violations': 91,
                'state': 'NY'
            }]
        },
        {
            'offender_record_exists': True,
            'open_data_results': [{
                'plate': 'ABC1234',
                'red_light_camera_count': 2,
                'speed_camera_count': 179,
                'total_camera_violations': 181,
                'state': 'NY'
            }, {
                'plate': 'ZYX9876',
                'red_light_camera_count': 22,
                'speed_camera_count': 59,
                'total_camera_violations': 81,
                'state': 'NY'
            }]
        },
        {
            'open_data_results': [{
                'plate': 'ABC1234',
                'red_light_camera_count': 7,
                'speed_camera_count': 23,
                'total_camera_violations': 30,
                'state': 'NY'
            }, {
                'plate': 'ZYX9876',
                'red_light_camera_count': 20,
                'speed_camera_count': 634,
                'total_camera_violations': 654,
                'state': 'NY'
            }]
        }
    )
    @mock.patch(
        'traffic_violations.jobs.covid_19_camera_offender_job.OpenDataService.look_up_vehicle')
    @mock.patch(
        'traffic_violations.jobs.covid_19_camera_offender_job.Covid19CameraOffender.get_by')
    @mock.patch(
        'traffic_violations.jobs.covid_19_camera_offender_job.TrafficViolationsTweeter.send_status')
    @mock.patch(
        'traffic_violations.jobs.covid_19_camera_offender_job.OpenDataService.lookup_covid_19_camera_violations')
    @ddt.unpack
    def test_print_covid_19_camera_offender_message(self,
                                 mocked_open_data_covid_19_lookup: MagicMock,
                                 mocked_traffic_violations_tweeter_send_status: MagicMock,
                                 mocked_covid_19_camera_offender_get_by: MagicMock,
                                 mocked_open_data_service_look_up_vehicle: MagicMock,
                                 open_data_results: Dict[str, str],
                                 dry_run: bool = False,
                                 offender_record_exists: bool = False):

        job = Covid19CameraOffenderJob()

        start_date = datetime.date(2020, 3, 10)
        end_date = datetime.date.today()

        days_in_period = (end_date - start_date).days
        num_years = days_in_period / 365.0

        mocked_open_data_covid_19_lookup.return_value = open_data_results

        if offender_record_exists or (
            (open_data_results[0]['speed_camera_count']/num_years) < self.DVAL_SPEED_CAMERA_THRESHOLD):
            mocked_covid_19_camera_offender_get_by.side_effect = [
                MagicMock(name='offender_record'), None]
            offender = open_data_results[1]
        else:
            mocked_covid_19_camera_offender_get_by.return_value = None
            offender = open_data_results[0]

        open_data_plate_lookup = OpenDataServicePlateLookup(
            boroughs=[],
            camera_streak_data=None,
            fines=MagicMock(),
            num_violations=offender['red_light_camera_count'] + offender['speed_camera_count'],
            plate=offender['plate'],
            plate_types=None,
            state=offender['state'],
            violations=[
                {'count': offender['red_light_camera_count'],
                 'title': 'Failure To Stop At Red Light'},
                {'count': offender['speed_camera_count'],
                 'title': 'School Zone Speed Camera Violation'}],
            years=[])

        open_data_response = OpenDataServiceResponse(
            data=open_data_plate_lookup,
            success=True)

        mocked_open_data_service_look_up_vehicle.return_value = open_data_response

        red_light_camera_violations_string = (
            f"{offender['red_light_camera_count']} | Red Light Camera Violations\n"
            if int(offender['red_light_camera_count']) else '')

        speed_camera_violations_string = (
            f"{offender['speed_camera_count']} | Speed Safety Camera Violations\n"
            if int(offender['speed_camera_count']) else '')

        covid_19_reckless_driver_string = (
            f"From March 10, 2020 to {end_date.strftime('%B %-d, %Y')}, "
            f"#{offender['state']}_{offender['plate']} "
            f"received {offender['red_light_camera_count'] + offender['speed_camera_count']} "
            f'camera violations:\n\n'
            f'{red_light_camera_violations_string}'
            f'{speed_camera_violations_string}')

        dval_string = (
            'This vehicle has received an average of '
            f"{round(offender['speed_camera_count'] / num_years, 1)} "
            'speed safety camera violations per '
            'year, qualifying it for towing or booting under '
            '@bradlander\'s Dangerous Vehicle Abatement Law and '
            'requiring its driver to take a course on the consequences '
            'of reckless driving.')

        job.run(is_dry_run=dry_run)

        if not dry_run:
            mocked_traffic_violations_tweeter_send_status.assert_called_with(
                # message_parts=[covid_19_reckless_driver_string, dval_string,
                #     [speeding_string, open_streets_string]],
                message_parts=[covid_19_reckless_driver_string, dval_string],
                on_error_message=(
                    f'Error printing COVID-19 reckless driver update. '
                    f'Tagging @bdhowald.'))
