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

    @ddt.data(
        {
            'dry_run': True,
            'open_data_results': [{
                'count': 15,
                'plate': 'ABC1234',
                'state': 'NY'
            }]
        },
        {
            'offender_record_exists': True,
            'open_data_results': [{
                'count': 13,
                'plate': 'ABC1234',
                'state': 'NY'
            }, {
                'count': 11,
                'plate': 'ZYX9876',
                'state': 'NY'
            }]
        },
        {
            'open_data_results': [{
                'count': 9,
                'plate': 'ABC1234',
                'state': 'NY'
            }, {
                'count': 7,
                'plate': 'ZYX9876',
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

        mocked_open_data_covid_19_lookup.return_value = open_data_results

        if offender_record_exists:
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
            num_violations=offender['count'],
            plate=offender['plate'],
            plate_types=None,
            state=offender['state'],
            violations=[
                {'count': offender['count'],
                 'title': 'School Zone Speed Camera Violation'}],
            years=[])

        open_data_response = OpenDataServiceResponse(
            data=open_data_plate_lookup,
            success=True)

        mocked_open_data_service_look_up_vehicle.return_value = open_data_response

        # red_light_camera_violations_string = (
        #     f'{red_light_camera_violations_after_previous_lookup} | Red Light Camera Violations\n'
        #     if red_light_camera_violations_after_previous_lookup else '')

        speed_camera_violations_string = (
            f"{offender['count']} | Speed Safety Camera Violations\n"
            if int(offender['count']) else '')

        covid_19_reckless_driver_string = (
            'From March 10, 2020 to March 30, 2020, '
            f"#{offender['state']}_{offender['plate']} "
            f"received {offender['count']} camera violations:\n\n"
            f'{speed_camera_violations_string}')

        dval_string = (
            'At this rate, this vehicle will receive '
            f"{round(366.0/21 * int(offender['count']))} "
            'speed safety camera violations over '
            'a year, qualifying it for towing or booting under '
            '@bradlander\'s Dangerous Vehicle Abatement Law and '
            'requiring its driver to take a course on the consequences '
            'of reckless driving.')

        speeding_string = (
            'With such little traffic, many drivers are speeding '
            'regularly, putting New Yorkers at increased risk of '
            'ending up in a hospital at a time our hospitals are '
            'stretched to their limits. It\'s also hard to practice '
            'social distancing when walking on our narrow sidewalks.')

        open_streets_string = (
            'Let\'s solve two problems, @NYCMayor, '
            'by opening more streets for people to walk safely.')

        job.run(is_dry_run=dry_run)

        if not dry_run:
            mocked_traffic_violations_tweeter_send_status.assert_called_with(
                message_parts=[covid_19_reckless_driver_string, dval_string,
                    [speeding_string, open_streets_string]],
                on_error_message=(
                    f'Error printing COVID-19 reckless driver update. '
                    f'Tagging @bdhowald.'))
