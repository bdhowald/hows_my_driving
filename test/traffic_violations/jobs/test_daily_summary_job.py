import ddt
import mock
import pytz
import random
import statistics
import unittest

from datetime import datetime, timezone, time, timedelta
from unittest.mock import call, MagicMock

from traffic_violations.jobs.daily_summary_job import DailySummaryJob

from traffic_violations.models.repeat_camera_offender import \
    RepeatCameraOffender


@ddt.ddt
class TestDailySummaryJob(unittest.TestCase):

    @ddt.data(
        {
            'dry_run': True
        },
        {
            'dry_run': False
        }
    )
    @mock.patch(
        'traffic_violations.jobs.daily_summary_job.PlateLookup.query')
    @mock.patch(
        'traffic_violations.jobs.daily_summary_job.TrafficViolationsTweeter')
    def test_print_daily_summary(self,
                                 mocked_traffic_violations_tweeter,
                                 mocked_plate_lookup_query,
                                 dry_run):

        job = DailySummaryJob()

        utc = pytz.timezone('UTC')
        eastern = pytz.timezone('US/Eastern')

        today = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(days=1)).astimezone(utc)

        message_id = random.randint(10000000000000000000, 20000000000000000000)

        plate_lookups = []
        for _ in range(random.randint(5, 20)):
            lookup = MagicMock()
            lookup.num_tickets = random.randint(1, 200)
            lookup.boot_eligible = random.random() >= 0.5
            plate_lookups.append(lookup)

        num_lookups = len(plate_lookups)
        ticket_counts = [
            plate_lookup.num_tickets for plate_lookup in plate_lookups]
        total_tickets = sum(ticket_counts)
        num_empty_lookups = len([
            lookup for lookup in plate_lookups if lookup.num_tickets == 0])
        num_reckless_drivers = len([
            lookup for lookup in plate_lookups if lookup.boot_eligible == True])

        mocked_plate_lookup_query.join().all.return_value = plate_lookups

        total_reckless_drivers = random.randint(10, 10000)

        mocked_plate_lookup_query.session.query(
        ).distinct().filter().count.return_value = total_reckless_drivers

        median = statistics.median(
            plate_lookup.num_tickets for plate_lookup in plate_lookups)

        lookup_string = (
            f"On {midnight_yesterday.strftime('%A, %B %-d, %Y')}, users requested {num_lookups} lookup{'' if num_lookups == 1 else 's'}. "
            f"{'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have'} received {'{:,}'.format(total_tickets)} ticket{'' if total_tickets == 1 else 's'} "
            f"for an average of {round(total_tickets / num_lookups, 2)} ticket{'' if total_tickets == 1 else 's'} "
            f"and a median of {median} ticket{'' if median == 1 else 's'} per vehicle. "
            f"{num_empty_lookups} lookup{'' if num_empty_lookups == 1 else 's'} returned no tickets.")

        reckless_string = (
            f"{num_reckless_drivers} {'vehicle was' if num_reckless_drivers == 1 else 'vehicles were'} "
            f"eligible to be booted or impounded under @bradlander's proposed legislation "
            f"({total_reckless_drivers} such lookups since June 6, 2018).")

        job.run(is_dry_run=dry_run)

        if not dry_run:
            mocked_traffic_violations_tweeter().send_status.assert_called_with(
                message_parts=[lookup_string, reckless_string],
                on_error_message=(
                    f'Error printing featured plate. '
                    f'Tagging @bdhowald.'))
