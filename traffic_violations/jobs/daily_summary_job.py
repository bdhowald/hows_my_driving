import logging
import argparse
import os
import pytz
import statistics

from datetime import datetime, time, timedelta
from sqlalchemy import and_
from sqlalchemy.sql.expression import func
from typing import List

from traffic_violations.constants import L10N

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.plate_lookup import PlateLookup

from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter

LOG = logging.getLogger(__name__)


class DailySummaryJob(BaseJob):

    def perform(self, *args, **kwargs):
        """ Tweet out daily summary of yesterday's lookups """

        is_dry_run: bool = kwargs.get('is_dry_run') or False

        tweeter = TrafficViolationsTweeter()

        utc = pytz.timezone('UTC')
        eastern = pytz.timezone('US/Eastern')

        today = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(days=1)).astimezone(utc)
        end_of_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(seconds=1)).astimezone(utc)

        # find all of yesterday's lookups, using only the most
        # recent of yesterday's queries for a vehicle.
        subquery = PlateLookup.query.session.query(
            PlateLookup.plate, PlateLookup.state, func.max(
                PlateLookup.id).label('most_recent_vehicle_lookup')
        ).filter(
            and_(PlateLookup.created_at >= midnight_yesterday,
                 PlateLookup.created_at <= end_of_yesterday,
                 PlateLookup.count_towards_frequency == True)
        ).group_by(
            PlateLookup.plate,
            PlateLookup.state
        ).subquery('subquery')

        full_query = PlateLookup.query.join(subquery,
                                            (PlateLookup.id == subquery.c.most_recent_vehicle_lookup))

        yesterdays_lookups: List[PlateLookup] = full_query.all()

        num_lookups: int = len(yesterdays_lookups)
        ticket_counts: int = [
            lookup.num_tickets for lookup in yesterdays_lookups]
        total_tickets: int = sum(ticket_counts)
        num_empty_lookups: int = len([
            lookup for lookup in yesterdays_lookups if lookup.num_tickets == 0])
        num_reckless_drivers: int = len([
            lookup for lookup in yesterdays_lookups if lookup.boot_eligible == True])

        total_reckless_drivers = PlateLookup.query.session.query(
            PlateLookup.plate, PlateLookup.state
        ).distinct().filter(
            and_(PlateLookup.boot_eligible == True,
                 PlateLookup.count_towards_frequency)).count()

        lookups_summary_string = (
            f'On {midnight_yesterday.strftime("%A, %B %-d, %Y")}, '
            f"users requested {num_lookups} lookup{L10N.pluralize(num_lookups)}. ")

        if num_lookups > 0:

            median = statistics.median(ticket_counts)

            lookups_summary_string += (
                f"{'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have'} "
                f"received {'{:,}'.format(total_tickets)} ticket{L10N.pluralize(total_tickets)} "
                f"for an average of {round(total_tickets / num_lookups, 2)} ticket{L10N.pluralize(total_tickets / num_lookups)} "
                f"and a median of {median} ticket{L10N.pluralize(median)} per vehicle. "
                f"{num_empty_lookups} lookup{L10N.pluralize(num_empty_lookups)} returned no tickets.")

        reckless_drivers_summary_string = (
            f"{num_reckless_drivers} {'vehicle was' if num_reckless_drivers == 1 else 'vehicles were'} "
            f"eligible to be booted or impounded under @bradlander's "
            f"proposed legislation ({'{:,}'.format(total_reckless_drivers)} such lookups "
            f"since June 6, 2018).")

        messages: List[str] = [
          lookups_summary_string,
          reckless_drivers_summary_string]

        if not is_dry_run:
            success: bool = tweeter.send_status(
                message_parts=messages,
                on_error_message=(
                    f'Error printing daily summary. '
                    f'Tagging @bdhowald.'))

            if success:
                LOG.debug('Daily summary plate ran successfully.')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Perform lookups to collect yesterday\'s statistics.')

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't tweet results")

    return parser.parse_args()

if __name__ == '__main__':
    arguments = parse_args()

    job = DailySummaryJob()
    job.run(is_dry_run=arguments.dry_run)