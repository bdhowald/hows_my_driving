import logging
import argparse
import os
import pytz
import statistics

from datetime import datetime, time, timedelta
from sqlalchemy import and_
from sqlalchemy.sql.expression import func
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

        yesterdays_lookups: list[PlateLookup] = full_query.all()

        num_lookups: int = len(yesterdays_lookups)
        ticket_counts: int = [
            lookup.num_tickets for lookup in yesterdays_lookups]
        total_tickets: int = sum(ticket_counts)
        num_empty_lookups: int = len([
            lookup for lookup in yesterdays_lookups if lookup.num_tickets == 0])
        num_rdaa_drivers: int = len([
            lookup for lookup in yesterdays_lookups if lookup.boot_eligible_under_rdaa_threshold == True])
        num_dvaa_drivers: int = len([
            lookup for lookup in yesterdays_lookups if lookup.boot_eligible_under_dvaa_threshold == True])

        base_query = PlateLookup.query.session.query(
            PlateLookup.plate, PlateLookup.state
        ).distinct()

        total_rdaa_drivers = base_query.filter(
            and_(PlateLookup.boot_eligible_under_rdaa_threshold == True,
                 PlateLookup.count_towards_frequency)).count()

        total_dvaa_drivers = base_query.filter(
            and_(PlateLookup.boot_eligible_under_dvaa_threshold == True,
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

        rdaa_summary_string = (
            f"{num_rdaa_drivers} {'vehicle was' if num_rdaa_drivers == 1 else 'vehicles were'} "
            f"eligible to be booted or impounded under the Reckless Driver Accountability Act "
            f"(>= 5 camera violations in 12 months). "
            f"There have been {'{:,}'.format(total_rdaa_drivers)} such vehicles queried "
            f"since June 6, 2018.")

        dvaa_summary_string = (
            f"{num_dvaa_drivers} {'vehicle was' if num_dvaa_drivers == 1 else 'vehicles were'} "
            f"eligible to be booted or impounded under @bradlander's Dangerous Vehicle Abatement Act "
            f"(>= 5 red light camera violations or >= 15 speed camera violations in 12 months). "
            f"There have been {'{:,}'.format(total_dvaa_drivers)} such vehicles queried "
            f"since June 6, 2018.")

        messages: list[str] = [
            lookups_summary_string,
            rdaa_summary_string,
            dvaa_summary_string]

        if not is_dry_run:
            success: bool = tweeter.send_status(
                message_parts=messages,
                on_error_message=(
                    f'Error printing daily summary. '
                    f'Tagging @bdhowald.'))

            if success:
                LOG.debug('Daily summary plate ran successfully.')

        else:
            print(messages)

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