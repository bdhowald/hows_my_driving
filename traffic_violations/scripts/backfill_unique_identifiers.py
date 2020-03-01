import argparse
import math

import os
import logging
import random
import string
import threading

from sqlalchemy import and_
from sqlalchemy.orm import scoped_session, sessionmaker

from typing import List

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.campaign import Campaign
from traffic_violations.models.plate_lookup import PlateLookup
# from traffic_violations.models.plate_query import PlateQuery
# from traffic_violations.models.response.open_data_service_response \
#     import OpenDataServiceResponse

# from traffic_violations.services.apis.open_data_service import OpenDataService

LOG = logging.getLogger(__name__)


class BackfillUniqueIdentifiersJob(BaseJob):
    """ Backfill camera violations for old lookups """

    CAMERA_VIOLATIONS = ['Bus Lane Violation',
                         'Failure To Stop At Red Light',
                         'School Zone Speed Camera Violation']

    UNIQUE_IDENTIFIER_STRING_LENGTH = 8

    def perform(self, *args, **kwargs):
        is_dry_run: bool = kwargs.get('is_dry_run') or False

        threads = []
        num_threads = 100

        num_records = PlateLookup.query.count()
        chunk_length = math.ceil(num_records/num_threads)

        plate_lookups = PlateLookup.query.filter(PlateLookup.unique_identifier == None).all()
        unique_identifiers = {}

        for i in range(0, len(plate_lookups)):
            random_string = self._generate_unique_identifier()
            while unique_identifiers.get(random_string):
                random_string = self._generate_unique_identifier()
            unique_identifiers[random_string] = random_string

        unique_identifiers_list = [key for key in unique_identifiers.keys()]

        for n in range(0, num_threads):
            chunk_begin = n * chunk_length
            chunk_plate_lookups = plate_lookups[chunk_begin:(chunk_begin + chunk_length)]
            chunk_identifiers = unique_identifiers_list[chunk_begin:(chunk_begin + chunk_length)]
            print(f'this thread will handle lookups: {[l.id for l in chunk_plate_lookups]}')
            threads.append(threading.Thread(target=self._update_lookups, args=(
                chunk_identifiers, chunk_plate_lookups, is_dry_run)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        if not is_dry_run:
            PlateLookup.query.session.commit()

    def _generate_unique_identifier(self):
        return ''.join(
            random.SystemRandom().choice(
              string.ascii_lowercase + string.digits) for _ in range(self.UNIQUE_IDENTIFIER_STRING_LENGTH))

    def _update_lookups(self, identifiers: List[str], lookups: List[PlateLookup], is_dry_run: bool):
        for i in range(0, len(lookups)):
            lookups[i].unique_identifier = identifiers[i]
            print(lookups[i].id, identifiers[i],)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Job that backfills the unique identifiers of plate lookups')

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't save results")

    return parser.parse_args()


if __name__ == '__main__':
    arguments = parse_args()

    job = BackfillUniqueIdentifiersJob()
    job.run(is_dry_run=arguments.dry_run)
