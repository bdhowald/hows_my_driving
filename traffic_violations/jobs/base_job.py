import argparse
import logging

LOG = logging.getLogger(__name__)

class BaseJob:

    def perform(self, *args, **kwargs):
        raise NotImplementedError(
            'Subclassed job must implement this method.')

    def run(self, *args, **kwargs):
        try:
            self.perform(*args, **kwargs)
        except NotImplementedError as ex:
            LOG.error(ex)
