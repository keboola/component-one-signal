'''


'''

import logging
import os
import sys
from datetime import datetime

from kbc.env_handler import KBCEnvHandler

from onesignal.onesignal_client import OnesignalClient
from onesignal.onesignal_result import NotificationsWriter

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PERIOD_FROM = 'period_from'

KEY_APP_ID = 'app_id'
KEY_NOTIFICATIONS = 'notifications'

MANDATORY_PARS = [KEY_API_TOKEN, KEY_APP_ID]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        # override debug from config
        if self.cfg_params.get('debug'):
            debug = True

        self.set_default_logger('DEBUG' if debug else 'INFO')
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

        # intialize instance parameteres
        self.client = OnesignalClient(self.cfg_params[KEY_API_TOKEN])

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        if self.cfg_params.get(KEY_PERIOD_FROM):
            start_date, end_date = self.get_date_period_converted(params.get(KEY_PERIOD_FROM),
                                                                  datetime.utcnow().strftime('%Y-%m-%d'))
            start_date = int(start_date.timestamp())
        else:
            start_date = None

        app_ids = [self.cfg_params[KEY_APP_ID]]

        # get all notifications
        if params.get(KEY_NOTIFICATIONS, False):
            self.get_n_save_notifications(app_ids)

        # get csv export
        self.get_n_store_players_csv(app_ids, start_date)

    def get_n_save_notifications(self, app_ids):
        """

        """
        logging.info('Extracting notifications')

        # writer setup
        not_writer = NotificationsWriter(
            self.tables_out_path)

        with not_writer:
            for appid in app_ids:
                self._extract_notification_for_app(appid, not_writer)

            # store manifest
            logging.info("Storing notification manifest file.")
            self.create_manifests(not_writer.collect_results())

        return

    def _extract_notification_for_app(self, app_id, writer):
        for res in self.client.get_notifications(app_id=app_id):
            if isinstance(res, list):
                writer.write_all(res, object_from_arrays=True)
            else:
                writer.write(res, object_from_arrays=True)

    def get_n_store_players_csv(self, app_ids, active_since, extra_fields=None):
        output_folder = os.path.join(self.tables_out_path)  # , 'players')
        res_files = []
        for app_id in app_ids:
            res = self.client.get_n_download_players_csv(app_id, output_folder, extra_fields, active_since)
            res_files.append(res)

        logging.info("Storing players manifest file.")
        self.configuration.write_table_manifest(os.path.join(output_folder, 'players.gz.csv'), primary_key=['id'],
                                                incremental=True)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = False
    comp = Component(debug)
    comp.run()
