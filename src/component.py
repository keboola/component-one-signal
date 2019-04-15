'''


'''

import logging
import os
import sys
from datetime import datetime

from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef
from kbc.result import ResultWriter

from onesignal import onesignal_client
from onesignal.onesignal_client import OnesignalClient

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PERIOD_FROM = 'period_from'

KEY_APP_IDS = 'app_ids'

MANDATORY_PARS = [KEY_API_TOKEN]
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
            start_date = start_date.timestamp()
        else:
            start_date = None

        # get applications
        app_ids = self.get_n_save_applications()

        # override appids list if specified
        if self.cfg_params.get(KEY_APP_IDS):
            app_ids = self.cfg_params[KEY_APP_IDS]

        # get all notifications
        self.get_n_save_notifications(app_ids)

        # get csv export
        self.get_n_store_players_csv(app_ids, start_date)
        logging.info("Extraction finished")

    def get_n_save_applications(self):
        """

        :return: list of all app ids
        """
        logging.info('Extracting applications')

        # table def
        apps_table = KBCTableDef(name='apps', columns=[], pk='id')
        # writer setup
        apps_writer = ResultWriter(result_dir_path=self.tables_out_path, table_def=apps_table, fix_headers=False)
        app_res = self.client.get_apps()
        apps_writer.write_all(app_res)

        # store manifest
        logging.info("Storing Apps manifest file.")
        self.create_manifests(apps_writer.collect_results())

        return [r['id'] for r in app_res]

    def get_n_save_notifications(self, app_ids):
        """

        """
        logging.info('Extracting notifications')

        # table def
        not_table = KBCTableDef(name='notifications', columns=[], pk='id')
        # writer setup
        not_writer = ResultWriter(result_dir_path=self.tables_out_path, table_def=not_table, fix_headers=False)

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
                writer.write_all(res)
            else:
                writer.write(res)

    def get_n_store_players_csv(self, app_ids, active_since, extra_fields=None):

        output_folder = os.path.join(self.tables_out_path, 'players')
        res_files = []
        for app_id in app_ids:
            res = self.client.get_n_download_players_csv(app_id, output_folder, extra_fields, active_since)
            res_files.append(res)

        logging.info("Storing players manifest file.")
        self.configuration.write_table_manifest(output_folder, primary_key='id', incremental=True,
                                                columns=onesignal_client.PLAYERS_ALL_FIELDS)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True
    comp = Component(debug)
    comp.run()
