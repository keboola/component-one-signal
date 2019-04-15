import json
import os
from collections.abc import Iterable

from kbc.client_base import HttpClientBase

ENDPOINT_APPS = 'apps'
ENDPOINT_NOTIFICATIONS = 'notifications'
ENDPOINT_CSV_EXPORT = 'players/csv_export'

PLAYERS_EXTRA_FIELDS = ['location', 'country', 'rooted', 'notification_types', 'as_id', 'ip', 'external_user_id',
                        'web_auth',
                        'web_p256']

PLAYERS_DEFAULT_FIELDS = ["id",
                          "identifier",
                          "session_count",
                          "language",
                          "timezone",
                          "game_version",
                          "device_os",
                          "device_type",
                          "device_model",
                          "ad_id",
                          "tags",
                          "last_active",
                          "playtime",
                          "amount_spent",
                          "created_at",
                          "invalid_identifier",
                          "badge_count"]

PLAYERS_ALL_FIELDS = PLAYERS_DEFAULT_FIELDS + PLAYERS_EXTRA_FIELDS

# endpoints

MAX_RETRIES = 10
BASE_URL = 'https://onesignal.com/api/v1/'


class OnesignalClient(HttpClientBase):
    """
    Basic HTTP client taking care of core HTTP communication with the API service.

    It exttends the kbc.client_base.HttpClientBase class, setting up the specifics for Onesignal service and adding
    methods for handling pagination.

    """

    def __init__(self, token):
        HttpClientBase.__init__(self, base_url=BASE_URL, max_retries=MAX_RETRIES, backoff_factor=0.3,
                                status_forcelist=(429, 500, 502, 504),
                                default_http_header={"Authorization": 'Basic ' + token})

    def _get_paged_result_pages(self, endpoint, parameters, res_obj_name, limit_attr, offset_req_attr, offset_resp_attr,
                                offset, limit):
        """
        Generic pagination getter method returning Iterable instance that can be used in for loops.

        :param endpoint:
        :param parameters:
        :param res_obj_name:
        :param limit_attr:
        :param offset_req_attr:
        :param offset_resp_attr:
        :param has_more_attr:
        :param offset:
        :param limit:
        :return:
        """
        has_more = True
        while has_more:

            parameters[offset_req_attr] = offset
            parameters[limit_attr] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            resp_text = str.encode(req.text, 'utf-8')
            req_response = json.loads(resp_text)

            if len(req_response[res_obj_name]) < req_response[limit]:
                has_more = True
            else:
                has_more = False
            offset = req_response[offset_resp_attr]

            yield req_response[res_obj_name]

    def get_n_download_players_csv(self, app_id, output_folder, extra_fields=None, last_active_since=None):
        body = {}
        if extra_fields:
            body['extra_fields'] = PLAYERS_EXTRA_FIELDS
        else:
            body['extra_fields'] = extra_fields

        if last_active_since:
            body['last_active_since'] = last_active_since

        url = self.base_url + ENDPOINT_CSV_EXPORT

        res = self.post(url=url, data=body, params={'app_id': app_id})

        # download file
        file_url = res['csv_file_url']
        file_name = os.path.basename(file_url)
        r = self.get_raw(file_url)
        res_file_path = os.path.join(output_folder, file_name)
        open(os.path.join(output_folder, file_name), 'wb').write(r.content)

        return res_file_path

    def get_notifications(self, app_id, kind=None) -> Iterable:
        """
        View the details of multiple notifications.
        :param app_id: The app ID that you want to view notifications from
        :param kind: Kind of notifications returned. Default (not set) is all notification types.
                    Dashboard only is 0.
                    API only is 1.
                    Automated only is 3.
        :type kind: int
        :return: Iterable of notifications object
        """

        url_endpoint = ENDPOINT_NOTIFICATIONS
        params = {
            "app_id": app_id,
            "kind": kind
        }
        return self._get_paged_result_pages(url_endpoint, params, 'notifications', 'limit', 'offset', 'offset', 0, 50)

    def get_apps(self):
        """
        View the details of all of your current OneSignal apps
        :return:
        """
        return self.get(self.base_url + ENDPOINT_APPS)
