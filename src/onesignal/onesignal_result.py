from kbc.result import KBCTableDef
from kbc.result import ResultWriter

# Pkeys
NOTIFICATIONS_COLS = ['adm_big_picture', 'adm_group', 'adm_group_message', 'adm_large_icon', 'adm_small_icon',
                      'adm_sound', 'alexa_display_title', 'alexa_ssml', 'amazon_background_data',
                      'android_accent_color', 'android_group', 'android_group_message', 'android_led_color',
                      'android_sound', 'android_visibility', 'apns_alert', 'app_id', 'app_url', 'big_picture',
                      'buttons', 'canceled', 'chrome_big_picture', 'chrome_icon', 'chrome_web_badge', 'chrome_web_icon',
                      'chrome_web_image', 'completed_at', 'content_available', 'contents', 'converted',
                      'delayed_option', 'delivery_time_of_day', 'errored', 'failed', 'firefox_icon', 'headings', 'id',
                      'include_external_user_ids', 'include_player_ids', 'ios_attachments', 'ios_badgeCount',
                      'ios_badgeType', 'ios_category', 'ios_sound', 'isAdm', 'isAlexa', 'isAndroid', 'isChrome',
                      'isChromeWeb', 'isEdge', 'isFirefox', 'isIos', 'isSafari', 'isWP', 'isWP_WNS', 'large_icon',
                      'platform_delivery_stats', 'priority', 'queued_at', 'remaining', 'send_after', 'small_icon',
                      'spoken_text', 'successful', 'tags', 'template_id', 'thread_id', 'ttl', 'url', 'web_buttons',
                      'web_push_topic', 'web_url', 'wp_sound', 'wp_wns_sound']

"""
Class extending the kbc.result.ResultWriter class to add some additional functionality.

In particular it is used to process more complex nested objects while using
the functionality the base ResultWriter provides. The class overrides constructor and core write methods.



"""


class NotificationsWriter(ResultWriter):
    """
    Overridden constructor method of ResultWriter. It creates extra ResultWriter instance that handles processing of
    the nested object 'deals_stage_history'. That writer is then called from within the write method.
    """

    def __init__(self, out_path, buffer=8192):
        # table def
        not_table = KBCTableDef(name='notifications', columns=NOTIFICATIONS_COLS, pk=['id'])
        ResultWriter.__init__(self, result_dir_path=out_path, table_def=not_table, fix_headers=True,
                              flatten_objects=False)

        notifications_data_tbl = KBCTableDef('notification_data', ['denikId', 'url'],
                                             ['notification_id'])
        self.data_writer = ResultWriter(out_path, notifications_data_tbl,
                                        exclude_fields=['data'],
                                        flatten_objects=False,
                                        user_value_cols={'notification_id'}, buffer_size=buffer, fix_headers=True)

        notifications_filter_tbl = KBCTableDef('notification_filters',
                                               ['field', 'key', 'relation', 'value'],
                                               ['notification_id'])
        self.filter_writer = ResultWriter(out_path, notifications_filter_tbl,
                                          exclude_fields=['filters'],
                                          flatten_objects=False,
                                          user_value_cols={'notification_id'}, buffer_size=buffer, fix_headers=True)

    """
    Overridden write method that is modified to process the nested object separately using newly created nested writer.
    """

    def write(self, data, file_name=None, user_values=None, object_from_arrays=False, write_header=True):
        # write ext users
        nt_data = data.pop('data', None)
        if nt_data:
            self.data_writer.write(nt_data, user_values={'notification_id': self._get_pkey_values(data, {})})
            self.results = {**self.results, **self.data_writer.results}

        filters = data.pop('filters', None)
        if filters:
            self.filter_writer.write_all(filters, user_values={'notification_id': self._get_pkey_values(data, {})})
            self.results = {**self.results, **self.data_writer.results}

        super().write(data, object_from_arrays=object_from_arrays)
