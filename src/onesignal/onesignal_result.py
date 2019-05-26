from kbc.result import KBCTableDef
from kbc.result import ResultWriter

# Pkeys
COMPANY_PK = ['companyId']
DEAL_PK = ['dealId']
DEAL_STAGE_HIST_PK = ['Deal_ID', 'sourceVid', 'sourceId', 'timestamp']

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
        not_table = KBCTableDef(name='notifications', columns=[], pk=['id'])
        ResultWriter.__init__(self, result_dir_path=out_path, table_def=not_table, fix_headers=False,
                              flatten_objects=False)

        notifications_data_tbl = KBCTableDef('notification_data', ['denikId', 'notification_id', 'url'],
                                             ['notification_id'])
        self.data_writer = ResultWriter(out_path, notifications_data_tbl,
                                        exclude_fields=['data'],
                                        flatten_objects=False,
                                        user_value_cols={'notification_id'}, buffer_size=buffer, fix_headers=True)

        notifications_filter_tbl = KBCTableDef('notification_filters',
                                               ['field', 'key', 'notification_id', 'relation', 'value'],
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

        filters = data.pop('filters', 'None')
        if filters:
            self.filter_writer.write_all(filters, user_values={'notification_id': self._get_pkey_values(data, {})})
            self.results = {**self.results, **self.data_writer.results}

        super().write(data, object_from_arrays=object_from_arrays)
