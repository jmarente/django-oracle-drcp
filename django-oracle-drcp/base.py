# pylint: disable=W0401
from django.core.exceptions import ImproperlyConfigured
from django.db.backends.oracle.base import *
from django.db.backends.oracle.base import DatabaseWrapper as DjDatabaseWrapper
from django.db.backends.oracle.utils import convert_unicode

import cx_Oracle


class DatabaseWrapper(DjDatabaseWrapper):

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        default_pool = {
            'min': 1,
            'max': 2,
            'increment': 1,
        }
        pool_config = self.settings_dict.get('POOL', default_pool)
        if set(pool_config.keys()) != {'min', 'max', 'increment'}:
            raise ImproperlyConfigured('POOL database option requires \'min\', \'max\', and \'increment\'')
        if not all(isinstance(val, int) for val in pool_config.values()):
            raise ImproperlyConfigured('POOL database option values must be numeric')

        options = self.settings_dict.get('OPTIONS', None)
        threaded = options.get('threaded', False) if options else False

        self.pool = cx_Oracle.SessionPool(
            user=self.settings_dict['USER'],
            password=self.settings_dict['PASSWORD'],
            dsn=self.get_dsn(),
            threaded=threaded,
            **pool_config)

    def get_new_connection(self, conn_params):
        conn_params.update({
            'pool': self.pool,
        })
        return super(DatabaseWrapper, self).get_new_connection(conn_params)

    def get_dsn(self):
        settings_dict = self.settings_dict

        if settings_dict['PORT']:
            dsn = self.Database.makedsn(settings_dict['HOST'],
                                   int(settings_dict['PORT']),
                                   settings_dict['NAME'])
        else:
            dsn = settings_dict['NAME']

        return dsn

    def _close(self):
        if self.connection is not None:
            with self.wrap_database_errors:
                return self.pool.release(self.connection)
