import glob

from pandarallel import pandarallel

from multiprocessing import Pool

from tqdm.contrib.concurrent import process_map

import numpy as np
import pandas as pd
import pglast
import time

pandarallel.initialize()

class Preprocessor:
    # https://www.postgresql.org/docs/13/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-CSVLOG
    PG_LOG_COLUMNS = [
        'log_time',
        'user_name',
        'database_name',
        'process_id',
        'connection_from',
        'session_id',
        'session_line_num',
        'command_tag',
        'session_start_time',
        'virtual_transaction_id',
        'transaction_id',
        'error_severity',
        'sql_state_code',
        'message',
        'detail',
        'hint',
        'internal_query',
        'internal_query_pos',
        'context',
        'query',
        'query_pos',
        'location',
        'application_name',
        'backend_type',
    ]

    def get_dataframe(self):
        return self._df

    def get_grouped_dataframe(self):
        return self._grouped_df

    @staticmethod
    def _read_csv(csvlog):
        # This function must have a separate non-local binding from _read_df
        # so that it is pickleable.
        return pd.read_csv(
            csvlog,
            names=Preprocessor.PG_LOG_COLUMNS,
            parse_dates=['log_time', 'session_start_time'],
            usecols=['log_time', 'session_start_time', 'command_tag', 'message', 'detail'],
            header=None,
            index_col=False,
        )

    @staticmethod
    def _read_df(csvlogs):
        return pd.concat(process_map(Preprocessor._read_csv, csvlogs))

    @staticmethod
    def _extract_query(message_series):
        query = message_series.str.extract('((?:DELETE|INSERT|SELECT|UPDATE).*)', expand=False)
        print('TODO(WAN): expose prettify option as command line. skipping that.')
        #query = query.parallel_map(pglast.prettify, na_action='ignore')
        query.fillna('', inplace=True)
        return query.astype(str)

    @staticmethod
    def _extract_params(detail_series):
        def extract(detail):
            detail = str(detail)
            prefix = 'parameters: '
            idx = detail.find(prefix)
            if idx == -1:
                return {}
            parameter_list = detail[idx + len(prefix):]
            params = {}
            for pstr in parameter_list.split(', '):
                pnum, pval = pstr.split(' = ')
                assert pnum.startswith('$')
                assert pnum[1:].isdigit()
                params[pnum] = pval
            return params
        return detail_series.parallel_apply(extract)

    @staticmethod
    def _substitute_params(df, query_col, params_col):
        def substitute(query, params):
            for k, v in params.items():
                query = query.replace(k, v)
            return query
        return df.parallel_apply(
            lambda row: substitute(row[query_col], row[params_col]), axis=1)

    @staticmethod
    def _parse(query_series):
        def parse(sql):
            new_sql, params, last_end = [], [], 0
            for token in pglast.parser.scan(sql):
                token_str = sql[token.start:token.end + 1]
                if token.start > last_end:
                    new_sql.append(' ')
                if token.name in ['ICONST', 'FCONST', 'SCONST']:
                    # Integer, float, or string constant.
                    new_sql.append('$' + str(len(params) + 1))
                    params.append(token_str)
                else:
                    new_sql.append(token_str)
                last_end = token.end + 1
            new_sql = ''.join(new_sql)
            return new_sql, params
        return query_series.parallel_apply(parse)

    def __init__(self, csvlogs):
        time_end, time_start = None, time.perf_counter()

        def clock(label):
            nonlocal time_end, time_start
            time_end = time.perf_counter()
            print('\r{}: {:.2f} s'.format(label, time_end - time_start))
            time_start = time_end

        df = self._read_df(csvlogs)
        clock('Read dataframe')

        print('Extract queries: ', end='', flush=True)
        df['query_raw'] = self._extract_query(df['message'])
        clock('Extract queries')

        print('Extract parameters: ', end='', flush=True)
        df['params'] = self._extract_params(df['detail'])
        clock('Extract parameters')

        print('Substitute parameters into query: ', end='', flush=True)
        df['query_subst'] = self._substitute_params(df, 'query_raw', 'params')
        clock('Substitute parameters into query')

        print('Parse query: ', end='', flush=True)
        parsed = self._parse(df['query_subst'])
        df[['query_template', 'query_params']] = pd.DataFrame(parsed.tolist(), index=df.index)
        clock('Parse query')

        # Round all times to closest second.
        df['log_time_s'] = df['log_time'].round('S')

        gb = df.groupby(['query_template', 'log_time_s']).size()
        grouped_df = pd.DataFrame(gb, columns=['count'])
        grouped_df.drop('', axis=0, level=0, inplace=True)

        self._df = df
        self._grouped_df = grouped_df


def main():
    pgfiles = [
        'data/extracted/simple/postgresql-2021-12-06_160140.csv',
        'data/extracted/extended/postgresql-2021-12-06_142428.csv',
    ]

    pgfiles = []
    pgfiles += glob.glob('data/extracted/simple/postgresql*.csv')
    pgfiles += glob.glob('data/extracted/extended/postgresql*.csv')

    Preprocessor(pgfiles)


if __name__ == '__main__':
    main()
