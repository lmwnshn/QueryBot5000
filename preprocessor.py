import pandas as pd
import pglast
import time


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

  @staticmethod
  def _read_df(csvlogs):
    df = pd.concat(
      pd.read_csv(
        csvlog,
        names=Preprocessor.PG_LOG_COLUMNS,
        parse_dates=['log_time', 'session_start_time'],
        usecols=['log_time', 'session_start_time', 'command_tag', 'message', 'detail'],
        header=None,
        index_col=False,
      )
      for csvlog in csvlogs
    )
    return df

  @staticmethod
  def _extract_query(message_series):
    query = message_series.str.extract('((?:DELETE|INSERT|SELECT|UPDATE).*)')
    print(query)
    return query.apply(pglast.prettify, axis=1)
    commands = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
    def extract(message):
      for command in commands:
        idx = message.find(command)
        if idx != -1:
          query = message[idx:]
          # Standardize query format.
          return pglast.prettify(query)
      return ''
    return message_series.apply(extract)

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
    return detail_series.apply(extract)

  @staticmethod
  def _substitute_params(df, query_col, params_col):
    def substitute(query, params):
      for k, v in params.items():
        query = query.replace(k, v)
      return query
    return df.apply(lambda row: substitute(row[query_col], row[params_col]),
                    axis=1)

  @staticmethod
  def _parse(query_series):
    def parse(sql):
      new_sql, params, last_end = [], [], 0
      for token in pglast.parser.scan(sql):
        token_str = sql[token.start:token.end + 1]
        if token.start > last_end:
          new_sql.append(' ')
        if token.name in ['ICONST', 'FCONST']:
          # Integer or float constant.
          new_sql.append('#')
          params.append(token_str)
        elif token.name in ['SCONST']:
          # String constant.
          new_sql.append('&&&')
          params.append(token_str)
        else:
          new_sql.append(token_str)
        last_end = token.end + 1
      new_sql = ''.join(new_sql)
      return new_sql, params
    return query_series.apply(parse)

  def __init__(self, csvlogs):
    time_old, time_new = time.time_ns(), time.time_ns()
    df = self._read_df(csvlogs)

    print('Extract query ')
    df['query_raw'] = self._extract_query(df['message'])
    time_old, time_new = time_new, time.time_ns()
    print(time_new - time_old, ' ns')

    print('Extract params', end='')
    df['params'] = self._extract_params(df['detail'])
    print('Subst params into query')
    df['query_subst'] = self._substitute_params(df, 'query_raw', 'params')
    print('Parse query')
    df['query'] = self._parse(df['query_subst'])
    print(df['query'].iloc[0])




pgfiles = [
 'data/extracted/simple/postgresql-2021-12-06_160140.csv',
 'data/extracted/extended/postgresql-2021-12-06_142428.csv',
]

Preprocessor(pgfiles)
