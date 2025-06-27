from contextlib import contextmanager

import teradatasql
import time
import dbt_common.exceptions
import dbt.adapters.exceptions
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager
import re

logger = AdapterLogger("teradata")
from dataclasses import dataclass, field
from typing import Optional, Tuple, Any, Dict


@dataclass
class TeradataCredentials(Credentials):
    query_band: Optional[str] = field(default="org=teradata-internal-telem;appname=dbt;")                  # query_band parameter will be populated by profiles.yml, but it not a connection parameter so this won't be used to connect by driver
    server: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    port: Optional[str] = None
    tmode: Optional[str] = "ANSI"
    logmech: Optional[str] = None
    charset: Optional[str] = None
    account: Optional[str] = None
    column_name: Optional[str] = None
    cop: Optional[str] = None
    coplast: Optional[str] = None
    encryptdata: Optional[str] = None
    fake_result_sets: Optional[str] = None
    field_quote: Optional[str] = None
    field_sep: Optional[str] = None
    lob_support: Optional[str] = None
    log: Optional[str] = None
    logdata: Optional[str] = None
    max_message_body: Optional[str] = None
    partition: Optional[str] = None
    sip_support: Optional[str] = None
    teradata_values: Optional[str] = None
    retries: int = 0
    retry_timeout: int = 1
    sslmode: Optional[str] = None
    sslca: Optional[str] = None
    sslcapath: Optional[str] = None
    sslcrc: Optional[str] = None
    sslcipher: Optional[str] = None
    sslprotocol: Optional[str] = None
    browser: Optional[str] = None
    browser_tab_timeout: Optional[int] = None
    browser_timeout: Optional[int] = None
    sp_spl: Optional[bool] = None
    sessions: Optional[int] = None
    runstartup: Optional[bool] = None
    logon_timeout: Optional[int] = None
    https_port: Optional[int] = None
    connect_timeout: Optional[int] = None
    request_timeout: Optional[int] = None
    http_proxy: Optional[str] = None
    http_proxy_user: Optional[str] = None
    http_proxy_password: Optional[str] = None
    https_proxy: Optional[str] = None
    https_proxy_user: Optional[str] = None
    https_proxy_password: Optional[str] = None
    sslcrl: Optional[bool] = None
    sslocsp: Optional[bool] = None
    proxy_bypass_hosts: Optional[str] = None
    oidc_sslmode: Optional[str] = None

    _ALIASES = {
        "UID": "username",
        "user": "username",
        "PWD": "password",
        "host": "server"
    }

    def __post_init__(self):
        if self.logmech is not None and self.logmech.lower() == "browser":
            # When logmech is "browser", username and password should not be provided.
            if self.username is not None or self.password is not None:
                raise dbt_common.exceptions.DbtRuntimeError(
                    "Couldn’t connect to Teradata Vantage SQL Engine. Neither username nor password parameters can be "
                    "specified in the profile when the logon mechanism (logmech) is ‘BROWSER'. Correct the profile "
                    "and retry.")
        else:
            if self.username is None:
                raise dbt_common.exceptions.DbtRuntimeError("Couldn’t  connect to Teradata Vantage SQL Engine. The "
                                                            "‘user’ parameter in the profile must be specified when "
                                                            "the logon mechanism (logmech) is ‘TD2'. Correct the "
                                                            "profile and retry.")
            elif self.password is None:
                raise dbt_common.exceptions.DbtRuntimeError("Couldn’t  connect to Teradata Vantage SQL Engine. The "
                                                            "‘password’ parameter in the profile must be specified "
                                                            "when the logon mechanism (logmech) is ‘TD2'. Correct the "
                                                            "profile and retry.")
        if self.schema is None:
            raise dbt_common.exceptions.DbtRuntimeError("Couldn’t  connect to Teradata Vantage SQL Engine. The "
                                                        "‘schema’ parameter in the profile must be specified . "
                                                        "Correct the profile and retry.")
        # teradata classifies database and schema as the same thing
        if (
                self.database is not None and
                self.database != self.schema
        ):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"Couldn’t  connect to Teradata Vantage SQL Engine. The ‘database’ parameter in the profile is "
                f"specified and does not match the ‘schema’ parameter value. Correct the profile by removing the "
                f"‘database’ parameter or changing it to same value as ‘schema’ parameter and then retry."
            )
        if self.tmode == "TERA":
            note_for_tera = '''
----------------------------------------------------------------------------------
IMPORTANT NOTE: This is an initial implementation of the TERA transaction mode
and may not support some use cases.
We strongly advise validating all records or transformations utilizing this mode
to preempt any potential anomalies or errors
----------------------------------------------------------------------------------
            '''
            logger.info(note_for_tera)

    @property
    def type(self):
        return "teradata"

    @property
    def unique_field(self):
        return self.schema

    def _connection_keys(self):
        """
        Returns an iterator of keys to pretty-print in 'dbt debug'
        """
        return (
            "server",
            "port",
            "database",
            "schema",
            "user",
            "tmode",
            "logmech",
            "account",
            "column_name",
            "cop",
            "coplast",
            "encryptdata",
            "fake_result_sets",
            "field_quote",
            "field_sep",
            "lob_support",
            "log",
            "logdata",
            "max_message_body",
            "partition",
            "sip_support",
            "teradata_values",
            "sslmode",
            "sslca",
            "sslcapath",
            "sslcrc",
            "sslcipher",
            "sslprotocol",
            "browser",
            "browser_tab_timeout",
            "browser_timeout",
            "sp_spl",
            "sessions",
            "runstartup",
            "logon_timeout",
            "https_port",
            "connect_timeout",
            "request_timeout",
            "query_band",
            "http_proxy",
            "http_proxy_user",
            "http_proxy_password",
            "https_proxy",
            "https_proxy_user",
            "https_proxy_password",
            "sslcrl",
            "sslocsp",
            "proxy_bypass_hosts",
            "oidc_sslmode"
        )

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        # If database is not defined as adapter credentials
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

class TeradataConnectionManager(SQLConnectionManager):
    TYPE = "teradata"

    '''
    disabling transactional logic by default for dbt-teradata
    by disabling add_begin_query(), add_commit_query(), begin(), commit()
    and clear_transaction() methods
    '''
    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}

        kwargs["host"] = credentials.server
        kwargs["tmode"] = credentials.tmode
        if credentials.username:
           kwargs["user"] = credentials.username
        if credentials.password:
            kwargs["password"] = credentials.password
        if credentials.logmech:
            kwargs["logmech"] = credentials.logmech
        if credentials.account:
          kwargs["account"] = credentials.account
        if credentials.column_name:
          kwargs["column_name"] = credentials.column_name
        if credentials.cop:
          kwargs["cop"] = credentials.cop
        if credentials.coplast:
          kwargs["coplast"] = credentials.coplast
        if credentials.encryptdata:
          kwargs["encryptdata"] = credentials.encryptdata
        if credentials.fake_result_sets:
          kwargs["fake_result_sets"] = credentials.fake_result_sets
        if credentials.field_quote:
          kwargs["field_quote"] = credentials.field_quote
        if credentials.field_sep:
          kwargs["field_sep"] = credentials.field_sep
        if credentials.lob_support:
          kwargs["lob_support"] = credentials.lob_support
        if credentials.log:
          kwargs["log"] = credentials.log
        if credentials.logdata:
          kwargs["logdata"] = credentials.logdata
        if credentials.max_message_body:
          kwargs["max_message_body"] = credentials.max_message_body
        if credentials.partition:
          kwargs["partition"] = credentials.partition
        if credentials.sip_support:
          kwargs["sip_support"] = credentials.sip_support
        if credentials.teradata_values:
          kwargs["teradata_values"] = credentials.teradata_values
        if credentials.port:
            kwargs["dbs_port"] = credentials.port
        if credentials.sslmode:
           kwargs["sslmode"] = credentials.sslmode
        if credentials.sslca:
           kwargs["sslca"] = credentials.sslca
        if credentials.sslcapath:
           kwargs["sslcapath"] = credentials.sslcapath
        if credentials.sslcrc:
           kwargs["sslcrc"] = credentials.sslcrc
        if credentials.sslcipher:
           kwargs["sslcipher"] = credentials.sslcipher
        if credentials.sslprotocol:
           kwargs["sslprotocol"] = credentials.sslprotocol
        if credentials.browser:
           kwargs["browser"]=credentials.browser
        if credentials.browser_tab_timeout:
           kwargs["browser_tab_timeout"]=credentials.browser_tab_timeout
        if credentials.browser_timeout:
           kwargs["browser_timeout"]=credentials.browser_timeout
        if credentials.sp_spl:
           kwargs["sp_spl"]=credentials.sp_spl
        if credentials.sessions:
           kwargs["sessions"]=credentials.sessions
        if credentials.runstartup:
           kwargs["runstartup"]=credentials.runstartup
        if credentials.logon_timeout:
           kwargs["logon_timeout"]=credentials.logon_timeout
        if credentials.https_port:
           kwargs["https_port"]=credentials.https_port
        if credentials.connect_timeout:
           kwargs["connect_timeout"]=credentials.connect_timeout
        if credentials.request_timeout:
           kwargs["request_timeout"]=credentials.request_timeout
        if credentials.http_proxy:
            kwargs["http_proxy"]=credentials.http_proxy
        if credentials.http_proxy_user:
            kwargs["http_proxy_user"]=credentials.http_proxy_user
        if credentials.http_proxy_password:
            kwargs["http_proxy_password"]=credentials.http_proxy_password
        if credentials.https_proxy:
            kwargs["https_proxy"]=credentials.https_proxy
        if credentials.https_proxy_user:
            kwargs["https_proxy_user"]=credentials.https_proxy_user
        if credentials.https_proxy_password:
            kwargs["https_proxy_password"]=credentials.https_proxy_password
        if credentials.sslcrl:
            kwargs["sslcrl"]=credentials.sslcrl
        if credentials.sslocsp:
            kwargs["sslocsp"]=credentials.sslocsp
        if credentials.proxy_bypass_hosts:
            kwargs["proxy_bypass_hosts"]=credentials.proxy_bypass_hosts
        if credentials.oidc_sslmode:
            kwargs["oidc_sslmode"]=credentials.oidc_sslmode

        # Save the transaction mode
        cls.TMODE = credentials.tmode

        if credentials.retries > 0:
            def connect():
                connection.handle = teradatasql.connect(**kwargs)
                connection.state = 'open'

                # checking if query_band connection parameter exists. If it does -> executing the set query_band sql
                if credentials.query_band:
                    return cls.apply_query_band(connection.handle, credentials.query_band)

                return connection.handle

            retryable_exceptions = [teradatasql.OperationalError]

            return cls.retry_connection(
                connection,
                connect=connect,
                logger=logger,
                retry_limit=credentials.retries,
                retry_timeout=credentials.retry_timeout,
                retryable_exceptions=retryable_exceptions,
            )

        try:
            connection.handle = teradatasql.connect(**kwargs)
            connection.state = 'open'

            # checking if query_band connection parameter exists. If it does -> executing the set query_band sql
            if credentials.query_band:
                return cls.apply_query_band(connection.handle, credentials.query_band)

        except teradatasql.Error as e:
            logger.debug("Couldn’t  connect to Teradata Vantage SQL Engine. The Teradata driver error message is: '{}'"
                         "Correct the problem and retry."
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.adapters.exceptions.FailedToConnectError(str(e))

        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def cancel(self, connection: Connection):
        connection.handle.close()

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except teradatasql.DatabaseError as e:
            logger.debug('Teradata error: {}'.format(str(e)))

            try:
                self.rollback_if_open()
            except teradatasql.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Couldn’t  execute a SQL request against the Teradata Vantage SQL Engine. The SQL that "
                         "failed is: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(e) from e

    @classmethod
    def get_response(cls, cursor):
        num_rows = 0
        activity = "success"
        message = "OK"
        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount
            activity = cursor.activityname
            message = "activity: {}, rows_affected: {}".format(cursor.activityname, cursor.rowcount)
        return AdapterResponse(
          _message=message,
          rows_affected=num_rows,
          code=activity
        )
    '''
    overriding add_query method to disable
    transactional logic for dbt-teradata
    '''
    def add_query(
        self,
        sql: str,
        auto_begin: bool = False, #this avoid calling begin() method of SQLConnectionManager and hence disabling transactional logic
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        try:
            return SQLConnectionManager.add_query(self, sql, auto_begin, bindings, abridge_sql_log)
        except Exception as ex:
            ignored = False
            query = sql.strip()
            if ("DROP view /*+ IF EXISTS */" in query) or ("DROP table /*+ IF EXISTS */" in query):
                for error_number in [3807, 3854, 3853]:
                    if f"[Error {error_number}]" in str (ex):
                        ignored = True
                        return None, None
            if ("DELETE DATABASE /*+ IF EXISTS */" in query) or ("DROP DATABASE /*+ IF EXISTS */" in query):
                for error_number in [3802]:
                    if f"[Error {error_number}]" in str (ex):
                        ignored = True
                        return None, None
            if not ignored:
                raise # rethrow

    # this method will return the datatype as string
    @classmethod
    def data_type_code_to_name(cls, type_code) -> str:
        return str(type_code)

    @classmethod
    def apply_query_band(cls, handle, query_band_text):
        try:
            cur = handle.cursor()

            if query_band_text[-1] != ';':
                query_band_text += ';'

            """checking if appname= key exist in query_band and if dbt exist as value of 
            that key otherwise appending +dbt in the end """
            if 'appname=' in query_band_text or 'appname =' in query_band_text:
                pair = query_band_text.split(';')
                for i in range(len(pair)):
                    if 'appname' in pair[i]:
                        val = pair[i].split('=')[1]
                        patterns = "^" + re.escape('dbt') + "$"
                        if not re.match(patterns, val):
                            pair[i] += '+dbt'
                query_band_text = ';'.join(pair)
            else:
                query_band_text = query_band_text + 'appname=dbt;'

            """ checking if 'org=' or 'org =' exist, if it doesn't appending 'org=teradata-internal-telem; in query_band 
                string.  If it exists, user might have set some value of their own, so doing nothing in that case"""

            if 'org=' not in query_band_text and 'org =' not in query_band_text:
                query_band_text += 'org=teradata-internal-telem;'

            query_band_str = "set query_band = '{}' for session;".format(query_band_text)
            cur.execute(query_band_str)
            cur.execute("sel GetQueryBand();")
            rows = cur.fetchone()
            logger.debug("Query Band set to {}".format(rows))           # To log in dbt.log
        except teradatasql.Error as ex:
            logger.debug(ex)
            logger.info("Couldn’t set the query_band using the specified value. Correct the query_band parameter in "
                        "the profile and retry.")
            raise dbt.exceptions.DbtRuntimeError(str(ex))

        return handle                                                     # returning the connection handle
