from contextlib import contextmanager

import teradatasql

import dbt.exceptions
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.connection import Connection
from dbt.adapters.base import Credentials
from dbt.events import AdapterLogger
logger = AdapterLogger("teradata")
from dataclasses import dataclass
from typing import Optional, Tuple, Any, Dict


@dataclass
class TeradataCredentials(Credentials):
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

    _ALIASES = {
        "UID": "username",
        "user": "username",
        "PWD": "password",
        "host": "server"
    }

    def __post_init__(self):
        if self.username is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `user` in profile")
        elif self.password is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `password` in profile")
        elif self.schema is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `schema` in profile")
        # teradata classifies database and schema as the same thing
        if (
                self.database is not None and
                self.database != self.schema
        ):
            raise dbt.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Teradata, database must be omitted or have the same value as"
                f" schema."
            )

        # Only allow the ANSI transaction mode
        if self.tmode != "ANSI":
            raise dbt.exceptions.DbtRuntimeError(
                f"This version only allows a tmode of ANSI."
            )

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
            "request_timeout"
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
    TMODE = "ANSI"

    def add_begin_query(self):
        if self.TMODE == 'ANSI':
            return self.add_query('', auto_begin=False)
        elif self.TMODE == 'TERA':
            return self.add_query('BEGIN TRANSACTION', auto_begin=False)

    def add_commit_query(self):
        if self.TMODE == 'ANSI':
            return self.add_query('COMMIT', auto_begin=False)
        elif self.TMODE == 'TERA':
            return self.add_query('', auto_begin=False)

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

        # Save the transaction mode
        cls.TMODE = credentials.tmode

        if credentials.retries > 0:
            def connect():
                connection.handle = teradatasql.connect(**kwargs)
                connection.state = 'open'
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
        except teradatasql.Error as e:
            logger.debug("Got an error when attempting to open a teradata "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectError(str(e))

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

            raise dbt.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.DbtRuntimeError(e) from e

    @classmethod
    def get_response(cls, cursor):
        # There's no real way to get this from teradatasql, so just return "OK"
        num_rows = 0
        if cursor is not None and cursor.rowcount is not None:
          num_rows = cursor.rowcount
        return AdapterResponse(
          _message="OK",
          rows_affected = num_rows,
          code='SUCCESS'
        )

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
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