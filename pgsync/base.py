"""PGSync Base class."""
import collections
import logging
import os
import sys
import warnings

import sqlalchemy as sa
import sqlparse
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import Values
import boto3

from .constants import (
    BUILTIN_SCHEMAS,
    LOGICAL_SLOT_PREFIX,
    LOGICAL_SLOT_SUFFIX,
    MATERIALIZED_VIEW,
    PLUGIN,
    SCHEMA,
    TG_OP,
    TRIGGER_FUNC,
    UPDATE,
)
from .exc import ForeignKeyError, LogicalSlotParseError, TableNotFoundError
from .settings import PG_SSLMODE, PG_SSLROOTCERT, QUERY_CHUNK_SIZE
from .trigger import CREATE_TRIGGER_TEMPLATE
from .utils import get_postgres_url
from .view import CreateIndex, CreateView, DropIndex, DropView

try:
    import citext  # noqa
except ImportError:
    pass

try:
    import geoalchemy2  # noqa
except ImportError:
    pass


logger = logging.getLogger(__name__)


class Base(object):
    def __init__(self, database, verbose=False, *args, **kwargs):
        """Initialize the base class constructor.

        Args:
            database: The database name
        """
        self.__engine = pg_engine(database, **kwargs)
        self.__schemas = None
        # models is a dict of f'{schema}.{table}'
        self.models = {}
        self.__metadata = {}
        self.verbose = verbose

    def connect(self) -> None:
        """Connect to database."""
        try:
            conn = self.__engine.connect()
            conn.close()
        except Exception as e:
            logger.exception(f"Cannot connect to database: {e}")
            raise

    def pg_settings(self, column) -> None:
        try:
            return self.fetchone(
                sa.select([sa.column("setting")])
                .select_from(sa.text("pg_settings"))
                .where(sa.column("name") == column),
                label="pg_settings",
            )[0]
        except (TypeError, IndexError):
            return None

    def has_permission(self, username: str, permission: str) -> bool:
        """Check if the given user is a superuser or replication user.

        Args:
            username (str): The username to check
            permission (str): The permission to check

        Returns:
            True if successful, False otherwise.

        """
        if permission not in ("usecreatedb", "usesuper", "userepl"):
            raise RuntimeError(f"Invalid user permission {permission}")

        try:
            return self.fetchone(
                sa.select([sa.column(permission)])
                .select_from(sa.text("pg_user"))
                .where(
                    sa.and_(
                        *[
                            sa.column("usename") == username,
                            sa.column(permission) == True,  # noqa
                        ]
                    )
                ),
                label="has_permission",
            )[0]
        except Exception as e:
            logger.exception(f"{e}")
        return False

    # Tables...
    def model(self, table: str, schema: str):
        """Get an SQLAlchemy model representation from a table.

        Args:
            table (str): The tablename
            schema (str): The database schema

        Returns:
            The SQLAlchemy aliased model representation

        """
        name = f"{schema}.{table}"
        if name not in self.models:
            if schema not in self.__metadata:
                metadata = sa.MetaData(schema=schema)
                metadata.reflect(self.__engine, views=True)
                self.__metadata[schema] = metadata
            metadata = self.__metadata[schema]
            if name not in metadata.tables:
                raise TableNotFoundError(
                    f'Table "{name}" not found in registry'
                )
            model = metadata.tables[name]
            model.append_column(sa.Column("xmin", sa.BigInteger))
            model.append_column(sa.Column("oid", sa.dialects.postgresql.OID))
            model = model.alias()
            setattr(
                model,
                "primary_keys",
                sorted([primary_key.key for primary_key in model.primary_key]),
            )
            self.models[f"{model.original}"] = model

        return self.models[name]

    @property
    def database(self):
        """str: Get the database name."""
        return self.__engine.url.database

    @property
    def session(self):
        Session = sessionmaker(bind=self.__engine.connect(), autoflush=True)
        return Session()

    @property
    def engine(self):
        """Get the database engine."""
        return self.__engine

    @property
    def schemas(self):
        """Get the database schema names."""
        if self.__schemas is None:
            self.__schemas = sa.inspect(self.engine).get_schema_names()
            for schema in BUILTIN_SCHEMAS:
                if schema in self.__schemas:
                    self.__schemas.remove(schema)
        return self.__schemas

    def tables(self, schema):
        """:obj:`list` of :obj:`str`: Get all tables.

        returns the fully qualified table name with schema {schema}.{table}
        """
        if schema not in self.__metadata:
            metadata = sa.MetaData(schema=schema)
            metadata.reflect(self.__engine)
            self.__metadata[schema] = metadata
        metadata = self.__metadata[schema]
        return metadata.tables.keys()

    def _get_schema(self, schema, table):
        pairs = table.split(".")
        if len(pairs) == 2:
            return pairs[0], pairs[1]
        if len(pairs) == 1:
            return schema, pairs[0]
        raise ValueError(f"Invalid definition {table} for schema: {schema}")

    def truncate_table(self, table, schema=SCHEMA):
        """Truncate a table.

        Note:
            we need to quote table names that can be reserved sql statements
            like user

        Args:
            table (str): The tablename
            schema (str): The database schema

        """
        if not table.startswith(f"{schema}."):
            table = f"{schema}.{table}"
        schema, table = self._get_schema(schema, table)
        logger.debug(f"Truncating table: {schema}.{table}")
        query = f'TRUNCATE TABLE "{schema}"."{table}" CASCADE'
        self.execute(query)

    def truncate_tables(self, tables, schema=SCHEMA):
        """Truncate all tables."""
        logger.debug(f"Truncating tables: {tables}")
        for table in tables:
            self.truncate_table(table, schema=schema)

    def truncate_schema(self, schema):
        """Truncate all tables in a schema."""
        logger.debug(f"Truncating schema: {schema}")
        self.truncate_tables(self.tables(schema), schema=schema)

    def truncate_schemas(self):
        """Truncate all tables in a database."""
        for schema in self.schemas:
            self.truncate_schema(schema)

    # Replication slots...
    def replication_slots(
        self,
        slot_name,
        plugin=PLUGIN,
        slot_type="logical",
    ):
        """List replication slots.

        SELECT * FROM PG_REPLICATION_SLOTS
        """
        return self.fetchall(
            sa.select(["*"])
            .select_from(sa.text("PG_REPLICATION_SLOTS"))
            .where(
                sa.and_(
                    *[
                        sa.column("slot_name") == slot_name,
                        sa.column("slot_type") == slot_type,
                        sa.column("plugin") == plugin,
                    ]
                )
            ),
            label="replication_slots",
        )

    def create_replication_slot(self, slot_name):
        """Create a replication slot.

        TODO:
        - Only create the replication slot if it does not exist
          otherwise warn that it already exists and return

        SELECT * FROM PG_REPLICATION_SLOTS
        """
        logger.debug(f"Creating replication slot: {slot_name}")
        return self.fetchone(
            sa.select(["*"]).select_from(
                sa.func.PG_CREATE_LOGICAL_REPLICATION_SLOT(
                    slot_name,
                    PLUGIN,
                )
            ),
            label="create_replication_slot",
        )

    def drop_replication_slot(self, slot_name):
        """Drop a replication slot."""
        logger.debug(f"Dropping replication slot: {slot_name}")
        if self.replication_slots(slot_name):
            try:
                return self.fetchone(
                    sa.select(["*"]).select_from(
                        sa.func.PG_DROP_REPLICATION_SLOT(slot_name),
                    ),
                    label="drop_replication_slot",
                )
            except Exception as e:
                logger.exception(f"{e}")
                raise

    def _logical_slot_changes(
        self,
        slot_name,
        func,
        txmin=None,
        txmax=None,
        upto_lsn=None,
        upto_nchanges=None,
    ):
        filters = []
        statement = sa.select(
            [sa.column("xid"), sa.column("data")]
        ).select_from(
            func(
                slot_name,
                upto_lsn,
                upto_nchanges,
            )
        )
        if txmin:
            filters.append(
                sa.cast(
                    sa.cast(sa.column("xid"), sa.Text),
                    sa.BigInteger,
                )
                >= txmin
            )
        if txmax:
            filters.append(
                sa.cast(
                    sa.cast(sa.column("xid"), sa.Text),
                    sa.BigInteger,
                )
                < txmax
            )
        if filters:
            statement = statement.where(sa.and_(*filters))
        return self.fetchall(statement)

    def logical_slot_get_changes(
        self,
        slot_name,
        txmin=None,
        txmax=None,
        upto_lsn=None,
        upto_nchanges=None,
    ):
        """Get/Consume changes from a logical replication slot.

        To get one change and data in existing replication slot:
        SELECT * FROM PG_LOGICAL_SLOT_GET_CHANGES('testdb', NULL, 1)

        To get ALL changes and data in existing replication slot:
        SELECT * FROM PG_LOGICAL_SLOT_GET_CHANGES('testdb', NULL, NULL)
        """
        return self._logical_slot_changes(
            slot_name,
            sa.func.PG_LOGICAL_SLOT_GET_CHANGES,
            txmin=txmin,
            txmax=txmax,
            upto_lsn=upto_lsn,
            upto_nchanges=upto_nchanges,
        )

    def logical_slot_peek_changes(
        self,
        slot_name,
        txmin=None,
        txmax=None,
        upto_lsn=None,
        upto_nchanges=None,
    ):
        """Peek a logical replication slot without consuming changes.

        SELECT * FROM PG_LOGICAL_SLOT_PEEK_CHANGES('testdb', NULL, 1)
        """
        return self._logical_slot_changes(
            slot_name,
            sa.func.PG_LOGICAL_SLOT_PEEK_CHANGES,
            txmin=txmin,
            txmax=txmax,
            upto_lsn=upto_lsn,
            upto_nchanges=upto_nchanges,
        )

    # Views...
    def _primary_keys(self, schema, tables):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa.exc.SAWarning)
            pg_class = self.model("pg_class", "pg_catalog")
            pg_index = self.model("pg_index", "pg_catalog")
            pg_attribute = self.model("pg_attribute", "pg_catalog")
            pg_namespace = self.model("pg_namespace", "pg_catalog")

        alias = pg_class.alias("x")
        return (
            sa.select(
                [
                    sa.func.REPLACE(
                        sa.func.REVERSE(
                            sa.func.SPLIT_PART(
                                sa.func.REVERSE(
                                    sa.cast(
                                        sa.cast(
                                            pg_index.c.indrelid,
                                            sa.dialects.postgresql.REGCLASS,
                                        ),
                                        sa.Text,
                                    )
                                ),
                                ".",
                                1,
                            )
                        ),
                        '"',
                        "",
                    ).label("table_name"),
                    sa.func.ARRAY_AGG(pg_attribute.c.attname).label(
                        "primary_keys"
                    ),
                ]
            )
            .join(
                pg_attribute,
                pg_attribute.c.attrelid == pg_index.c.indrelid,
            )
            .join(
                pg_class,
                pg_class.c.oid == pg_index.c.indexrelid,
            )
            .join(
                alias,
                alias.c.oid == pg_index.c.indrelid,
            )
            .join(
                pg_namespace,
                pg_namespace.c.oid == pg_class.c.relnamespace,
            )
            .where(
                *[
                    pg_namespace.c.nspname.notin_(["pg_catalog", "pg_toast"]),
                    pg_index.c.indisprimary,
                    sa.cast(
                        sa.cast(
                            pg_index.c.indrelid,
                            sa.dialects.postgresql.REGCLASS,
                        ),
                        sa.Text,
                    ).in_(
                        map(
                            self.__engine.dialect.identifier_preparer.quote,
                            tables,
                        )
                    ),
                    pg_attribute.c.attnum == sa.any_(pg_index.c.indkey),
                ]
            )
            .group_by(pg_index.c.indrelid)
        )

    def _foreign_keys(self, schema, tables):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa.exc.SAWarning)
            table_constraints = self.model(
                "table_constraints",
                "information_schema",
            )
            key_column_usage = self.model(
                "key_column_usage",
                "information_schema",
            )
            constraint_column_usage = self.model(
                "constraint_column_usage",
                "information_schema",
            )

        return (
            sa.select(
                [
                    table_constraints.c.table_name,
                    sa.func.ARRAY_AGG(
                        sa.cast(
                            key_column_usage.c.column_name,
                            sa.TEXT,
                        )
                    ).label("foreign_keys"),
                ]
            )
            .join(
                key_column_usage,
                sa.and_(
                    key_column_usage.c.constraint_name
                    == table_constraints.c.constraint_name,
                    key_column_usage.c.table_schema
                    == table_constraints.c.table_schema,
                    key_column_usage.c.table_schema == schema,
                ),
            )
            .join(
                constraint_column_usage,
                sa.and_(
                    constraint_column_usage.c.constraint_name
                    == table_constraints.c.constraint_name,
                    constraint_column_usage.c.table_schema
                    == table_constraints.c.table_schema,
                ),
            )
            .where(
                *[
                    table_constraints.c.table_name.in_(tables),
                    table_constraints.c.constraint_type == "FOREIGN KEY",
                ]
            )
            .group_by(table_constraints.c.table_name)
        )

    def create_view(self, schema, tables, user_defined_fkey_tables):
        """
        View describing primary_keys and foreign_keys for each table
        with an index on table_name

        This is only called once on bootstrap.
        It is used within the trigger function to determine what payload
        values to send to pg_notify.

        Since views cannot be modified, we query the existing view for exiting
        rows and union this to the next query.

        So if 'specie' was the only row before, and the next query returns
        'unit' and 'structure', we want to end up with the result below.

        table_name | primary_keys | foreign_keys
        -----------+--------------+--------------
        specie     | {id}         | {id, user_id}
        unit       | {id}         | {id, profile_id}
        structure  | {id}         | {id}
        """

        views = sa.inspect(self.engine).get_view_names(schema)

        rows = {}
        if MATERIALIZED_VIEW in views:
            for table_name, primary_keys, foreign_keys in self.fetchall(
                sa.select(["*"]).select_from(
                    sa.text(f"{schema}.{MATERIALIZED_VIEW}")
                )
            ):
                rows.setdefault(
                    table_name,
                    {"primary_keys": set([]), "foreign_keys": set([])},
                )
                if primary_keys:
                    rows[table_name]["primary_keys"] = set(primary_keys)
                if foreign_keys:
                    rows[table_name]["foreign_keys"] = set(foreign_keys)

            self.__engine.execute(DropView(schema, MATERIALIZED_VIEW))

        if schema != SCHEMA:
            for table in set(tables):
                tables.add(f"{schema}.{table}")

        for table_name, columns in self.fetchall(
            self._primary_keys(schema, tables)
        ):
            rows.setdefault(
                table_name,
                {"primary_keys": set([]), "foreign_keys": set([])},
            )
            if columns:
                rows[table_name]["primary_keys"] |= set(columns)

        for table_name, columns in self.fetchall(
            self._foreign_keys(schema, tables)
        ):
            rows.setdefault(
                table_name,
                {"primary_keys": set([]), "foreign_keys": set([])},
            )
            if columns:
                rows[table_name]["foreign_keys"] |= set(columns)

        if user_defined_fkey_tables:
            for table_name, columns in user_defined_fkey_tables.items():
                rows.setdefault(
                    table_name,
                    {"primary_keys": set([]), "foreign_keys": set([])},
                )
                if columns:
                    rows[table_name]["foreign_keys"] |= set(columns)

        if not rows:
            rows.setdefault(
                None,
                {"primary_keys": set([]), "foreign_keys": set([])},
            )

        statement = sa.select(
            Values(
                sa.column("table_name"),
                sa.column("primary_keys"),
                sa.column("foreign_keys"),
            )
            .data(
                [
                    (
                        table_name,
                        array(fields["primary_keys"])
                        if fields.get("primary_keys")
                        else None,
                        array(fields.get("foreign_keys"))
                        if fields.get("foreign_keys")
                        else None,
                    )
                    for table_name, fields in rows.items()
                ]
            )
            .alias("t")
        )

        logger.debug(f"Creating view: {schema}.{MATERIALIZED_VIEW}")
        self.__engine.execute(CreateView(schema, MATERIALIZED_VIEW, statement))
        self.__engine.execute(DropIndex("_idx"))
        self.__engine.execute(
            CreateIndex("_idx", schema, MATERIALIZED_VIEW, ["table_name"])
        )
        logger.debug(f"Created view: {schema}.{MATERIALIZED_VIEW}")

    def drop_view(self, schema):
        logger.debug(f"Dropping view: {schema}.{MATERIALIZED_VIEW}")
        self.__engine.execute(DropView(schema, MATERIALIZED_VIEW))
        logger.debug(f"Dropped view: {schema}.{MATERIALIZED_VIEW}")

    # Triggers...
    def create_triggers(self, schema, tables=None):
        """Create a database triggers."""
        self.execute(
            CREATE_TRIGGER_TEMPLATE.replace(
                MATERIALIZED_VIEW, f"{schema}.{MATERIALIZED_VIEW}"
            )
        )
        views = sa.inspect(self.engine).get_view_names(schema)
        queries = []
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
            if (tables and table not in tables) or (table in views):
                continue
            logger.debug(f"Creating trigger on table: {schema}.{table}")
            for name, for_each, tg_op in [
                ("notify", "ROW", ["INSERT", "UPDATE", "DELETE"]),
                ("truncate", "STATEMENT", ["TRUNCATE"]),
            ]:
                self.drop_triggers(schema, [table])
                queries.append(
                    sa.DDL(
                        f"CREATE TRIGGER {table}_{name} "
                        f'AFTER {" OR ".join(tg_op)} ON "{schema}"."{table}" '
                        f"FOR EACH {for_each} EXECUTE PROCEDURE "
                        f"{TRIGGER_FUNC}()",
                    )
                )

        for query in queries:
            self.execute(query)

    def drop_triggers(self, schema, tables=None):
        """Drop all pgsync defined triggers in database."""
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
            if tables and table not in tables:
                continue
            logger.debug(f"Dropping trigger on table: {schema}.{table}")
            for name in ("notify", "truncate"):
                query = (
                    f"DROP TRIGGER IF EXISTS {table}_{name} ON "
                    f'"{schema}"."{table}"'
                )
                self.execute(query)

    def disable_triggers(self, schema):
        """Disable all pgsync defined triggers in database."""
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
            logger.debug(f"Disabling trigger on table: {schema}.{table}")
            for name in ("notify", "truncate"):
                query = (
                    f'ALTER TABLE "{schema}"."{table}" '
                    f"DISABLE TRIGGER {table}_{name}"
                )
                self.execute(query)

    def enable_triggers(self, schema):
        """Enable all pgsync defined triggers in database."""
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
            logger.debug(f"Enabling trigger on table: {schema}.{table}")
            for name in ("notify", "truncate"):
                query = (
                    f'ALTER TABLE "{schema}"."{table}" '
                    f"ENABLE TRIGGER {table}_{name}"
                )
                self.execute(query)

    @property
    def txid_current(self):
        """
        Get last committed transaction id from the database.

        SELECT txid_current()
        """
        return self.fetchone(
            sa.select(["*"]).select_from(sa.func.TXID_CURRENT()),
            label="txid_current",
        )[0]

    def parse_value(self, type_, value):
        """
        Parse datatypes from db.

        NB: All integers are long in python3 and call to convert is just int
        """
        if value.lower() == "null":
            return None

        if type_.lower() in (
            "bigint",
            "bigserial",
            "int",
            "int2",
            "int4",
            "int8",
            "integer",
            "serial",
            "serial2",
            "serial4",
            "serial8",
            "smallint",
            "smallserial",
        ):
            try:
                value = int(value)
            except ValueError:
                raise
        if type_.lower() in (
            "char",
            "character",
            "character varying",
            "text",
            "uuid",
            "varchar",
        ):
            value = value.lstrip("'").rstrip("'")
        if type_.lower() == "boolean":
            try:
                value = bool(value)
            except ValueError:
                raise
        if type_.lower() in (
            "double precision",
            "float4",
            "float8",
            "real",
        ):
            try:
                value = float(value)
            except ValueError:
                raise
        return value

    def parse_logical_slot(self, row):
        def _parse_logical_slot(data):

            while True:

                match = LOGICAL_SLOT_SUFFIX.search(data)
                if not match:
                    break

                key = match.groupdict().get("key")
                if key:
                    key = key.replace('"', "")
                value = match.groupdict().get("value")
                type_ = match.groupdict().get("type")

                value = self.parse_value(type_, value)

                # set data for next iteration of the loop
                data = f"{data[match.span()[1]:]} "
                yield key, value

        payload = dict(schema=None, tg_op=None, table=None, old={}, new={})

        match = LOGICAL_SLOT_PREFIX.search(row)
        if not match:
            raise LogicalSlotParseError(f"No match for row: {row}")

        payload.update(match.groupdict())
        span = match.span()
        # trailing space is deliberate
        suffix = f"{row[span[1]:]} "

        if "old-key" and "new-tuple" in suffix:
            # this can only be an UPDATE operation
            if payload["tg_op"] != UPDATE:
                msg = f"Unknown {payload['tg_op']} operation for row: {row}"
                raise LogicalSlotParseError(msg)

            i = suffix.index("old-key:")
            if i > -1:
                j = suffix.index("new-tuple:")
                s = suffix[i + len("old-key:") : j]
                for key, value in _parse_logical_slot(s):
                    payload["old"][key] = value

            i = suffix.index("new-tuple:")
            if i > -1:
                s = suffix[i + len("new-tuple:") :]
                for key, value in _parse_logical_slot(s):
                    payload["new"][key] = value
        else:
            # this can be an INSERT, DELETE, UPDATE or TRUNCATE operation
            if payload["tg_op"] not in TG_OP:
                msg = f"Unknown {payload['tg_op']} operation for row: {row}"
                raise LogicalSlotParseError(msg)

            for key, value in _parse_logical_slot(suffix):
                payload["new"][key] = value

        return payload

    # Querying...

    def execute(self, statement, values=None, options=None):
        """Execute a query statement."""
        conn = self.__engine.connect()
        try:
            if options:
                conn = conn.execution_options(**options)
            conn.execute(statement, values)
            conn.close()
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise

    def fetchone(self, statement, label=None, literal_binds=False):
        """Fetch one row query."""
        if self.verbose:
            compiled_query(statement, label=label, literal_binds=literal_binds)

        conn = self.__engine.connect()
        try:
            row = conn.execute(statement).fetchone()
            conn.close()
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise
        return row

    def fetchall(self, statement, label=None, literal_binds=False):
        """Fetch all rows from a query statement."""
        if self.verbose:
            compiled_query(statement, label=label, literal_binds=literal_binds)

        conn = self.__engine.connect()
        try:
            rows = conn.execute(statement).fetchall()
            conn.close()
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise
        return rows

    def fetchmany(self, statement, chunk_size=None):
        chunk_size = chunk_size or QUERY_CHUNK_SIZE
        logger.debug(f"Chunk size: {chunk_size}")
        with self.__engine.begin() as conn:
            logger.debug(f"Executing query...")
            result = conn.execution_options(stream_results=True).execute(
                statement.select()
            )
            logger.debug(f"Processing results...")
            for partition in result.partitions(chunk_size):
                for keys, row, *primary_keys in partition:
                    yield keys, row, primary_keys

    def fetchcount(self, statement):
        logger.debug(f"Fetching count...")
        with self.__engine.connect() as conn:
            return conn.execute(
                statement.original.with_only_columns(
                    [sa.func.COUNT()]
                ).order_by(None)
            ).scalar()


# helper methods


def subtransactions(session):
    """Context manager for executing code within a sub-transaction."""

    class ControlledExecution:
        def __init__(self, session):
            self.session = session

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            try:
                self.session.commit()
            except Exception:
                self.session.rollback()
                raise

    return ControlledExecution(session)


def _get_foreign_keys(model_a, model_b):

    foreign_keys = collections.defaultdict(list)

    if model_a.foreign_keys:

        for key in model_a.original.foreign_keys:
            if key._table_key() == str(model_b.original):
                foreign_keys[str(key.parent.table)].append(key.parent)
                foreign_keys[str(key.column.table)].append(key.column)

    if not foreign_keys:

        if model_b.original.foreign_keys:

            for key in model_b.original.foreign_keys:
                if key._table_key() == str(model_a.original):
                    foreign_keys[str(key.parent.table)].append(key.parent)
                    foreign_keys[str(key.column.table)].append(key.column)

    if not foreign_keys:
        raise ForeignKeyError(
            f"No foreign key relationship between "
            f'"{model_a.original}" and '
            f'"{model_b.original}"'
        )

    return foreign_keys


def get_foreign_keys(node_a, node_b):
    """Return dict of single foreign key with multiple columns.

    e.g:
        {
            fk1['table_1']: [column_1, column_2, column_N],
            fk2['table_2']: [column_1, column_2, column_N],
        }

    column_1, column_2, column_N are of type ForeignKeyContraint
    """
    foreign_keys = {}
    # if either offers a foreign_key via relationship, use it!
    if (
        node_a.relationship.foreign_key.parent
        or node_b.relationship.foreign_key.parent
    ):
        if node_a.relationship.foreign_key.parent:
            foreign_keys[node_a.parent.name] = sorted(
                node_a.relationship.foreign_key.parent
            )
            foreign_keys[node_a.name] = sorted(
                node_a.relationship.foreign_key.child
            )
        if node_b.relationship.foreign_key.parent:
            foreign_keys[node_b.parent.name] = sorted(
                node_b.relationship.foreign_key.parent
            )
            foreign_keys[node_b.name] = sorted(
                node_b.relationship.foreign_key.child
            )
    else:
        for table, columns in _get_foreign_keys(
            node_a.model,
            node_b.model,
        ).items():
            foreign_keys[table] = sorted([column.name for column in columns])
    return foreign_keys


def pg_engine(
    database,
    user=None,
    host=None,
    password=None,
    port=None,
    echo=False,
    sslmode=None,
    sslrootcert=None,
):
    connect_args = {}
    sslmode = sslmode or PG_SSLMODE
    sslrootcert = sslrootcert or PG_SSLROOTCERT

    if sslmode:
        if sslmode not in (
            "allow",
            "disable",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ):
            raise ValueError(f'Invalid sslmode: "{sslmode}"')
        connect_args["sslmode"] = sslmode

    if sslrootcert:
        if not os.path.exists(sslrootcert):
            raise IOError(
                f'"{sslrootcert}" not found.\n'
                f"Provide a valid file containing SSL certificate "
                f"authority (CA) certificate(s)."
            )
        connect_args["sslrootcert"] = sslrootcert

    url = get_postgres_url(
        database,
        user=user,
        host=host,
        password=password,
        port=port,
    )
    return sa.create_engine(url, echo=echo, connect_args=connect_args)


def pg_execute(engine, query, values=None, options=None):
    options = options or {"isolation_level": "AUTOCOMMIT"}
    conn = engine.connect()
    try:
        if options:
            conn = conn.execution_options(**options)
        conn.execute(query, values)
        conn.close()
    except Exception as e:
        logger.exception(f"Exception {e}")
        raise


def create_schema(engine, schema):
    """Create database schema."""
    if schema != SCHEMA:
        engine.execute(sa.schema.CreateSchema(schema))


def create_database(database, echo=False):
    """Create a database."""
    logger.debug(f"Creating database: {database}")
    engine = pg_engine(database="postgres", echo=echo)
    pg_execute(engine, f'CREATE DATABASE "{database}"')
    logger.debug(f"Created database: {database}")


def drop_database(database, echo=False):
    """Drop a database."""
    logger.debug(f"Dropping database: {database}")
    engine = pg_engine(database="postgres", echo=echo)
    pg_execute(engine, f'DROP DATABASE IF EXISTS "{database}"')
    logger.debug(f"Dropped database: {database}")


def create_extension(database, extension, echo=False):
    """Create a database extension."""
    logger.debug(f"Creating extension: {extension}")
    engine = pg_engine(database=database, echo=echo)
    pg_execute(engine, f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
    logger.debug(f"Created extension: {extension}")


def drop_extension(database, extension, echo=False):
    """Drop a database extension."""
    logger.debug(f"Dropping extension: {extension}")
    engine = pg_engine(database=database, echo=echo)
    pg_execute(engine, f'DROP EXTENSION IF EXISTS "{extension}"')
    logger.debug(f"Dropped extension: {extension}")


def compiled_query(query, label=None, literal_binds=False):
    """Compile an SQLAlchemy query with an optional label."""
    query = str(
        query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": literal_binds},
        )
    )
    query = sqlparse.format(query, reindent=True, keyword_case="upper")
    if label:
        logger.debug(f"\033[4m{label}:\033[0m\n{query}")
        sys.stdout.write(f"\033[4m{label}:\033[0m\n{query}")
    else:
        logging.debug(f"{query}")
        sys.stdout.write(f"{query}")
