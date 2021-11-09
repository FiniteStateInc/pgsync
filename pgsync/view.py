"""PGSync views."""
<<<<<<< HEAD
=======
from abc import ABC
from typing import List

from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
>>>>>>> b4deea3 (Replacing trigger usage by leaning on replication_slot a bit more)
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement


<<<<<<< HEAD
class CreateView(DDLElement):
    def __init__(self, schema, name, selectable, materialized=True):
        self.schema = schema
        self.name = name
        self.selectable = selectable
        self.materialized = materialized


@compiler.compiles(CreateView)
def compile_create_view(element, compiler, **kwargs):
    statement = compiler.sql_compiler.process(
        element.selectable,
        literal_binds=True,
    )
    materialized = "MATERIALIZED" if element.materialized else ""
=======
class CreateView(DDLElement, ABC):
    def __init__(
        self,
        schema: str,
        name: str,
        selectable: Select,
        materialized: bool = True,
    ):
        self.schema: str = schema
        self.name: str = name
        self.selectable: Select = selectable
        self.materialized: bool = materialized


@compiler.compiles(CreateView)
def compile_create_view(element: CreateView, compiler: PGDDLCompiler) -> str:
    statement: str = compiler.sql_compiler.process(
        element.selectable,
        literal_binds=True,
    )
    materialized: str = "MATERIALIZED" if element.materialized else ""
>>>>>>> b4deea3 (Replacing trigger usage by leaning on replication_slot a bit more)
    return (
        f'CREATE {materialized} VIEW "{element.schema}"."{element.name}" AS '
        f"{statement}"
    )


class DropView(DDLElement):
    def __init__(self, schema, name, materialized=True, cascade=True):
        self.schema = schema
        self.name = name
        self.materialized = materialized
        self.cascade = cascade


@compiler.compiles(DropView)
<<<<<<< HEAD
def compile_drop_view(element, compiler, **kwargs):
    materialized = "MATERIALIZED" if element.materialized else ""
    cascade = "CASCADE" if element.cascade else ""
=======
def compile_drop_view(element: CreateView) -> str:
    materialized: str = "MATERIALIZED" if element.materialized else ""
    cascade: str = "CASCADE" if element.cascade else ""
>>>>>>> b4deea3 (Replacing trigger usage by leaning on replication_slot a bit more)
    return (
        f"DROP {materialized} VIEW IF EXISTS "
        f'"{element.schema}"."{element.name}" {cascade}'
    )


class CreateIndex(DDLElement):
    def __init__(self, name, schema, view, columns):
        self.schema = schema
        self.name = name
        self.view = view
        self.columns = columns


@compiler.compiles(CreateIndex)
<<<<<<< HEAD
def compile_create_index(element, compiler, **kwargs):
=======
def compile_create_index(element: CreateView) -> str:
>>>>>>> b4deea3 (Replacing trigger usage by leaning on replication_slot a bit more)
    return (
        f"CREATE UNIQUE INDEX {element.name} ON "
        f'"{element.schema}"."{element.view}" ({", ".join(element.columns)})'
    )


class DropIndex(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(DropIndex)
<<<<<<< HEAD
def compile_drop_index(element, compiler, **kwargs):
=======
def compile_drop_index(element: CreateView) -> str:
>>>>>>> b4deea3 (Replacing trigger usage by leaning on replication_slot a bit more)
    return f"DROP INDEX IF EXISTS {element.name}"
