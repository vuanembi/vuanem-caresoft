from abc import ABCMeta, abstractmethod
import importlib

from sqlalchemy import Table, MetaData

from components.getter import SimpleGetter, IncrementalGetter, DetailsGetter
from components.loader import (
    BigQuerySimpleLoader,
    BigQueryIncrementalLoader,
    PostgresIncrementalLoader,
    PostgresStandardLoader,
)

from components.utils import TIMESTAMP_FORMAT, COUNT


metadata = MetaData(schema="Caresoft")


class Caresoft(metaclass=ABCMeta):
    @staticmethod
    def factory(table, start, end):
        try:
            module = importlib.import_module(f"models.{table}")
            model = getattr(module, table)
            return model(start, end)
        except (ImportError, AttributeError):
            raise ValueError(table)

    def __init__(self, *args):
        self.table = self.__class__.__name__
        self.model = Table(self.table, metadata, *self.columns)
        self._getter = self.getter(self)
        self._loader = [loader(self) for loader in self.loader]

    @property
    @abstractmethod
    def endpoint(self):
        pass

    @property
    @abstractmethod
    def row_key(self):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    @abstractmethod
    def columns(self):
        pass

    @abstractmethod
    def transform(self, rows):
        pass

    def run(self):
        rows = self._getter.get()
        response = {
            "table": self.table,
            "num_processed": len(rows),
        }
        if getattr(self, "start", None) and getattr(self, "end", None):
            response["start"] = self.start.strftime(TIMESTAMP_FORMAT)
            response["end"] = self.end.strftime(TIMESTAMP_FORMAT)
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = [loader.load(rows) for loader in self._loader]
            response["loads"] = loads
        return response


class CaresoftStatic(Caresoft):
    getter = SimpleGetter
    loader = [
        BigQuerySimpleLoader,
        PostgresStandardLoader,
    ]


class CaresoftIncremental(Caresoft):
    getter = IncrementalGetter
    load = [
        BigQueryIncrementalLoader,
        PostgresIncrementalLoader,
    ]

    def __init__(self, start, end):
        self.start, self.end = start, end
        super().__init__(self)

    @property
    @abstractmethod
    def keys(self):
        pass

    @property
    @abstractmethod
    def params(self):
        pass


class CaresoftIncrementalStandard(CaresoftIncremental):
    @property
    def params(self):
        return {
            "start_time_since": self.start.strftime(TIMESTAMP_FORMAT),
            "start_time_to": self.end.strftime(TIMESTAMP_FORMAT),
            "count": COUNT,
            "order_by": "start_time",
            "order_type": "asc",
        }


class CaresoftIncrementalDetails(CaresoftIncremental):
    @property
    def params(self):
        return {
            "updated_since": self.start.strftime(TIMESTAMP_FORMAT),
            "updated_to": self.end.strftime(TIMESTAMP_FORMAT),
            "count": COUNT,
            "order_by": "updated_at",
            "order_type": "asc",
        }


class CaresoftDetails(Caresoft):
    getter = DetailsGetter
    loader = [
        BigQueryIncrementalLoader,
        PostgresIncrementalLoader,
    ]

    @property
    @abstractmethod
    def keys(self):
        pass

    @property
    @abstractmethod
    def parent(self):
        pass

    @property
    @abstractmethod
    def detail_key(self):
        pass


TABLES = {
    "static": [
        "Agents",
        "Groups",
        "Services",
        "ContactsCustomFields",
        "TicketsCustomFields",
    ],
    "incre": [
        "Calls",
        "Contacts",
        "Tickets",
    ],
    "details": [
        "ContactsDetails",
        "TicketsDetails",
    ]
}
