from abc import ABCMeta, abstractmethod
import importlib

from sqlalchemy import Table, MetaData

from components.getter import (
    IncrementalDetailsGetter,
    IncrementalStandardGetter,
    SimpleGetter,
    IncrementalGetter,
    DetailsGetter,
)
from components.loader import (
    BigQuerySimpleLoader,
    BigQueryIncrementalLoader,
    PostgresIncrementalLoader,
    PostgresStandardLoader,
)

from components.utils import TIMESTAMP_FORMAT


metadata = MetaData(schema="Caresoft")


class Caresoft(metaclass=ABCMeta):
    @staticmethod
    def factory(table, start, end):
        try:
            module = importlib.import_module(f"models.{table}")
            model = getattr(module, table)
            return model(start=start, end=end)
        except (ImportError, AttributeError):
            raise ValueError(table)

    def __init__(self, **kwargs):
        self.table = self.__class__.__name__
        self.model = Table(self.table, metadata, *self.columns)
        self.start, self.end = kwargs.get("start"), kwargs.get("end")
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
        if getattr(self._getter, "start", None) and getattr(self._getter, "end", None):
            response["start"] = self._getter.start.strftime(TIMESTAMP_FORMAT)
            response["end"] = self._getter.end.strftime(TIMESTAMP_FORMAT)
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = [loader.load(rows) for loader in self._loader]
            response["loads"] = loads
        return response


class CaresoftIncremental(Caresoft):
    loader = [
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


class CaresoftIncrementalStandard(CaresoftIncremental):
    getter = IncrementalStandardGetter


class CaresoftIncrementalDetails(CaresoftIncremental):
    getter = IncrementalDetailsGetter


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
    ],
}
