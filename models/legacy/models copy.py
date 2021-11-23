from abc import ABCMeta, abstractmethod
import importlib

from libs.caresoft import get_simple
from libs.bigquery import load_simple


class ICaresoft(metaclass=ABCMeta):
    def __init__(self):
        self.table = self.__class__.__name__

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

    @abstractmethod
    def transform(self, rows):
        pass

    @abstractmethod
    def run(self):
        pass


class ICaresoftDimensions(ICaresoft):
    def run(self, dataset, **kwargs):
        data = get_simple(self.endpoint, self.row_key)
        response = {
            "table": self.table,
            "num_processed": len(data),
        }
        if len(data) > 0:
            response["output_rows"] = load_simple(
                dataset,
                self.table,
                self.schema,
                data,
            )
        return response


class ICaresoftIncremental(ICaresoft):
    @property
    @abstractmethod
    def keys(self):
        pass

    @property
    @abstractmethod
    def detail_key(self):
        pass


class ICaresoftDetails(ICaresoft):
    @property
    @abstractmethod
    def keys(self):
        pass

    @property
    @abstractmethod
    def detail_key(self):
        pass
