import importlib

def factory(module):
    def _factory(item):
        try:
            return getattr(importlib.import_module(f"{module}.{item}"), item)
        except (ImportError, AttributeError):
            raise ValueError(item)
    return _factory

pipelines_factory = factory("models")
controller_factory = factory("controllers")
