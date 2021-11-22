import requests

from libs.caresoft import get_dimensions
from controller.pipelines.base import transform_and_load

def run(model, start, end):
    with requests.Session() as session:
        return transform_and_load(model, get_dimensions(model, session))
