from typing import Any
import argparse
import logging
import json
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.auth import default

from caresoft.pipeline import pipelines, interface
from secret_manager import get_token


DATASET = "dev2_Caresoft"


class GetService(beam.DoFn):
    def __init__(self, get_fn):
        self.get_fn = get_fn

    def setup(self):
        os.environ["ACCESS_TOKEN"] = get_token()

    def process(self, element):
        return self.get_fn(element)


class PipelineService(beam.PTransform):
    def __init__(self, pipeline: interface.Pipeline):
        self.pipeline = pipeline

    def expand(self, collection: beam.PCollection) -> beam.PCollection:
        data = (
            collection
            | "Get" >> beam.ParDo(GetService(self.pipeline.get))
            # | "Get" >> beam.FlatMap(self.pipeline.get)
            | "Transform" >> beam.Map(self.pipeline.transform)
        )

        data | "Load" >> beam.io.WriteToBigQuery(
            f"p_{self.pipeline.name}" if self.pipeline.key else self.pipeline.name,
            DATASET,
            schema={"fields": self.pipeline.schema},
            write_disposition="WRITE_APPEND" if self.pipeline.key else "WRITE_TRUNCATE",
            additional_bq_parameters={
                "timePartitioning": {
                    "field": self.pipeline.key.partition,
                    "type": "DAY",
                },
            }
            if self.pipeline.key
            else None,
        )

        return data


class ListConcat(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, acc, cur):
        return [*acc, cur]

    def merge_accumulators(self, accs):
        return [i for j in accs for i in j]

    def extract_output(self, acc):
        return acc


def main(args: argparse.Namespace, beam_args: list[str]):
    options = PipelineOptions(
        beam_args,
        runner=args.runner,
        project=args.project,
        temp_location=args.temp_location,
        region=args.region,
        setup_file="./setup.py",
    )

    options.view_as(SetupOptions).save_main_session = True

    body = json.loads(args.input)
    params: dict[str, Any] = {k: v for k, v in body.items() if k != "table"}

    pipeline: interface.Pipeline = pipelines[body["table"]]

    with beam.Pipeline(options=options) as p:
        primary_params = p | "AddParams" >> beam.Create([params])

        primary_data = primary_params | "PrimaryPipeline" >> PipelineService(pipeline)

        if pipeline.details and pipeline.key:
            x = (
                primary_data
                | "GetIds" >> beam.Map(lambda ele: ele[pipeline.key.id])
                | "CombineIds" >> beam.CombineGlobally(ListConcat())
                | "DetailsPipeline" >> PipelineService(pipeline.details)
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", type=str)
    # parser.add_argument("--runner", type=str, default="DirectRunner")
    parser.add_argument("--runner", type=str, default="DataFlowRunner")
    parser.add_argument("--project", type=str, default=default()[1])
    parser.add_argument(
        "--temp_location",
        type=str,
        default="gs://vuanem-caresoft/temp",
    )
    parser.add_argument("--region", type=str, default="us-central1")

    args, beam_args = parser.parse_known_args()

    main(args, beam_args)
