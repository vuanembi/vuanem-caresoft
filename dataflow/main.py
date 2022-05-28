import argparse
import logging
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.auth import default

from caresoft.pipeline import pipelines, interface

DATASET = "dev_Caresoft"

def main(args: argparse.Namespace, beam_args: list[str]):
    options = PipelineOptions(
        beam_args,
        runner=args.runner,
        project=args.project,
        temp_location=args.temp_location,
        region=args.region,
        save_main_session=True,
    )

    body = json.loads(args.input)
    params = {k: v for k, v in body.items() if k != "table"}

    pipeline: interface.Pipeline = pipelines[body["table"]]

    with beam.Pipeline(options=options) as p:
        data = (
            p
            | "AddParams" >> beam.Create([params])
            | "Get" >> beam.FlatMap(pipeline.get)
            | "Transform" >> beam.Map(pipeline.transform)
        )

        data | "Load" >> beam.io.WriteToBigQuery(
            pipeline.name,
            DATASET,
            schema={"fields": pipeline.schema},
            write_disposition="WRITE_APPEND" if pipeline.id_key else "WRITE_TRUNCATE",
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", type=str)
    parser.add_argument("--runner", type=str, default="DirectRunner")
    parser.add_argument("--project", type=str, default=default()[1])
    parser.add_argument(
        "--temp_location",
        type=str,
        default="gs://vuanem-caresoft/temp",
    )
    parser.add_argument("--region", type=str, default="us-central1")

    args, beam_args = parser.parse_known_args()

    main(args, beam_args)
