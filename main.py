from controller.pipelines import factory, run
from controller.tasks import orchestrate

DATASET = "Caresoft"


def main(request) -> dict:
    data = request.get_json()
    print(data)

    if "tasks" in data:
        results = orchestrate(data['tasks'])
    elif "table" in data:
        results = run(DATASET, factory(data["table"]), data)
    else:
        raise ValueError(data)

    response = {
        "pipelines": "Caresoft",
        "results": results,
    }
    print(response)
    return response
