from typing import Any

from caresoft.caresoft_controller import caresoft_controller
from tasks.tasks_service import create_cron_tasks_service


def main(request):
    data: dict[str, Any] = request.get_json()
    print(data)

    if "tasks" in data:
        response = create_cron_tasks_service(data)
    elif "table" in data:
        response = caresoft_controller(data)
    else:
        raise ValueError(data)

    print(response)
    return response
