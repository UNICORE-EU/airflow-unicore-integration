from typing import Dict
from typing import List

from airflow_unicore_integration.operators.unicore_operators import (
    UnicoreGenericOperator,
)


class UnicoreContainerOperator(UnicoreGenericOperator):
    def __init__(
        self, name: str, docker_image_url: str, command: str, options: str | None = None, **kwargs
    ):
        params: Dict[str, str | List[str]] = {"COMMAND": command, "IMAGE_URL": docker_image_url}
        if options is not None:
            params["OPTIONS"] = options
        super().__init__(
            name=name,
            application_name="CONTAINER",
            application_version="1.0",
            parameters=params,
            **kwargs,
        )
