from dagster import Definitions, EnvVar
from dagster_dbt import DbtCliResource
from dagster_airbyte import AirbyteWorkspace, build_airbyte_assets_definitions

from .assets import jaffle_shop_dbt_assets
from .project import jaffle_shop_project
from .schedules import schedules


airbyte = AirbyteWorkspace(
    rest_api_base_url="http://localhost:8000/api/public/v1",
    configuration_api_base_url="http://localhost:8000/api/v1",
    workspace_id=EnvVar("AIRBYTE_WORKSPACE_ID"),
    client_id=EnvVar("AIRBYTE_CLIENT_ID"),
    client_secret=EnvVar("AIRBYTE_CLIENT_SECRET"),
)

airbyte_assets = build_airbyte_assets_definitions(workspace=airbyte)


defs = Definitions(
    assets=[
        *airbyte_assets,
        jaffle_shop_dbt_assets,
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=jaffle_shop_project),
        "airbyte": airbyte,
    },
)
