import httpx
from prefect import flow
from prefect.artifacts import create_markdown_artifact

# airbyte sync
from prefect_airbyte.server import AirbyteServer
from prefect_airbyte.connections import AirbyteConnection
from prefect_airbyte.flows import run_connection_sync

# dbt runs via docker
from prefect_docker import DockerHost
from prefect_docker.containers import create_docker_container

# shell
from prefect_shell import ShellOperation

airbyte_server = AirbyteServer(server_host='host.docker.internal', server_port=8000)

connection = AirbyteConnection(
    airbyte_server=airbyte_server,
    connection_id="6220f018-2f19-4280-92f9-d75572fe7094",
    status_updates=True,
)

@flow(name="DBT Transformation")
def run_dbt():
    # Run port listening to docker host machine
    # https://stackoverflow.com/a/39858696/1353874
    # socat TCP-LISTEN:2375,reuseaddr,fork UNIX-CONNECT:/var/run/docker.sock &
    # host = DockerHost(
    #     base_url="tcp://host.docker.internal:2375",
    #     max_pool_size=4
    # )
    # container = create_docker_container(
    #     docker_host=host,

    # )
    ShellOperation(
        # commands=["echo \"ls -l\" > /hostpipe"]
        commands=[
            "ls -l"
        ]
    ).run()



@flow(name="Run Data Pipeline")
def run_pipeline():

    # sync data
    sync_result = run_connection_sync(airbyte_connection=connection)

    # run dbt
    # run_dbt()

    markdown_report = f"""## Report on Text Pipeline Run

### Sync result
- created_at: {sync_result.created_at}
- job_status: {sync_result.job_status}
- job_id: {sync_result.job_id}
- records_synced: {sync_result.records_synced}
- updated_at: {sync_result.updated_at}

    """
    create_markdown_artifact(
        key="test-pipline-report",
        markdown=markdown_report,
        description="Test Client Pipeline Run",
    )

if __name__ == "__main__":
    # run_pipeline()
    run_dbt()