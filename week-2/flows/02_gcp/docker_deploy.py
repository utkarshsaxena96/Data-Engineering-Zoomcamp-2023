from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_master_flow import etl_master_flow

docker_block = DockerContainer.load("prefect-docker-block")

docker_dep = Deployment.build_from_flow(
    flow=etl_master_flow,
    name="docker-master-flow",
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()