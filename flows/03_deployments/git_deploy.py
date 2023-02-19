from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.filesystems import GitHub

github_block = GitHub.load("zoom-git")

git_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="git-flow"
)


if __name__ == "__main__":
    git_dep.apply()