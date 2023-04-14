from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_to_gcs_hw import etl_web_to_gcs

github_block = GitHub.load("zoomcamp-github")

github_dep= Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name='github_flow',
    infrastructure=github_block
    )
if __name__ == '__main__':
   github_dep.apply()