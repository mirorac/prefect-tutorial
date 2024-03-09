import httpx
from prefect import flow
from prefect.artifacts import create_markdown_artifact

@flow
def get_repo_info():
    url = "https://api.github.com/repos/PrefectHQ/prefect"
    response = httpx.get(url)
    response.raise_for_status()
    repo = response.json()
    print("PrefectHQ/prefect repository statistics 🤓:")
    print(f"Stars 🌠 : {repo['stargazers_count']}")
    print(f"Forks 🍴 : {repo['forks_count']}")

    markdown_report = f"""## Report on Prefect Repository

PrefectHQ/prefect repository statistics:
Stars 🌠 : {repo['stargazers_count']}
Forks 🍴 : {repo['forks_count']}
    """
    create_markdown_artifact(
        key="prefect-repo-report",
        markdown=markdown_report,
        description="Prefect Repository Report",
    )

if __name__ == "__main__":
    get_repo_info.from_source(
        source="https://github.com/mirorac/prefect-tutorial.git", 
        entrypoint="repo_info.py:get_repo_info"
    ).deploy(name="my-first-deployment", work_pool_name="default-agent-pool")
