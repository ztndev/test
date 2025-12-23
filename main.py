from prefect import flow
from prefect.logging import get_run_logger
from prefect_shell import ShellOperation


@flow
def fetch_runner():
    logger = get_run_logger()
    logger.info(
        ShellOperation(commands=["curl -s https://api.ipify.org", "uname"]).run()
    )


if __name__ == "__main__":
    fetch_runner()
