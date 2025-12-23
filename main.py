import subprocess

from prefect import flow
from prefect.logging import get_run_logger


@flow
def run_command():
    """
    Run a shell command and log its output in real-time.

    Returns:
        int: Return code of the command
    """
    command = "echo 'Hello, World!' && ls -la && uname"
    logger = get_run_logger()
    logger.info(f"Starting command: {command}")
    try:
        process = subprocess.Popen(
            command,
            shell=isinstance(command, str),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        for line in process.stdout:
            line = line.rstrip()
            if line:
                logger.info(f"STDOUT: {line}")


        _, stderr = process.communicate()
        if stderr:
            for line in stderr.splitlines():
                if line:
                    logger.error(f"STDERR: {line}")


        return_code = process.returncode
        if return_code == 0:
            logger.info(
                f"Command completed successfully with return code: {return_code}"
            )
        else:
            logger.error(f"Command failed with return code: {return_code}")

        return return_code

    except Exception as e:
        logger.error(f"Error executing command: {str(e)}")
        return -1


if __name__ == "__main__":
    run_command()
