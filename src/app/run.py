from dataclasses import dataclass
from datetime import datetime, timezone
from logging import Logger

from app.constants import (
    LOG_EXECUTION_COMPLETED,
    LOG_EXECUTION_FAILED,
    LOG_EXECUTION_STARTED,
)

def execute(
    logger: Logger,
    parameters,
) -> None:
    try:
        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)


        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as e:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(e)
        raise e
