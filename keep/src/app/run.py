from dataclasses import dataclass
from datetime import datetime, timezone
from logging import Logger

from pyeqx.core import Configuration, Operation

from app.constants import (
    LOG_EXECUTION_COMPLETED,
    LOG_EXECUTION_FAILED,
    LOG_EXECUTION_STARTED,
)
from app.execution import (
    DailySummaryProcess,
    ETLProcess,
    MigrationProcess,
    RecoveryProcess,
)
from app.parameters import (
    ProcessExecuteParameters,
    ProcessParameters,
)


@dataclass
class ExecuteParameters:
    execution: ProcessExecuteParameters
    process: ProcessParameters


def execute(
    configuration: Configuration,
    logger: Logger,
    operation: Operation,
    parameters: ExecuteParameters,
) -> None:
    try:
        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = ETLProcess(config=configuration, logger=logger, operation=operation)

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(parameters=parameters.process)
        process.execute(parameters=parameters.execution)

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as e:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(e)
        raise e


def execute_migration(
    configuration: Configuration,
    logger: Logger,
    operation: Operation,
    parameters: ExecuteParameters,
) -> None:
    try:
        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = MigrationProcess(
            config=configuration, logger=logger, operation=operation
        )

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(parameters=parameters.process)
        process.execute(parameters=parameters.execution)

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as e:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(e)
        raise e


def execute_recovery(
    configuration: Configuration,
    logger: Logger,
    operation: Operation,
    parameters: ExecuteParameters,
) -> None:
    try:
        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = RecoveryProcess(
            config=configuration, logger=logger, operation=operation
        )

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(parameters=parameters.process)
        process.execute(parameters=parameters.execution)

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as e:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(e)
        raise e


def execute_daily_summary(
    configuration: Configuration,
    logger: Logger,
    operation: Operation,
    parameters: ExecuteParameters,
) -> None:
    try:
        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        process = DailySummaryProcess(
            config=configuration, logger=logger, operation=operation
        )

        logger.info(LOG_EXECUTION_STARTED)
        process.configure(parameters=parameters.process)
        process.execute(parameters=parameters.execution)

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        logger.info(
            f"{LOG_EXECUTION_COMPLETED} (elapsed: {end_timestamp - start_timestamp} ms)"
        )
    except Exception as e:
        logger.info(LOG_EXECUTION_FAILED)
        logger.error(e)
        raise e
