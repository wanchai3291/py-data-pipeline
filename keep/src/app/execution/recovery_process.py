from datetime import datetime, timedelta
from logging import Logger

from pyspark.sql import functions as F

from pyeqx.core import Configuration, Operation

from app.execution import ETLProcess
from app.parameters import ETLProcessExecuteParameters


class RecoveryProcess(ETLProcess):
    def __init__(self, config: Configuration, logger: Logger, operation: Operation):
        super().__init__(config=config, logger=logger, operation=operation)

    def _process_datas(self, parameters: ETLProcessExecuteParameters):
        process_data = (
            self._read_destination(name="process")
            .filter(
                (F.col("is_processed") == False)
                & ((F.col("retries") == -1) | (F.col("retries") == 3))
            )
            .sort(F.col("start_at"))
        )

        process_rows = process_data.collect()

        for process_row in process_rows:
            process_id = str(process_row["id"]).upper()

            self.logger.info(f"execute recovery process. (id: {process_id})")

            start_at_dt = datetime.fromisoformat(process_row["start_at"])
            end_at_dt = datetime.fromisoformat(process_row["end_at"])

            while start_at_dt < end_at_dt:
                self.logger.info(
                    f"execute process (normal): start: {start_at_dt}, end: {end_at_dt}"
                )

                self._do_process(
                    start_at=start_at_dt,
                    end_at=end_at_dt,
                    process_id=process_id,
                    is_check_duplicate=True,
                    parameters=parameters,
                )

                start_at_dt += timedelta(days=1)
