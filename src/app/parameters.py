from dataclasses import dataclass
from pyeqx.execution.parameter import ExecuteParameters


@dataclass
class ProcessParameters:
    def __init__(
        self,
        is_log_load_data=False,
    ):
        self.is_log_load_data = is_log_load_data


class ProcessExecuteParameters(ExecuteParameters):
    def __init__(
        self,
        name: str,
    ):
        self.name = name


class ETLProcessExecuteParameters(ProcessExecuteParameters):
    def __init__(
        self,
        name: str,
        is_filter_by_datetime=False,
        filter_by_datetime_start=None,
        filter_by_datetime_end=None,
        is_process_queue_enabled=False,
        is_upsert=True,
        is_migration=False,
        is_migration_to_json=False,
        is_migration_to_delta=False,
        is_retry=False,
    ):
        super().__init__(name=name)

        self.is_filter_by_datetime = is_filter_by_datetime
        self.filter_by_datetime_start = filter_by_datetime_start
        self.filter_by_datetime_end = filter_by_datetime_end
        self.is_process_queue_enabled = is_process_queue_enabled
        self.is_upsert = is_upsert
        self.is_migration = is_migration
        self.is_migration_to_json = is_migration_to_json
        self.is_migration_to_delta = is_migration_to_delta
        self.is_retry = is_retry


class EventDataProcessExecuteParameters(ETLProcessExecuteParameters):
    def __init__(
        self,
        name: str,
        is_filter_by_datetime=False,
        filter_by_datetime_start=None,
        filter_by_datetime_end=None,
        is_process_queue_enabled=False,
        is_upsert=True,
    ):
        super().__init__(
            name=name,
            is_filter_by_datetime=is_filter_by_datetime,
            filter_by_datetime_start=filter_by_datetime_start,
            filter_by_datetime_end=filter_by_datetime_end,
            is_process_queue_enabled=is_process_queue_enabled,
            is_upsert=is_upsert,
        )
