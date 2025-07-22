from datetime import datetime, timezone
from logging import Logger
from typing import Iterable
from urllib.parse import parse_qsl, urlparse

import pymssql
from pyspark.sql import functions as F, DataFrame, Row
from pyspark.sql.types import StructType

from pyeqx.core import Configuration, Operation
from pyeqx.core.models import AppData
from pyeqx.core.models.module.properties import DatabaseDataModuleProperties
from pyeqx.core.models.storage.properties import (
    MSSqlDataProperties,
)
from pyeqx.execution import ProcessBase

from app.parameters import ProcessExecuteParameters, ProcessParameters


class Process(ProcessBase):
    _is_log_load_data: bool

    _execution_timestamp: datetime

    _datetime_format = "%Y-%m-%d %H:%M:%S"

    def __init__(self, config: Configuration, logger: Logger, operation: Operation):
        super().__init__(config, logger, operation)

    def _do_execute(self, parameters: ProcessExecuteParameters, *args, **kwargs):
        self._execution_timestamp = datetime.now()

        timestamp_str = self._execution_timestamp.isoformat()

        self.logger.info(f"start process at {timestamp_str}")

        # initialize read datas
        self._read_datas(parameters=parameters)

        # process datas
        self._process_datas(parameters=parameters)

    def configure(
        self,
        parameters: ProcessParameters,
    ):
        self._is_log_load_data = parameters.is_log_load_data

    def _read_datas(self, parameters: ProcessExecuteParameters):
        """
        Read all datas from both source and destination.

        Args:
            parameters (ProcessExecuteParameters): Parameters to execute process.
        """
        pass

    def _process_datas(self, parameters: ProcessExecuteParameters):
        """
        Process all datas from both source and destination.

        Args:
            parameters (ProcessExecuteParameters): Parameters to execute process.
        """
        pass

    def _read_source(self, name: str, schema: StructType = None, options: dict = {}):
        """
        Read from source data.

        Args:
            name (str): name of module in configuration to find table and its database.
            schema (StructType, optional): schema for table. Defaults to None - for dynamic schema.

        Returns:
            DataFrame: DataFrame of source data.
        """
        data_config = self._get_data_config(name=name, cls=AppData)

        return self.operation.read_source(
            data_module=data_config.src,
            schema=schema,
            options=options,
        )

    def _read_destination(self, name: str, schema: StructType = None):
        """
        Read from destination data.

        Args:
            name (str): name of module in configuration to find table and its database.
            schema (StructType, optional): schema for table. Defaults to None - for dynamic schema.

        Returns:
            DataFrame: DataFrame of destination data.
        """
        data_config = self._get_data_config(name=name, cls=AppData)

        return self.operation.read_source(
            data_module=data_config.dest,
            schema=schema,
        )

    def _write_destination_to_mssql(self, name: str, data: DataFrame, batch_size: int):
        """
        Write to destination (Azure SQL).

        Args:
            name (str): name of module in configuration to find table and its database.
            data (DataFrame): DataFrame of data to write.
        """
        data_config = self._get_data_config(name=name, cls=AppData)
        props = data_config.dest.get_properties(DatabaseDataModuleProperties)
        storage_props = self.operation.get_storage_properties(
            name=props.storage, cls=MSSqlDataProperties
        )

        self.logger.debug(
            f"write data to destination (Azure SQL), table: {props.table}."
        )

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        spark_options = {
            "url": storage_props.url,
            "dbtable": props.table,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "batchsize": batch_size,
            "schemaCheckEnabled": False,
            "tableLock": False,
            "isolationLevel": "READ_UNCOMMITTED",
            "numPartitions": (
                (
                    self.config.engine.spark_executor_max_instances
                    * self.config.engine.spark_executor_core
                )
                * 3
            ),
        }

        self.operation.get_writer().write_to_sql(
            data=data,
            mode="append",
            format="jdbc",
            options=spark_options,
        )

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        self.logger.debug(
            f"write data to destination (Azure SQL) completed in {end_timestamp - start_timestamp} ms."
        )

    def _load_to(
        self,
        name: str,
        table: str,
        keys: list[str],
        data: DataFrame,
        is_ignore_conflict: bool = False,
        is_rollback_on_error: bool = True,
        is_upsert: bool = True,
        is_check_duplicate: bool = False,
        is_repartition: bool = False,
        batch_size: int = 1000000,
    ) -> int:
        if self._is_log_load_data:
            self._show_data(message=table, data=data, n=20)

        if is_upsert:
            self._upsert_to_mssql(
                name=name,
                keys=keys,
                data=data,
                is_ignore_conflict=is_ignore_conflict,
                is_rollback_on_error=is_rollback_on_error,
            )

            return data.count()
        else:
            return self.__load_to_mssql(
                name=name,
                table=table,
                keys=keys,
                data=data,
                is_check_duplicate=is_check_duplicate,
                is_repartition=is_repartition,
                batch_size=batch_size,
            )

    def _show_data(self, message: str, data: DataFrame, n: int = 50):
        print(message)
        data.show(n=n, truncate=False)

    def _upsert_to_mssql(
        self,
        name: str,
        keys: list[str],
        data: DataFrame,
        page_size: int = 10000,
        is_rollback_on_error: bool = True,
        is_ignore_conflict: bool = False,
    ):
        """
        Upsert data to destination (Azure SQL).

        Args:
            name (str): name of module in configuration to find table and its database.
            keys (list[str]): list of keys. Use in 'ON CONFLICT' clause.
            data (DataFrame): DataFrame of data to upsert.
            page_size (int, optional): page size to upsert. Defaults to 10000.
            is_rollback_on_error (Optional[bool], optional): is rollback on error. Defaults to None - use default value from configuration.
        """
        data_config = self._get_data_config(name=name, cls=AppData)
        data_props = data_config.dest.get_properties(DatabaseDataModuleProperties)
        storage_props = self.operation.get_storage_properties(
            name=data_props.storage, cls=MSSqlDataProperties
        )

        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        self.__upsert_to_mssql(
            table=data_props.table,
            keys=keys,
            data=data,
            storage_props=storage_props,
            page_size=page_size,
            is_rollback_on_error=is_rollback_on_error,
            is_ignore_conflict=is_ignore_conflict,
        )

        end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

        self.logger.debug(
            f"upsert data to destination (Azure SQL) completed in {end_timestamp - start_timestamp} ms."
        )

    def __upsert_to_mssql(
        self,
        table: str,
        keys: list[str],
        data: DataFrame,
        storage_props: MSSqlDataProperties,
        page_size: int = 10000,
        is_rollback_on_error: bool = True,
        is_ignore_conflict: bool = False,
    ):
        """
        Upsert data to Azure SQL

        Args:
            table (str): Table name
            keys (list[str]): List of keys. Use in 'ON CONFLICT' clause.
            data (DataFrame): Dataframe to upsert
            storage_props (MSSqlDataProperties): Storage properties
            page_size (int, optional): Page size. Defaults to 10000.
            is_rollback_on_error (bool, optional): Rollback on error. Defaults to True.
        """

        try:
            self.logger.debug(f"upsert data into Azure SQL, table: {table}.")

            parsed_url = urlparse(storage_props.url)
            parsed_path = urlparse(parsed_url.path)

            splits = parsed_path.netloc.split(";")
            hostname = parsed_path.hostname
            port = splits[0].split(":")[1]

            columns = data.columns

            source_values = ", ".join(["%s"] * len(columns))
            update_clauses = ", ".join([f"{col} = source.{col}" for col in columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            on_clauses = " AND ".join([f"target.{key} = source.{key}" for key in keys])

            query = f"""
                MERGE INTO {table} AS target
                USING (VALUES ({source_values})) AS source ({', '.join(columns)})
                ON {on_clauses}
                { "" if is_ignore_conflict else f"WHEN MATCHED THEN UPDATE SET {update_clauses}" }
                WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values});
            """

            # partition_counts = int(data.count() / page_size) + 1
            # repartitioned_data = data.repartition(partition_counts)

            default_partition_count = self.config.engine.spark_executor_max_instances
            partition_counts = min(
                max(int(data.count() / page_size) + 1, default_partition_count), 500
            )

            repartitioned_data = data.repartition(partition_counts)

            def execute_upsert(idx: int, partition: Iterable[Row]):
                import logging
                import sys

                log_formatter = logging.Formatter(
                    "%(asctime)s %(levelname)s Operation: %(message)s"
                )

                log_handler = logging.StreamHandler(sys.stdout)
                log_handler.setLevel(logging.DEBUG)
                log_handler.setFormatter(log_formatter)

                logger = logging.getLogger(__name__)
                logger.addHandler(log_handler)
                logger.setLevel(logging.DEBUG)

                try:
                    parameters = dict(parse_qsl(parsed_path.netloc, separator=";"))

                    connection = pymssql.connect(
                        server=f"{hostname}:{port}",
                        user=parameters.get("user"),
                        password=parameters.get("password"),
                        database=parameters.get("database"),
                    )

                    with connection.cursor() as cursor:
                        data_to_upsert = [
                            tuple(row[key] for key in columns) for row in partition
                        ]

                        cursor.executemany(
                            query,
                            data_to_upsert,
                        )
                        connection.commit()

                    connection.close()

                    return [
                        f"upsert into Azure SQL, table: {table}, partition: {idx} completed."
                    ]
                except Exception as inner_e:
                    if is_rollback_on_error and connection:
                        connection.rollback()

                    if connection:
                        connection.close()

                    message = (
                        f"error upsert into Azure SQL, table: {table}, partition: {idx}. {'(rollback)' if is_rollback_on_error else ''}",
                        inner_e,
                    )

                    logger.error(
                        message,
                        exc_info=True,
                    )

                    return [message]

            log_results = repartitioned_data.rdd.mapPartitionsWithIndex(
                execute_upsert
            ).collect()

            (self.logger.debug(log_result) for log_result in log_results)
        except Exception as e:
            self.logger.error(e, exc_info=True)

    def __load_to_mssql(
        self,
        name: str,
        table: str,
        keys: list[str],
        data: DataFrame,
        is_check_duplicate: bool = False,
        is_repartition: bool = False,
        batch_size: int = 20000,
    ) -> int:
        self.logger.debug("load data to Azure SQL: started.")

        data_to_write = data

        if is_check_duplicate:
            self.logger.info("check duplication is enabled.")

            data_to_write = self._check_duplication(
                name=name, table=table, keys=keys, data=data_to_write
            )

        count = data_to_write.count()

        # using number of max instances for repartition datas
        if is_repartition:
            num_partitions = (
                self.config.engine.spark_executor_max_instances
                * self.config.engine.spark_executor_core
            ) * 5
            if count > 0 and batch_size > 0:
                num_partitions = -(-count // batch_size)
            data_to_write = data_to_write.repartition(num_partitions)

        self._write_destination_to_mssql(
            name=name, data=data_to_write, batch_size=batch_size
        )

        self.logger.debug("load data to Azure SQL: completed.")

        return count

    def _check_duplication(
        self, name: str, table: str, keys: list[str], data: DataFrame
    ) -> DataFrame:
        self.logger.info(f"check duplication on table: {table}")

        dest_data = self._read_destination(
            name=name,
        )

        join_conditions = [F.col(f"left.{k}") == F.col(f"right.{k}") for k in keys]
        return (
            data.alias("left")
            .join(other=dest_data.alias("right"), on=join_conditions, how="anti")
            .select(["left.*"])
        )

    def _count_exists(
        self,
        name: str,
        table: str,
        keys: list[str],
        data: DataFrame,
        process_id: str,
        count_name: str,
    ):
        dest_data = self._read_destination(
            name=name,
        )

        summary_rows = (
            self._read_destination(name="processSummary")
            .filter(
                (F.col("process_id") == process_id)
                & (F.col("effective_date") < self._execution_timestamp)
            )
            .collect()
        )

        sum_count = 0

        for summary_row in summary_rows:
            sum_count += int(summary_row[count_name])

        join_conditions = [F.col(f"left.{k}") == F.col(f"right.{k}") for k in keys]

        exists_count = (
            data.alias("left").join(
                other=dest_data.alias("right"), on=join_conditions, how="inner"
            )
        ).count()

        return exists_count - sum_count

    def _update_to_sql(
        self,
        table: str,
        filters: dict,
        updates: dict,
        storage_props: MSSqlDataProperties,
    ):
        try:
            parsed_url = urlparse(storage_props.url)
            parsed_path = urlparse(parsed_url.path)

            splits = parsed_path.netloc.split(";")
            hostname = parsed_path.hostname
            port = splits[0].split(":")[1]

            set_clause = ", ".join([f"{key} = '{updates[key]}'" for key in updates])
            where_clause = " AND ".join(
                [f"{key} = '{filters[key]}'" for key in filters]
            )

            query = f"""
                UPDATE {table}
                SET {set_clause}
                WHERE {where_clause}
            """

            parameters = dict(parse_qsl(parsed_path.netloc, separator=";"))

            connection = pymssql.connect(
                server=f"{hostname}:{port}",
                user=parameters.get("user"),
                password=parameters.get("password"),
                database=parameters.get("database"),
            )

            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()

            connection.close()
        except Exception as e:
            if connection is not None:
                connection.rollback()
                connection.close()

            message = (
                f"error update to Azure SQL, table: {table}. (rollback)",
                e,
            )

            self.logger.error(message, exc_info=True)
