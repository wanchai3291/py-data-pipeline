from dataclasses import dataclass
from datetime import datetime, timedelta
from logging import Logger

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    IntegerType,
)
from pyeqx.core import Configuration, Operation

from app.execution.process import Process
from app.parameters import ETLProcessExecuteParameters


@dataclass
class Sources:
    summary_record: DataFrame


class DailySummaryProcess(Process):
    def __init__(self, config: Configuration, logger: Logger, operation: Operation):
        super().__init__(config=config, logger=logger, operation=operation)

    def _read_datas(self, parameters: ETLProcessExecuteParameters):
        summary_record_data = self._read_source(name="processSummary")

        self._sources = Sources(summary_record=summary_record_data)

    def _process_datas(self, parameters: ETLProcessExecuteParameters):
        start_at_raw = ""

        if parameters.is_filter_by_datetime:
            start_at_raw = str(parameters.filter_by_datetime_start).rstrip("Z")
        else:
            start_at_dt = self._execution_timestamp - timedelta(days=1)
            start_at_raw = start_at_dt.isoformat()

        start_at_raw_dt = datetime.fromisoformat(start_at_raw).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_at_dt = start_at_raw_dt + timedelta(days=1)

        filtered_data = self._sources.summary_record.filter(
            (F.col("effective_date") >= start_at_raw_dt.isoformat())
            & (F.col("effective_date") < end_at_dt.isoformat())
        ).cache()

        product_count_data = self.__extract_product_count_data(data=filtered_data)

        transformed_filtered_data = filtered_data.withColumn(
            "start_at", F.date_format("start_at", "yyyy-MM-dd")
        )

        round_count_data = transformed_filtered_data.groupBy("start_at").count()

        sum_by_day_data = (
            transformed_filtered_data.groupBy("start_at")
            .agg(
                F.sum("cma_count").cast(IntegerType()).alias("cma_count"),
                F.sum("cs_count").cast(IntegerType()).alias("cs_count"),
                F.sum("event_data_count").cast(IntegerType()).alias("event_data_count"),
                F.sum("payment_activity_count")
                .cast(IntegerType())
                .alias("payment_activity_count"),
                F.sum("loyalty_product_count")
                .cast(IntegerType())
                .alias("loyalty_product_count"),
                F.sum("payment_event_count")
                .cast(IntegerType())
                .alias("payment_event_count"),
            )
            .withColumn("execution_date", F.lit(self._execution_timestamp.isoformat()))
        )

        joined_data = (
            sum_by_day_data.alias("data")
            .join(
                other=round_count_data.alias("round_count_data"),
                on=F.col("data.start_at") == F.col("round_count_data.start_at"),
                how="inner",
            )
            .join(
                other=product_count_data.alias("product_count_data"),
                on=F.col("data.start_at") == F.col("product_count_data.date"),
                how="inner",
            )
            .select(
                [
                    F.col("data.start_at").alias("date"),
                    F.col("execution_date"),
                    F.col("count").cast(IntegerType()).alias("round_count"),
                    F.col("cma_count"),
                    F.col("cs_count"),
                    F.col("product_count"),
                    F.col("event_data_count"),
                    F.col("payment_activity_count"),
                    F.col("loyalty_product_count"),
                    F.col("payment_event_count"),
                ]
            )
            .distinct()
        )

        self._write_destination_to_mssql(
            name="processDailySummary",
            data=joined_data,
            batch_size=32,
        )

    def __extract_product_count_data(self, data: DataFrame):
        latest_start_at_data = data.groupBy(
            F.date_format("start_at", "yyyy-MM-dd").alias("date")
        ).agg(
            F.max("effective_date").alias("latest_effective_date"),
            F.max("start_at").alias("latest_start_at"),
        )

        return (
            data.alias("left")
            .join(
                other=latest_start_at_data.alias("right"),
                on=(F.date_format("left.start_at", "yyyy-MM-dd") == F.col("right.date"))
                & (F.col("left.effective_date") == F.col("right.latest_effective_date"))
                & (F.col("left.start_at") == F.col("right.latest_start_at")),
                how="inner",
            )
            .distinct()
            .withColumn(
                "formatted_effective_date",
                F.date_format("left.effective_date", "yyyy-MM-dd"),
            )
            .select(
                [
                    F.col("date"),
                    F.col("formatted_effective_date").alias("effective_date"),
                    F.col("left.product_count"),
                ]
            )
        )
