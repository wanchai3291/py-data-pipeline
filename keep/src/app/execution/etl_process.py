from ctypes import CDLL, create_string_buffer
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import Logger
import math
import os
import re
import ssl
from typing import Optional
import uuid
from urllib.parse import ParseResult, urlparse

from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob import BlobServiceClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
)
from cassandra.query import SimpleStatement, ConsistencyLevel
from delta import DeltaTable
import pandas as pd
from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import StringType, StructType, LongType
from pyspark.sql.functions import col

from pyeqx.core import Configuration, Operation
from pyeqx.core.enums import DataModuleType
from pyeqx.core.models import AppData
from pyeqx.core.models.module import DataModule
from pyeqx.core.models.module.properties import (
    FileDataModuleProperties,
    DatabaseDataModuleProperties,
)
from pyeqx.core.models.storage.properties import (
    AzureAdlsGen2StorageDataProperties,
    CassandraDataProperties,
    MSSqlDataProperties,
)
from pyeqx.core.spark import create_dataframe

from azure.core.credentials import AzureSasCredential, AzureNamedKeyCredential
from azure.storage.blob import BlobServiceClient

from app.constants import (
    CUSTOMER_MOBILE_ACTIVITY_SRC_SELECT_COLUMNS,
    CUSTOMER_MOBILE_ACTIVITY_DEST_SELECT_COLUMNS,
    EVENT_DATA_DEST_SELECT_COLUMNS,
    EVENT_DATA_SELECT_COLUMNS,
    PAYMENT_ACTIVITY_SELECT_COLUMNS,
    PAYMENT_EVENT_SELECT_COLUMNS,
    PRODUCT_SELECT_COLUMNS,
)
from app.execution.process import Process
from app.parameters import (
    ETLProcessExecuteParameters,
)
from app.schemas import (
    CUSTOMER_MOBILE_ACTIVITY_SCHEMA,
    CACHE_APP_SESSION_SCHEMA,
    CATEGORY_SRC_SCHEMA,
    LOYALTY_PROGRAM_PRODUCT_SPEC_SCHEMA,
    PROCESS_SCHEMA,
    SUMMARY_RECORD_SCHEMA,
    PRODUCT_SRC_SCHEMA,
    PRODUCT_OFFERING_PRICE_SRC_SCHEMA,
    PRODUCT_OFFERING_SRC_SCHEMA,
    PRODUCT_SPECIFICATION_SRC_SCHEMA,
    COMMUNICATION_MESSAGE_SRC_SCHEMA,
)


class CustomerMobileActivityObjectType:
    PRODUCT_OFFERING = "ProductOffering"
    PACKAGE_ORDER = "PackageOrder"
    LOYALTY_PROGRAM_PRODUCT_SPEC = "LoyaltyProgramProductSpec"


@dataclass
class Sources:
    category: DataFrame
    product: DataFrame
    product_offering: DataFrame
    product_offering_price: DataFrame
    product_specification: DataFrame
    loyalty_program_product_spec: DataFrame
    communication_message: DataFrame


class ETLProcess(Process):
    __interval_seconds: int = 3600

    def __init__(self, config: Configuration, logger: Logger, operation: Operation):
        super().__init__(config=config, logger=logger, operation=operation)

    def _read_datas(self, parameters: ETLProcessExecuteParameters):
        category_data = self._read_source(
            name="category",
            schema=CATEGORY_SRC_SCHEMA,
        )

        product_data = self._read_source(
            name="product",
            schema=PRODUCT_SRC_SCHEMA,
        )
        product_offering_data = self._read_source(
            name="productOffering",
            schema=PRODUCT_OFFERING_SRC_SCHEMA,
        )
        product_offering_price_data = self._read_source(
            name="productOfferingPrice",
            schema=PRODUCT_OFFERING_PRICE_SRC_SCHEMA,
        )
        product_specification_data = self._read_source(
            name="productSpecification",
            schema=PRODUCT_SPECIFICATION_SRC_SCHEMA,
        )

        loyalty_program_product_spec_data = self._read_source(
            name="loyaltyProduct",
            schema=LOYALTY_PROGRAM_PRODUCT_SPEC_SCHEMA,
        )
        communication_message_data = self._read_source(
            name="communicationMessage",
            schema=COMMUNICATION_MESSAGE_SRC_SCHEMA,
        )
        self._sources = Sources(
            category=category_data,
            product=product_data,
            product_offering=product_offering_data,
            product_offering_price=product_offering_price_data,
            product_specification=product_specification_data,
            loyalty_program_product_spec=loyalty_program_product_spec_data,
            communication_message=communication_message_data,
        )

    def _process_datas(self, parameters: ETLProcessExecuteParameters):
        if parameters.is_process_queue_enabled:
            process_data = (
                self._read_destination(name="process")
                .filter(
                    (
                        (F.col("is_processed") == False)
                        & ((F.col("retries") > -1) & (F.col("retries") < 3))
                    )
                )
                .sort(F.col("effective_date"))
            )

            if process_data.isEmpty():
                self.logger.info("no processes found (is_processed = False)")
            else:
                self._do_recovery_process(data=process_data, parameters=parameters)
                return

        start_at_raw = ""
        end_at_raw = ""

        if parameters.is_filter_by_datetime:
            start_at_raw = str(parameters.filter_by_datetime_start).rstrip("Z")
            end_at_raw = str(parameters.filter_by_datetime_end).rstrip("Z")
        else:
            interval_seconds = timedelta(seconds=self.__interval_seconds)

            start_at_dt = self._execution_timestamp - (interval_seconds * 2)
            start_at_raw = start_at_dt.isoformat()

            end_at_dt = self._execution_timestamp - interval_seconds
            end_at_raw = end_at_dt.isoformat()

            if parameters.is_process_queue_enabled:
                process_row = (
                    self._read_destination(name="process")
                    .sort(F.col("effective_date").desc())
                    .first()
                )

                if process_row:
                    start_at_raw = process_row["end_at"]
                    start_at_dt = datetime.fromisoformat(start_at_raw)
                    end_at = start_at_dt + interval_seconds
                    end_at_raw = end_at.isoformat()

        # reset time minutes, seconds, microseconds to 0
        start_at_raw_dt = datetime.fromisoformat(start_at_raw).replace(
            minute=0, second=0, microsecond=0
        )
        end_at_raw_dt = datetime.fromisoformat(end_at_raw).replace(
            minute=0, second=0, microsecond=0
        )

        # check if start_at hour is same as current hour skip
        if (
            start_at_raw_dt.hour == self._execution_timestamp.hour
            and start_at_raw_dt.day == self._execution_timestamp.day
        ):
            self.logger.info("both hour and day is the same, skip executing process.")
            return

        hour_of_execution = self._execution_timestamp.replace(
            minute=0, second=0, microsecond=0
        )

        while start_at_raw_dt < end_at_raw_dt and start_at_raw_dt < hour_of_execution:
            self.logger.info(
                f"execute process (normal): start: {start_at_raw_dt}, end: {end_at_raw_dt}"
            )

            self._do_process(
                parameters=parameters,
                start_at=start_at_raw_dt,
                end_at=end_at_raw_dt,
            )
            start_at_raw_dt += timedelta(hours=1)

    def _do_recovery_process(
        self, data: DataFrame, parameters: ETLProcessExecuteParameters
    ):
        process_rows = data.collect()

        for process_row in process_rows:
            process_id = str(process_row["id"]).upper()
            process_retries = process_row["retries"]

            if process_retries is None:
                process_retries = 0

            self.logger.info(
                f"execute from process queue. (id: {process_id}, retry: {process_retries + 1})"
            )

            self.__increment_process_retry(process_id=process_id, retry=process_retries)

            self._do_process(
                start_at=datetime.fromisoformat(process_row["start_at"]),
                end_at=datetime.fromisoformat(process_row["end_at"]),
                process_id=process_id,
                is_check_duplicate=True,
                parameters=parameters,
            )

    def _do_process(
        self,
        parameters: ETLProcessExecuteParameters,
        start_at: datetime,
        end_at: datetime,
        is_check_duplicate: bool = False,
        process_id: Optional[str] = None,
    ):

        filter_log = "filtering customer_mobile_activity data: start_at: {start_at}, end_at: {end_at}".format(
            start_at=start_at.isoformat(), end_at=end_at.isoformat()
        )
        self.logger.info(filter_log)

        filtered_cma_data = self.__read_customer_mobile_activity(start_at=start_at)

        existing_columns = set(filtered_cma_data.columns)
        missing_columns = (
            set(CUSTOMER_MOBILE_ACTIVITY_SRC_SELECT_COLUMNS) - existing_columns
        )

        for col in missing_columns:
            filtered_cma_data = filtered_cma_data.withColumn(
                col, F.lit(None).cast(StringType())
            )

        filtered_cma_data = filtered_cma_data.select(
            CUSTOMER_MOBILE_ACTIVITY_SRC_SELECT_COLUMNS
        )
        try:
            transformed_cma_data = (
                self._transform_customer_mobile_activity(data=filtered_cma_data)
            ).cache()

            transformed_payment_activity_data = self._transform_payment_activity(
                cma_data=transformed_cma_data
            )

            transformed_cs_data = self._transform_customer_asset(
                customer_mobile_activity_data=transformed_cma_data
            ).cache()

            selected_cma_data = transformed_cma_data.select(
                CUSTOMER_MOBILE_ACTIVITY_DEST_SELECT_COLUMNS
            )

            product_data = self._process_product(parameters=parameters).cache()

            selected_product_data = product_data.select(PRODUCT_SELECT_COLUMNS)

            loyalty_product_data = self._process_loyalty_product_data(
                cma_data=selected_cma_data, parameters=parameters
            ).cache()

            event_data_data = self._process_event_data(
                cma_data=selected_cma_data,
                cs_data=transformed_cs_data,
                product_data=product_data,
                loyalty_product_data=loyalty_product_data,
            ).cache()

            payment_event_data = self._process_payment_event(
                cs_data=transformed_cs_data,
                payment_activity_data=transformed_payment_activity_data,
            ).cache()

            if parameters.is_process_queue_enabled and process_id is None:
                process_id = str(uuid.uuid4()).upper()

                self.__create_process(
                    process_id=process_id,
                    start_at=start_at,
                    end_at=end_at,
                    cma_count=transformed_cma_data.count(),
                )

            self.__load_datas(
                cma_data=selected_cma_data,
                cs_data=transformed_cs_data,
                payment_activity_data=transformed_payment_activity_data,
                product_data=selected_product_data,
                event_data_data=event_data_data,
                loyalty_product_data=loyalty_product_data,
                payment_event_data=payment_event_data,
                is_check_duplicate=is_check_duplicate,
                parameters=parameters,
                start_at=start_at,
                end_at=end_at,
                process_id=process_id,
            )

            if parameters.is_process_queue_enabled:
                self.__update_is_processed(
                    process_id=process_id,
                )
        except Exception:
            raise
        finally:
            # cleanup teporary data
            self.__cleanup_temporary_data()

    def _process_product(self, parameters: ETLProcessExecuteParameters):
        selected_product_offering_data = self._sources.product_offering.select(
            F.col("id").alias("product_id"),
            F.col("name").alias("product_name"),
            F.col("productOfferingPrice"),
            F.col("productSpecification"),
            F.col("category"),
        )

        transformed_product_offering_data = selected_product_offering_data.withColumn(
            "productOfferingPrice",
            F.explode(
                F.when(
                    (F.col("productOfferingPrice").isNull())
                    | (F.size(F.col("productOfferingPrice")) == 0),
                    F.array(F.lit(None)),
                ).otherwise(F.col("productOfferingPrice")),
            ),
        ).drop_duplicates(["product_id"])

        joined_price_data = (
            transformed_product_offering_data.alias("product_offering")
            .join(
                other=self._sources.product_offering_price.alias(
                    "product_offering_price"
                ),
                on=F.col("product_offering.productOfferingPrice.id")
                == F.col("product_offering_price.id"),
                how="left",
            )
            .withColumn(
                "product_item_vat_price",
                F.when(
                    (F.col("product_offering_price.tax").isNull())
                    | (F.size(F.col("product_offering_price.tax")) == 0),
                    self.__set_null_value(),
                ).otherwise(
                    F.col("product_offering_price.tax")
                    .getItem(0)
                    .getItem("taxAmount")
                    .getItem("value")
                ),
            )
            .select(
                [
                    "product_offering.*",
                    "product_offering_price.price",
                    "product_item_vat_price",
                    "product_offering_price.recurringChargePeriodLength",
                    "product_offering_price.recurringChargePeriodType",
                ]
            )
        )
        joined_category_data = (
            joined_price_data.alias("product")
            .join(
                other=self._sources.category.alias("category"),
                on=F.col("product.category").getItem(0).getItem("id")
                == F.col("category.id"),
                how="left",
            )
            .withColumn("product_category", F.col("category.name"))
            .select(
                [
                    "product.*",
                    "product_category",
                ]
            )
        )
        joined_data: DataFrame = (
            joined_category_data.alias("product")
            .join(
                other=self._sources.product_specification.alias(
                    "product_specification"
                ),
                on=F.col("product.productSpecification.id")
                == F.col("product_specification.id"),
                how="left",
            )
            .select(
                [
                    "product.*",
                    F.col("product_specification.TORO_productType").alias(
                        "product_type"
                    ),
                    F.col("product_specification.productNumber").alias(
                        "product_number"
                    ),
                    F.col("product_specification.brand").alias("product_brand"),
                    F.col("product_specification.productSpecCharacteristic"),
                ]
            )
        )
        transformed_data = (
            joined_data.withColumn(
                "product_item_vat_unit_price",
                self.__set_null_value(),
            )
            .withColumn(
                "product_ratingtype_items",
                self.__explode_array_or_object_by(
                    column="productSpecCharacteristic",
                    key_name="name",
                    key_value="ratingType",
                    value_name="productSpecCharacteristicValue",
                    default_value=F.array(
                        F.array(
                            F.struct(
                                F.lit(None).alias("value"),
                                F.lit(None).alias("valueType"),
                                F.lit(None).alias("unitOfMeasure"),
                            )
                        )
                    ),
                    is_nullable=True,
                ),
            )
            .withColumn(
                "product_ratingtype_item", F.col("product_ratingtype_items").getItem(0)
            )
            .withColumn("product_ratingtype", F.col("product_ratingtype_item.value"))
            .withColumn(
                "product_charge_period_time",
                F.when(
                    (
                        F.col("recurringChargePeriodLength").isNull()
                        | (F.col("recurringChargePeriodLength") == "")
                    )
                    & (
                        F.col("recurringChargePeriodType").isNull()
                        | (F.col("recurringChargePeriodType") == "")
                    ),
                    F.lit(None),
                ).otherwise(
                    F.concat_ws(
                        " ",
                        F.col("recurringChargePeriodLength"),
                        F.col("recurringChargePeriodType"),
                    )
                ),
            )
        )

        transformed_data = self.__transform_product_specification_by_type(
            data=transformed_data,
            target_key="product_internet_specification",
            key="internet",
        )
        transformed_data = self.__transform_product_specification_by_type(
            data=transformed_data,
            target_key="product_voice_specification",
            key="voice",
        )
        transformed_data = self.__transform_product_specification_by_type(
            data=transformed_data,
            target_key="product_wifi_specification",
            key="wifi",
        )

        return transformed_data.select(
            [
                F.col("product_id"),
                F.col("product_name"),
                F.col("price.value").alias("product_price"),
                F.col("price.unit").alias("product_price_unit"),
                F.col("product_category"),
                F.col("product_type"),
                F.col("product_item_vat_price"),
                F.col("product_item_vat_unit_price"),
                F.col("product_number"),
                F.col("product_brand"),
                F.col("product_ratingtype"),
                F.col("product_internet_specification"),
                F.col("product_voice_specification"),
                F.col("product_wifi_specification"),
                F.col("product_charge_period_time"),
            ]
        )

    def _process_event_data(
        self,
        cma_data: DataFrame,
        cs_data: DataFrame,
        product_data: DataFrame,
        loyalty_product_data: DataFrame,
    ):
        def join_and_transform_cs_data(
            key: str,
            left_data: DataFrame,
            right_data: DataFrame,
            right_alias: str,
        ):
            return (
                left_data.join(
                    other=right_data.alias(right_alias),
                    on=F.col(f"cma.{key}") == F.col(f"{right_alias}.number"),
                    how="left",
                )
                .withColumn(
                    f"{key}_charging_type", F.col(f"{right_alias}.charging_type")
                )
                .withColumn(
                    f"hp1_{key}_serenade_type", F.col(f"{right_alias}.seranade_type")
                )
                .withColumn(
                    f"hp1_{key}_register_date", F.col(f"{right_alias}.register_date")
                )
                .withColumn(f"hp1_{key}_n_type", F.col(f"{right_alias}.n_type"))
            )

        joined_cs_ca_data = join_and_transform_cs_data(
            key="current_asset",
            left_data=cma_data.alias("cma"),
            right_data=cs_data,
            right_alias="cs_ca",
        )

        joined_cs_pi_data = join_and_transform_cs_data(
            key="public_id",
            left_data=joined_cs_ca_data,
            right_data=cs_data,
            right_alias="cs_pi",
        )

        selected_joined_data = joined_cs_pi_data.select(
            [
                F.col("cma.*"),
                F.col("notification_name"),
                F.col("current_asset_charging_type"),
                F.col("hp1_current_asset_serenade_type"),
                F.col("hp1_current_asset_register_date"),
                F.col("hp1_current_asset_n_type"),
                F.col("public_id_charging_type"),
                F.col("hp1_public_id_serenade_type"),
                F.col("hp1_public_id_register_date"),
                F.col("hp1_public_id_n_type"),
            ]
        )

        transformed_data = (
            selected_joined_data.withColumn("product_name", self.__set_null_value())
            .withColumn("product_type", self.__set_null_value())
            .withColumn("product_price", self.__set_null_value())
            .withColumn("product_price_unit", self.__set_null_value())
            .withColumn("product_subcategory", self.__set_null_value())
            .withColumn("product_item_vat_price", self.__set_null_value())
            .withColumn("product_item_vat_unit_price", self.__set_null_value())
            .withColumn("product_brand", self.__set_null_value())
            .withColumn("product_description", self.__set_null_value())
            .withColumn("product_number", self.__set_null_value())
            .withColumn("product_category", self.__set_null_value())
            .withColumn("product_wifi_specification", self.__set_null_value())
        )

        broadcast_product_data = F.broadcast(product_data)

        # transform product_offering
        transformed_data = self._transform_event_data(
            key="dynamic_id",
            object_type=CustomerMobileActivityObjectType.PRODUCT_OFFERING,
            left_data=transformed_data,
            right_data=broadcast_product_data,
            right_alias="product_offering",
        )

        # transform package_order
        transformed_data = self._transform_event_data(
            key="product_item",
            object_type=CustomerMobileActivityObjectType.PACKAGE_ORDER,
            left_data=transformed_data,
            right_data=broadcast_product_data,
            right_alias="product_package_order",
        )

        # transform loyalty_program_product_spec
        transformed_data = self._transform_event_data(
            key="dynamic_id",
            object_type=CustomerMobileActivityObjectType.LOYALTY_PROGRAM_PRODUCT_SPEC,
            left_data=transformed_data,
            right_data=loyalty_product_data,
            right_alias="loyalty",
        )

        transformed_data = (
            transformed_data.alias("cma")
            .join(
                self._sources.communication_message.alias("cms"),
                col("cma.dynamic_id") == col("cms.TORO_messageId"),
                how="left",
            )
            .withColumn(
                "notification_name",
                F.when(
                    F.col("cma.object_type") == "CommunicationMessage",
                    F.col("cms.subject"),
                ),
            )
            .withColumn("category_name", col("cma.category_name"))
            .withColumn("component_index", col("cma.component_index"))
            .withColumn(
                "network_connectivity_type", col("cma.network_connectivity_type")
            )
            .withColumn("previous_screen_name", col("cma.previous_screen_name"))
            .withColumn("status", col("cma.status"))
            .withColumn("detail", col("cma.detail"))
        )

        return transformed_data.select(EVENT_DATA_DEST_SELECT_COLUMNS)

    def _process_loyalty_product_data(
        self, cma_data: DataFrame, parameters: ETLProcessExecuteParameters
    ):
        filtered_cma_data = cma_data.filter(
            F.col("object_type")
            == CustomerMobileActivityObjectType.LOYALTY_PROGRAM_PRODUCT_SPEC
        ).select("dynamic_id")

        def extract_product_spec_characteristic_value():
            return (
                F.col("productSpecCharacteristic")
                .getItem(1)
                .getItem("productSpecCharacteristicValue")
                .getItem(0)
                .getItem("value")
            )

        transformed_data = (
            self._sources.loyalty_program_product_spec.withColumn(
                "product_item_vat_price", self.__set_null_value()
            )
            .withColumn("product_item_vat_unit_price", self.__set_null_value())
            .withColumn(
                "product_type",
                F.col("category").getItem(0).getItem("TORO_categoryType"),
            )
            .withColumn(
                "product_price",
                F.when(
                    extract_product_spec_characteristic_value().cast("int").isNotNull(),
                    extract_product_spec_characteristic_value(),
                ).otherwise(self.__set_null_value()),
            )
            .withColumn(
                "product_price_unit",
                F.col("productSpecCharacteristic").getItem(1).getItem("name"),
            )
            .withColumn(
                "product_category",
                F.col("category").getItem(0).getItem("name"),
            )
            .withColumn(
                "product_subcategory",
                F.col("category").getItem(1).getItem("name"),
            )
            .withColumn("coupon_enddate", F.col("validFor.endDateTime"))
            .withColumn(
                "project_subtype",
                F.col("productSpecCharacteristic")
                .getItem(10)
                .getItem("productSpecCharacteristicValue")
                .getItem(0)
                .getItem("value"),
            )
        )
        return (
            filtered_cma_data.alias("cma")
            .join(
                other=transformed_data.alias("loyalty"),
                on=F.col("cma.dynamic_id") == F.col("loyalty.id"),
                how="inner",
            )
            .select(
                [
                    F.col("loyalty.id").alias("product_id"),
                    F.col("loyalty.name").alias("product_name"),
                    F.col("loyalty.product_type"),
                    F.col("loyalty.product_price"),
                    F.col("loyalty.product_price_unit"),
                    F.col("loyalty.product_category"),
                    F.col("loyalty.product_subcategory"),
                    F.col("loyalty.product_item_vat_price"),
                    F.col("loyalty.product_item_vat_unit_price"),
                    F.col("loyalty.brand").alias("product_brand"),
                    F.col("loyalty.description").alias("product_description"),
                    F.col("loyalty.coupon_enddate"),
                    F.col("loyalty.project_subtype"),
                ]
            )
            .distinct()
        )

    def _process_payment_event(
        self, cs_data: DataFrame, payment_activity_data: DataFrame
    ):
        joined_payment_item = (
            payment_activity_data.alias("payment")
            .join(
                other=cs_data.alias("cs_payment_item"),
                on=(F.col("payment_item_type") == "number")
                & (
                    F.col("payment.payment_item_value")
                    == F.col("cs_payment_item.number")
                ),
                how="left",
            )
            .withColumn(
                "payment_item_charging_type", F.col("cs_payment_item.charging_type")
            )
            .withColumn(
                "payment_item_value_serenade_type",
                F.col("cs_payment_item.seranade_type"),
            )
            .withColumn(
                "hp1_payment_item_value_register_date",
                F.col("cs_payment_item.register_date"),
            )
            .withColumn(
                "payment_item_value_n_type",
                F.col("cs_payment_item.n_type"),
            )
        )

        joined_payment_payee = (
            joined_payment_item.alias("payment")
            .join(
                other=cs_data.alias("cs_payee"),
                on=(F.col("payment.payment_payee_mobile") == F.col("cs_payee.number")),
                how="left",
            )
            .withColumn("payee_mobile_charging_type", F.col("cs_payee.charging_type"))
            .withColumn("payee_mobile_serenade_type", F.col("cs_payee.seranade_type"))
            .withColumn("payee_mobile_register_date", F.col("cs_payee.register_date"))
            .withColumn("payee_mobile_n_type", F.col("cs_payee.n_type"))
        )

        joined_payment_payer = (
            joined_payment_payee.alias("payment")
            .join(
                other=cs_data.alias("cs_payer"),
                on=(F.col("payment.payment_payer_mobile") == F.col("cs_payer.number")),
                how="left",
            )
            .withColumn("payer_mobile_charging_type", F.col("cs_payer.charging_type"))
            .withColumn("payer_mobile_serenade_type", F.col("cs_payer.seranade_type"))
            .withColumn("payer_mobile_register_date", F.col("cs_payer.register_date"))
            .withColumn("payer_mobile_n_type", F.col("cs_payer.n_type"))
            .withColumn("action", F.col("payment.action"))
        )

        return joined_payment_payer.select(PAYMENT_EVENT_SELECT_COLUMNS)

    # region ETL
    def _extract_customer_mobile_number(self, key: str, data: DataFrame):
        return (
            data.filter(F.col(key).isNotNull())
            .withColumn("number", F.col(key))
            .select(["number"])
            .distinct()
        )

    def _transform_customer_asset(self, customer_mobile_activity_data: DataFrame):
        transformed_product_data = (
            self._sources.product.withColumn(
                "number",
                self._extract_product_characteristic_value(key="number"),
            )
            .withColumn(
                "segment",
                self._extract_product_characteristic_value(
                    key="segment", is_nullable=True
                ),
            )
            .withColumn(
                "rating_type",
                self._extract_product_characteristic_value(
                    key="ratingType", is_nullable=True
                ),
            )
            .withColumn(
                "n_type",
                self._extract_product_characteristic_value(
                    key="nType", is_nullable=True
                ),
            )
            .withColumn(
                "register_date",
                F.expr("to_date(to_timestamp(startDate) + INTERVAL 7 HOURS)").cast(
                    StringType()
                ),
            )
            .select(
                [
                    F.col("id").alias("product_id"),
                    F.col("name"),
                    F.col("number"),
                    F.col("segment"),
                    F.col("rating_type"),
                    F.col("n_type"),
                    F.col("register_date"),
                ]
            )
        )

        cma_ca_number_data = self._extract_customer_mobile_number(
            key="current_asset", data=customer_mobile_activity_data
        )
        cma_pi_number_data = self._extract_customer_mobile_number(
            key="public_id", data=customer_mobile_activity_data
        )
        cma_payment_item_value_number_data = self._extract_customer_mobile_number(
            key="payment_item_value",
            data=customer_mobile_activity_data.filter(
                F.col("payment_item_type") == "number"
            ),
        )
        cma_payment_payer_number_data = self._extract_customer_mobile_number(
            key="payment_payer_mobile", data=customer_mobile_activity_data
        )
        cma_payment_payee_number_data = self._extract_customer_mobile_number(
            key="payment_payee_mobile", data=customer_mobile_activity_data
        )

        cma_number_data = (
            cma_ca_number_data.union(cma_pi_number_data)
            .union(cma_payment_item_value_number_data)
            .union(cma_payment_payer_number_data)
            .union(cma_payment_payee_number_data)
        ).distinct()

        return (
            cma_number_data.alias("customer")
            .join(
                other=transformed_product_data.alias("product"),
                on=F.col("customer.number") == F.col("product.number"),
                how="inner",
            )
            .withColumn(
                "charging_type",
                F.when(
                    F.col("product.name") == "Mobile Line", F.col("product.rating_type")
                ).otherwise(
                    F.when(F.col("product.name") == "Fixed Broadband", F.lit("FBB"))
                ),
            )
            .select(
                [
                    F.col("customer.number"),
                    F.col("charging_type"),
                    F.col("segment").alias("seranade_type"),
                    F.col("n_type"),
                    F.col("register_date"),
                ]
            )
            .dropDuplicates(["number"])
        )

    def _transform_customer_mobile_activity(self, data: DataFrame):
        def hash(value: str):
            if value is None:
                return None

            crypto_lib = CDLL("libaiscrypto.so")

            buffer = create_string_buffer(b"\000" * 128)

            crypto_lib.ais_encrypt_public_id(value.encode("utf-8"), buffer, 128)

            encoded_bytes: bytes = buffer.value

            return encoded_bytes.decode("utf-8")

        hash_udf = F.udf(hash, StringType())

        encoded_data = (
            data.withColumn("hp1_public_id", hash_udf(F.col("public_id")))
            .withColumn("hp1_current_asset", hash_udf(F.col("current_asset")))
            .withColumn("hp1_payee_mobile", hash_udf(F.col("payment_payee_mobile")))
            .withColumn("hp1_payer_mobile", hash_udf(F.col("payment_payer_mobile")))
            .withColumn(
                "hp1_payment_item_value",
                F.when(
                    F.col("payment_item_type") == "number",
                    hash_udf(F.col("payment_item_value")),
                ).otherwise(self.__set_null_value()),
            )
        )

        transformed_data = self.__transform_hp1_value(
            src_column="public_id", dest_column="hp1_public_id_type", data=encoded_data
        )
        transformed_data = self.__transform_hp1_value(
            src_column="current_asset",
            dest_column="hp1_current_asset_type",
            data=transformed_data,
        )

        # transform payment
        transformed_data = self.__transform_hp1_value(
            src_column="payment_item_value",
            dest_column="hp1_payment_item_value_type",
            data=transformed_data,
        )
        transformed_data = self.__transform_hp1_value(
            src_column="payment_payee_mobile",
            dest_column="hp1_payee_mobile_type",
            data=transformed_data,
        )
        transformed_data = self.__transform_hp1_value(
            src_column="payment_payer_mobile",
            dest_column="hp1_payer_mobile_type",
            data=transformed_data,
        )

        transformed_data = self.__transform_screen_name(
            data=transformed_data,
        )
        transformed_data = self.__transform_notification_name(
            data=transformed_data,
        )

        join_category_data = (
            transformed_data.alias("cma")
            .join(
                other=self._sources.category.alias("category"),
                on=(
                    (F.col("category.id") == F.col("cma.dynamic_id"))
                    & (F.col("cma.object_type") == "Category")
                ),
                how="left",
            )
            .withColumn("category_name", F.col("category.name"))
            .select(
                [
                    "cma.*",
                    "category_name",
                ]
            )
        )

        # patch app_session and session
        patched_data = self.__patch_session_value(data=join_category_data)

        return patched_data.withColumn(
            "object_type",
            F.when(F.col("object_type").isNotNull(), F.trim(F.col("object_type"))),
        )

    def _transform_payment_activity(self, cma_data: DataFrame):
        cma_data = cma_data.filter(
            F.col("action").isin(
                ["paymentCompleted", "paymentCreated", "createPaymentOrder"]
            )
        ).select(PAYMENT_ACTIVITY_SELECT_COLUMNS)

        return cma_data

    def _transform_event_data(
        self,
        key: str,
        object_type: str,
        left_data: DataFrame,
        right_data: DataFrame,
        right_alias: str,
    ) -> DataFrame:
        joined_data = left_data.alias("left").join(
            other=right_data.alias(right_alias),
            on=(F.col("object_type") == object_type)
            & (F.col(f"left.{key}") == F.col(f"{right_alias}.product_id")),
            how="left",
        )

        object_type_col = "left.object_type"

        def set_value(check_key: str, set_key: str):
            return F.when(
                F.col(f"left.{check_key}").isNull(),
                F.col(f"{right_alias}.{set_key}"),
            ).otherwise(F.col(f"left.{check_key}"))

        def set_product_value(check_key: str, set_key: str):
            return F.when(
                F.col(object_type_col).isin(
                    [
                        CustomerMobileActivityObjectType.PRODUCT_OFFERING,
                        CustomerMobileActivityObjectType.PACKAGE_ORDER,
                    ]
                ),
                set_value(check_key=check_key, set_key=set_key),
            ).otherwise(F.lit(None))

        def set_loyalty_value(check_key: str, set_key: str):
            return F.when(
                F.col(object_type_col)
                == CustomerMobileActivityObjectType.LOYALTY_PROGRAM_PRODUCT_SPEC,
                set_value(check_key=check_key, set_key=set_key),
            ).otherwise(F.col(f"left.{check_key}"))

        transformed_data = (
            joined_data.withColumn(
                "transformed_product_name",
                set_value(check_key="product_name", set_key="product_name"),
            )
            .withColumn(
                "transformed_product_type",
                set_value(check_key="product_type", set_key="product_type"),
            )
            .withColumn(
                "transformed_product_price",
                set_value(check_key="product_price", set_key="product_price"),
            )
            .withColumn(
                "transformed_product_price_unit",
                set_value(check_key="product_price_unit", set_key="product_price_unit"),
            )
            .withColumn(
                "transformed_product_item_vat_price",
                set_value(
                    check_key="product_item_vat_price", set_key="product_item_vat_price"
                ),
            )
            .withColumn(
                "transformed_product_item_vat_unit_price",
                set_value(
                    check_key="product_item_vat_unit_price",
                    set_key="product_item_vat_unit_price",
                ),
            )
        )

        if object_type == CustomerMobileActivityObjectType.LOYALTY_PROGRAM_PRODUCT_SPEC:
            transformed_data = (
                transformed_data.withColumn(
                    "transformed_product_subcategory",
                    set_loyalty_value(
                        check_key="product_subcategory",
                        set_key="product_subcategory",
                    ),
                )
                .withColumn(
                    "transformed_product_brand",
                    set_loyalty_value(
                        check_key="product_brand", set_key="product_brand"
                    ),
                )
                .withColumn(
                    "transformed_product_description",
                    set_loyalty_value(
                        check_key="product_description", set_key="product_description"
                    ),
                )
                .withColumn(
                    "transformed_product_number",
                    F.col("left.product_number"),
                )
                .withColumn(
                    "transformed_coupon_enddate",
                    set_loyalty_value("coupon_enddate", "coupon_enddate"),
                )
                .withColumn(
                    "transformed_project_subtype",
                    set_loyalty_value("project_subtype", "project_subtype"),
                )
                .withColumn(
                    "transformed_product_category",
                    F.col("left.product_category"),
                )
                .withColumn(
                    "transformed_product_wifi_specification",
                    F.col("left.product_wifi_specification"),
                )
            )
        else:
            transformed_data = (
                transformed_data.withColumn(
                    "transformed_product_subcategory",
                    F.col("left.product_subcategory"),
                )
                .withColumn(
                    "transformed_product_brand",
                    set_product_value(
                        check_key="product_brand", set_key="product_brand"
                    ),
                )
                .withColumn(
                    "transformed_product_description",
                    F.col("left.product_description"),
                )
                .withColumn(
                    "transformed_product_number",
                    set_product_value(
                        check_key="product_number", set_key="product_number"
                    ),
                )
                .withColumn("transformed_coupon_enddate", F.lit(None))
                .withColumn("transformed_project_subtype", F.lit(None))
                .withColumn(
                    "transformed_product_category",
                    set_product_value("product_category", "product_category"),
                )
                .withColumn(
                    "transformed_product_wifi_specification",
                    set_product_value(
                        "product_wifi_specification", "product_wifi_specification"
                    ),
                )
            )

        transformed_data = (
            transformed_data.withColumn(
                "coupon_enddate", F.col("transformed_coupon_enddate")
            )
            .withColumn("project_subtype", F.col("transformed_project_subtype"))
            .withColumn("product_category", F.col("transformed_product_category"))
            .withColumn(
                "product_wifi_specification",
                F.col("transformed_product_wifi_specification"),
            )
        )

        selected_columns = EVENT_DATA_SELECT_COLUMNS + [
            "product_item",
            F.col("transformed_product_name").alias("product_name"),
            F.col("transformed_product_type").alias("product_type"),
            F.col("transformed_product_price").alias("product_price"),
            F.col("transformed_product_price_unit").alias("product_price_unit"),
            F.col("transformed_product_subcategory").alias("product_subcategory"),
            F.col("transformed_product_item_vat_price").alias("product_item_vat_price"),
            F.col("transformed_product_item_vat_unit_price").alias(
                "product_item_vat_unit_price"
            ),
            F.col("transformed_product_brand").alias("product_brand"),
            F.col("transformed_product_description").alias("product_description"),
            F.col("transformed_product_number").alias("product_number"),
            F.col("transformed_product_category").alias("product_category"),
            F.col("transformed_product_wifi_specification").alias(
                "product_wifi_specification"
            ),
            F.col("transformed_coupon_enddate").alias("coupon_enddate"),
            F.col("transformed_project_subtype").alias("project_subtype"),
            F.col("left.category_name"),
            F.col("left.component_index"),
            F.col("left.network_connectivity_type"),
            F.col("left.previous_screen_name"),
            F.col("left.status"),
            F.col("left.detail"),
        ]

        return transformed_data.select(selected_columns)

    def _extract_product_characteristic_value(self, key: str, is_nullable=False):
        return self.__explode_array_or_object_by(
            column="productCharacteristic",
            key_name="name",
            key_value=key,
            value_name="value",
            default_value=F.array(self.__set_null_value()),
            is_nullable=is_nullable,
        )

    def __extract_values_by_key_into_array(
        self,
        column: str,
        key_name: str,
        key_value: str,
        value_name: str,
        default_value: Column = None,
        is_nullable=False,
    ):
        if is_nullable:
            return F.when(
                F.size(
                    F.expr(
                        f"filter({column}, item -> item.{key_name} = '{key_value}' and item.{value_name} is not null)"
                    )
                )
                > 0,
                F.expr(
                    f"transform(filter({column}, item -> item.{key_name} = '{key_value}' and item.{value_name} is not null), item -> item.{value_name})"
                ),
            ).otherwise(default_value)
        else:
            return F.expr(
                f"transform(filter({column}, item -> item.{key_name} = '{key_value}'), item -> item.{value_name})"
            )

    def __explode_array_or_object_by(
        self,
        column: str,
        key_name: str,
        key_value: str,
        value_name: str,
        default_value: Column = None,
        is_nullable=False,
    ):
        return F.explode(
            self.__extract_values_by_key_into_array(
                column=column,
                key_name=key_name,
                key_value=key_value,
                value_name=value_name,
                default_value=default_value,
                is_nullable=is_nullable,
            )
        )

    def __transform_hp1_value(self, src_column: str, dest_column: str, data: DataFrame):
        return data.withColumn(
            dest_column,
            F.when(F.col(src_column).isNull(), self.__set_null_value())
            .when(F.col(src_column).rlike("^[0][0-9]{9}$"), F.lit("MOBILE"))
            .when(
                F.col(src_column).rlike("^[8][0-9]{9}$")
                | F.col(src_column).rlike("^[0]{2}7[0-9]{7}$"),
                F.lit("FBB"),
            )
            .when(
                F.col(src_column).contains("@") & ~F.col(src_column).contains(" "),
                F.lit("EMAIL"),
            )
            .otherwise(F.lit("DEVICE_ID")),
        )

    def __transform_screen_name(self, data: DataFrame):
        def parse_and_clean(url):
            if url is None:
                return url

            parsed_url: ParseResult = urlparse(url)
            cleaned_url = (
                f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                if parsed_url.scheme in ["http", "https"]
                else url
            )

            # truncate to 590 characters
            return cleaned_url[:590]

        parse_and_clean_udf = F.udf(parse_and_clean, StringType())

        return data.withColumn("screen_name", parse_and_clean_udf(F.col("screen_name")))

    def __transform_notification_name(self, data: DataFrame):
        return (
            data.alias("cma")
            .join(
                other=self._sources.communication_message.alias("cms"),
                on=(
                    (F.col("cma.dynamic_id") == F.col("cms.TORO_messageId"))
                    & (F.col("cma.object_type") == "CommunicationMessage")
                ),
                how="left",
            )
            .withColumn("notification_name", F.col("cms.subject"))
        )

    def __transform_product_specification_by_type(
        self, data: DataFrame, target_key: str, key: str
    ):
        return (
            data.withColumn(
                f"{target_key}_items",
                self.__extract_values_by_key_into_array(
                    column="productSpecCharacteristic",
                    key_name="TORO_productSpecCharacteristicType",
                    key_value=key,
                    value_name="productSpecCharacteristicValue",
                    default_value=F.array(
                        F.array(
                            F.struct(
                                F.lit(None).alias("value"),
                                F.lit(None).alias("valueType"),
                                F.lit(None).alias("unitOfMeasure"),
                            )
                        )
                    ),
                    is_nullable=True,
                ),
            )
            .withColumn(
                f"{target_key}_item", F.explode(F.col(f"{target_key}_items").getItem(0))
            )
            .withColumn(
                f"{target_key}_bullet_items",
                self.__extract_values_by_key_into_array(
                    column="productSpecCharacteristic",
                    key_name="TORO_productSpecCharacteristicType",
                    key_value=f"{key}Bullet",
                    value_name="productSpecCharacteristicValue",
                    default_value=F.array(
                        F.array(
                            F.struct(
                                F.lit(None).alias("value"),
                                F.lit(None).alias("valueType"),
                                F.lit(None).alias("unitOfMeasure"),
                            )
                        )
                    ),
                    is_nullable=True,
                ),
            )
            .withColumn(
                f"{target_key}_bullet_item",
                F.explode(F.col(f"{target_key}_bullet_items").getItem(0)),
            )
            .withColumn(
                target_key,
                F.when(
                    (F.col(f"{target_key}_item.value").isNull())
                    & (F.col(f"{target_key}_bullet_item.value").isNull()),
                    F.lit(None),
                ).otherwise(
                    F.when(
                        (F.col(f"{target_key}_item.value").isNotNull())
                        & (F.col(f"{target_key}_bullet_item.value").isNotNull()),
                        F.concat_ws(
                            " | ",
                            F.concat_ws(
                                " ",
                                F.col(f"{target_key}_item.value"),
                                F.col(f"{target_key}_item.unitOfMeasure"),
                            ),
                            F.concat_ws(
                                " ",
                                F.col(f"{target_key}_bullet_item.value"),
                                F.col(f"{target_key}_bullet_item.unitOfMeasure"),
                            ),
                        ),
                    ).otherwise(
                        F.concat_ws(
                            " ",
                            F.concat_ws(
                                " ",
                                F.col(f"{target_key}_item.value"),
                                F.col(f"{target_key}_item.unitOfMeasure"),
                            ),
                            F.concat_ws(
                                " ",
                                F.col(f"{target_key}_bullet_item.value"),
                                F.col(f"{target_key}_bullet_item.unitOfMeasure"),
                            ),
                        )
                    )
                ),
            )
        )

    def __patch_session_value(self, data: DataFrame):
        session_data_config = self._get_data_config(name="appSession", cls=AppData)
        session_data_props = session_data_config.dest.get_properties(
            cls=FileDataModuleProperties
        )
        session_storage_props = self.operation.get_storage(
            name=session_data_props.storage
        ).get_properties(cls=AzureAdlsGen2StorageDataProperties)
        session_data_path = self.__build_path(
            base_path=session_storage_props.endpoint,
            paths=[session_storage_props.path, session_data_props.path],
        )
        self.__create_delta_table_if_not_exists(
            path=session_data_path, scheme=CACHE_APP_SESSION_SCHEMA
        )

        previous_session_data = self._read_source(name="appSession")
        list_actions = [
            "myIdLoggedIn",
            "myIdRegistered",
            "paymentCompleted",
            "packageOrderCreated",
        ]

        if "time_partition" not in data.columns:
            data = data.withColumn("time_partition", F.lit(None).cast(StringType()))

        current_session_data = (
            data.filter(~F.col("action").isin(list_actions))
            .select(["time_partition", "app_session", "session"])
            .drop_duplicates(["app_session"])
        )

        session_data = previous_session_data.union(
            current_session_data
        ).drop_duplicates(["app_session"])

        self.__clean_old_app_session_cache()
        self.operation.write_destination(
            data=session_data, data_module=session_data_config.dest
        )

        joined_session_data = data.alias("cma").join(
            other=session_data.alias("session"),
            on=F.col("cma.app_session") == F.col("session.app_session"),
            how="left",
        )

        transformed_data = joined_session_data.withColumn(
            "actual_session",
            F.when(
                F.col("session.session").isNotNull(), F.col("session.session")
            ).otherwise(F.col("cma.session")),
        ).select(["cma.*", "actual_session"])
        select_columns = transformed_data.columns.copy()
        select_columns.remove("session")

        return transformed_data.select(
            [F.col(column) for column in select_columns]
            + [F.col("actual_session").alias("session")]
        )

    def __load_datas(
        self,
        cma_data: DataFrame,
        cs_data: DataFrame,
        product_data: DataFrame,
        event_data_data: DataFrame,
        loyalty_product_data: DataFrame,
        payment_event_data: DataFrame,
        payment_activity_data: DataFrame,
        parameters: ETLProcessExecuteParameters,
        process_id: str,
        start_at: datetime,
        end_at: datetime,
        is_check_duplicate: bool = False,
    ):
        cs_count = 0
        cma_count = 0
        product_count = 0
        event_data_count = 0
        loyalty_product_count = 0
        payment_event_count = 0
        payment_activity_count = 0

        cs_completed = False
        cma_completed = False
        product_completed = False
        event_data_completed = False
        loyalty_product_completed = False
        payment_event_completed = False
        payment_activity_completed = False

        try:
            cs_count = self._load_to(
                name="customerAsset",
                table="customer_asset",
                keys=["number"],
                data=cs_data,
                is_rollback_on_error=False,
                is_upsert=parameters.is_upsert,
                is_check_duplicate=True,
                is_repartition=False,
                batch_size=20000,
            )
            cs_completed = True

            cma_count = self._load_to(
                name="customerMobileActivity",
                table="customer_mobile_activity",
                keys=["cma_id"],
                data=cma_data,
                is_rollback_on_error=False,
                is_upsert=parameters.is_upsert,
                is_check_duplicate=is_check_duplicate,
                is_repartition=True,
                batch_size=20000,
            )
            cma_completed = True

            payment_activity_count = self._load_to(
                name="paymentActivity",
                table="payment_activity",
                keys=["payment_item"],
                data=payment_activity_data,
                is_rollback_on_error=False,
                is_upsert=parameters.is_upsert,
                is_check_duplicate=is_check_duplicate,
                is_repartition=True,
                batch_size=20000,
            )
            payment_activity_completed = True

            product_count = self._load_to(
                name="product",
                table="product",
                keys=["product_id"],
                data=product_data,
                is_check_duplicate=True,
                is_repartition=False,
                batch_size=20000,
            )
            product_completed = True

            loyalty_product_count = self._load_to(
                name="loyaltyProduct",
                table="loyalty_product",
                keys=["product_id"],
                data=loyalty_product_data,
                is_rollback_on_error=False,
                is_upsert=parameters.is_upsert,
                is_check_duplicate=True,
                is_repartition=True,
                batch_size=20000,
            )
            loyalty_product_completed = True

            event_data_count = self._load_to(
                name="eventData",
                table="event_data",
                keys=["cma_id"],
                data=event_data_data,
                is_rollback_on_error=False,
                is_upsert=parameters.is_upsert,
                is_check_duplicate=is_check_duplicate,
                is_repartition=True,
                batch_size=20000,
            )
            event_data_completed = True

            payment_event_count = self._load_to(
                name="paymentEvent",
                table="payment_event",
                keys=["payment_item"],
                data=payment_event_data,
                is_rollback_on_error=False,
                is_upsert=parameters.is_upsert,
                is_check_duplicate=is_check_duplicate,
                is_repartition=True,
                batch_size=20000,
            )
            payment_event_completed = True

            now = datetime.now(timezone.utc)
            elapsed_time = now.timestamp() - self._execution_timestamp.timestamp()

            self.logger.info(f"complete load datas at: {now.isoformat()}")

            if parameters.is_process_queue_enabled:
                self.__insert_process_summary(
                    process_id=process_id,
                    start_at=start_at,
                    end_at=end_at,
                    elapsed_time=elapsed_time,
                    cma_count=cma_count,
                    cs_count=cs_count,
                    product_count=product_count,
                    event_data_count=event_data_count,
                    loyalty_product_count=loyalty_product_count,
                    payment_activity_count=payment_activity_count,
                    payment_event_count=payment_event_count,
                )
        except:
            if not cs_completed:
                cs_count = self._count_exists(
                    name="customerAsset",
                    table="customer_asset",
                    keys=["number"],
                    data=cs_data,
                    process_id=process_id,
                    count_name="cs_count",
                )
            elif not cma_completed:
                cma_count = self._count_exists(
                    name="customerMobileActivity",
                    table="customer_mobile_activity",
                    keys=["cma_id"],
                    data=cma_data,
                    process_id=process_id,
                    count_name="cma_count",
                )
            elif not payment_activity_completed:
                payment_activity_count = self._count_exists(
                    name="paymentActivity",
                    table="payment_activity",
                    keys=["payment_item"],
                    data=payment_activity_data,
                    process_id=process_id,
                    count_name="payment_count",
                )
            elif not product_completed:
                product_count = self._count_exists(
                    name="product",
                    table="product",
                    keys=["product_id"],
                    data=product_data,
                    process_id=process_id,
                    count_name="product_count",
                )
            elif not event_data_completed:
                event_data_count = self._count_exists(
                    name="eventData",
                    table="event_data",
                    keys=["cma_id"],
                    data=event_data_data,
                    process_id=process_id,
                    count_name="event_data_count",
                )
            elif not loyalty_product_completed:
                loyalty_product_count = self._count_exists(
                    name="loyaltyProduct",
                    table="loyalty_product",
                    keys=["product_id"],
                    data=loyalty_product_data,
                    process_id=process_id,
                    count_name="loyalty_count",
                )
            elif not payment_event_completed:
                payment_event_count = self._count_exists(
                    name="paymentEvent",
                    table="payment_event",
                    keys=["payment_item"],
                    data=payment_event_data,
                    process_id=process_id,
                    count_name="payment_event_count",
                )

            now = datetime.now(timezone.utc)
            elapsed_time = now.timestamp() - self._execution_timestamp.timestamp()

            self.logger.info(f"failed load datas at: {now.isoformat()}")

            if parameters.is_process_queue_enabled:
                self.__insert_process_summary(
                    process_id=process_id,
                    start_at=start_at,
                    elapsed_time=elapsed_time,
                    end_at=end_at,
                    cma_count=cma_count,
                    cs_count=cs_count,
                    product_count=product_count,
                    event_data_count=event_data_count,
                    loyalty_product_count=loyalty_product_count,
                    payment_activity_count=payment_activity_count,
                    payment_event_count=payment_event_count,
                )
            raise

    def __read_customer_mobile_activity(self, start_at: datetime):
        data_config = self._get_data_config(name="customerMobileActivity", cls=AppData)
        self.__tmp_file_name = str(uuid.uuid4())

        if data_config.src.type == DataModuleType.Database:
            return self.__read_customer_mobule_activity_by_driver(
                data_module=data_config.src, start_at=start_at
            )
        elif data_config.src.type == DataModuleType.File:
            return self.__read_customer_mobile_activity_by_file(
                data_module=data_config.src, start_at=start_at
            )
        else:
            raise ValueError("data source type is not supported")

    def __read_customer_mobule_activity_by_driver(
        self, data_module: DataModule, start_at: datetime
    ):
        start_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.logger.debug("read customer_mobile_activity data: started")

        data_props = data_module.get_properties(DatabaseDataModuleProperties)
        storage_props = self.operation.get_storage_properties(
            name=data_props.storage, cls=CassandraDataProperties
        )

        spark_options = self.config.engine.spark_options

        contact_points = str(spark_options.get("spark.cassandra.connection.host", ""))
        port = spark_options.get("spark.cassandra.connection.port", 9042)
        local_dc = spark_options.get("spark.cassandra.connection.local_dc", "")
        user = spark_options.get("spark.cassandra.auth.username", "")
        password = spark_options.get("spark.cassandra.auth.password", "")
        keyspace = storage_props.keyspace
        table = data_props.table

        try:
            self.logger.debug(
                "read customer_mobile_activity data: connection to cassandra establishing"
            )
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            auth_provider = PlainTextAuthProvider(username=user, password=password)
            cluster = Cluster(
                contact_points=contact_points.split(","),
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=local_dc),
                auth_provider=auth_provider,
                protocol_version=4,
                ssl_context=ssl_context,
                port=int(port),
            )
            session = cluster.connect(keyspace=keyspace)
            session.default_timeout = 3600

            if session:
                self.logger.debug(
                    "read customer_mobile_activity data: connection to cassandra established, start query data"
                )
                cql_query = f"SELECT * FROM {keyspace}.{table} WHERE time_partition = '{start_at.strftime(self._datetime_format)}'"
                statement = SimpleStatement(
                    cql_query, consistency_level=ConsistencyLevel.ALL
                )
                rows = session.execute(statement)

                rows_list = list(rows)
                rows_count = len(rows_list)

                if rows_count == 0:
                    self.logger.info("no data found, returning empty dataframe.")
                    spark = self.operation.get_current_spark_session()
                    data = spark.createDataFrame([], CUSTOMER_MOBILE_ACTIVITY_SCHEMA)

                    session.shutdown()
                    cluster.shutdown()

                    end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

                    self.logger.info(
                        f"read customer_mobile_activity data: completed in {end_timestamp - start_timestamp} ms"
                    )

                    return data

                system_storage_props = self.operation.get_storage(
                    name="adlsGen2"
                ).get_properties(AzureAdlsGen2StorageDataProperties)
                container_client = self.__build_adls_gen2_container_client(
                    storage_props=system_storage_props
                )

                spark = self.operation.get_current_spark_session()

                num_instances = (
                    self.config.engine.spark_executor_max_instances
                    * self.config.engine.spark_executor_core
                )
                chunk_size = math.ceil(rows_count / num_instances)
                num_chunks = len(rows_list) // chunk_size + 1

                log_data = {
                    "uuid": self.__tmp_file_name,
                    "row_count": rows_count,
                    "chunk_size": chunk_size,
                    "chunk_count": num_chunks,
                }
                self.logger.debug(f"read from cassandra: {log_data}")

                for i in range(num_chunks):
                    chunk_rows_list = rows_list[i * chunk_size : (i + 1) * chunk_size]
                    chunk_data = pd.DataFrame(
                        chunk_rows_list,
                    )
                    actual_tmp_file_name = f"{self.__tmp_file_name}_{i}.json"
                    tmp_file_path = f"/tmp/{actual_tmp_file_name}"

                    selected_data = chunk_data.filter(
                        items=CUSTOMER_MOBILE_ACTIVITY_SRC_SELECT_COLUMNS
                    )
                    selected_data.to_json(
                        tmp_file_path,
                        orient="records",
                        lines=True,
                    )

                    self.logger.info(f"upload file to storage: {tmp_file_path}")

                    with open(tmp_file_path, "rb") as data:
                        container_client.upload_blob(
                            name=tmp_file_path, data=data, overwrite=True
                        )

                    os.remove(tmp_file_path)

                tmp_file_path = f"/tmp/{self.__tmp_file_name}_*.json"
                read_file_path = system_storage_props.endpoint + tmp_file_path

                data = (
                    spark.read.schema(CUSTOMER_MOBILE_ACTIVITY_SCHEMA)
                    .format("json")
                    .load(read_file_path)
                )

                # TODO:
                data = data.withColumn(
                    "engagement_time",
                    F.round(F.col("engagement_time").cast(LongType())),
                )
                data = data.withColumn(
                    "engagement_step",
                    F.round(F.col("engagement_step").cast(LongType())),
                )
                data = data.withColumn(
                    "total_engagement_time",
                    F.round(F.col("total_engagement_time").cast(LongType())),
                )

                session.shutdown()
                cluster.shutdown()

                end_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

                self.logger.info(
                    f"read customer_mobile_activity data: completed in {end_timestamp - start_timestamp} ms"
                )

                return data
            else:
                raise Exception(
                    "error reading customer_mobile_activity data, connecting to cassandra"
                )
        except Exception as e:
            self.logger.error(
                f"error reading customer_mobile_activity data: {e}", exc_info=True
            )
            raise e

    def __read_customer_mobile_activity_by_file(
        self, data_module: DataModule, start_at: datetime
    ):
        data_props = data_module.get_properties(FileDataModuleProperties)
        storage_props = self.operation.get_storage_properties(
            name=data_props.storage, cls=AzureAdlsGen2StorageDataProperties
        )
        if data_props.storage == "system":
            cma_data = self._read_source(name="customerMobileActivity")
            filter_cma_data = cma_data.filter(
                F.col("time_partition") == start_at.strftime(self._datetime_format)
            )
            return filter_cma_data
        else:
            paths = [f"{start_at.strftime(self._datetime_format)}.snappy.parquet"]
            if storage_props.path:
                paths.append(storage_props.path)

            if data_props.path:
                paths.append(data_props.path)

            path = self.__build_path(
                base_path=storage_props.endpoint,
                paths=paths,
            )

            self.logger.debug(f"read customer_mobile_activity data from path: {path}")

            spark = self.operation.get_current_spark_session()
            return spark.read.parquet(path)

    def __cleanup_temporary_data(self):
        system_storage_props = self.operation.get_storage(
            name="adlsGen2"
        ).get_properties(AzureAdlsGen2StorageDataProperties)
        container_client = self.__build_adls_gen2_container_client(
            storage_props=system_storage_props
        )

        blobs_to_delete = container_client.list_blobs(
            name_starts_with=f"tmp/{self.__tmp_file_name}"
        )
        for blob in blobs_to_delete:
            self.logger.debug(f"cleanup file: {blob.name}")
            container_client.delete_blob(blob.name)

    def __build_adls_gen2_container_client(
        self, storage_props: AzureAdlsGen2StorageDataProperties
    ):
        container_name = self.__get_container_name(storage_props=storage_props)
        account_name = storage_props.endpoint.split("@")[1].split(".")[0]
        account_key = ""

        spark_options = self.config.engine.spark_options
        spark_option_pattern = r"fs\.azure\.account\.key\.(.*?)\."

        for key, value in spark_options.items():
            match = re.match(spark_option_pattern, key)

            if match and match.group(1) == account_name:
                account_key = value
                break

        creds = AzureNamedKeyCredential(account_name, account_key)

        account_url = f"https://{account_name}.blob.core.windows.net"

        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=creds
        )
        return blob_service_client.get_container_client(container=container_name)

    # endregion

    # region internal
    def __create_process(
        self,
        process_id: str,
        start_at: datetime,
        end_at: datetime,
        cma_count: int,
    ):
        process_data = create_dataframe(
            spark=self.operation.get_current_spark_session(),
            schema=PROCESS_SCHEMA,
            datas=[
                {
                    "id": process_id.upper(),
                    "start_at": start_at.isoformat(),
                    "end_at": end_at.isoformat(),
                    "is_processed": False,
                    "effective_date": self._execution_timestamp.isoformat(),
                    "retries": 0,
                    "cma_count": cma_count,
                }
            ],
        )

        self._write_destination_to_mssql(
            name="process", data=process_data, batch_size=1000
        )

    def __update_is_processed(
        self,
        process_id: str,
    ):
        data_config = self._get_data_config(name="process", cls=AppData)
        props = data_config.dest.get_properties(DatabaseDataModuleProperties)
        storage_props = self.operation.get_storage_properties(
            name=props.storage, cls=MSSqlDataProperties
        )

        self._update_to_sql(
            table=props.table,
            filters={"id": process_id, "is_processed": 0},
            updates={"is_processed": 1},
            storage_props=storage_props,
        )

    def __increment_process_retry(self, process_id: str, retry: int):
        data_config = self._get_data_config(name="process", cls=AppData)
        props = data_config.dest.get_properties(DatabaseDataModuleProperties)
        storage_props = self.operation.get_storage_properties(
            name=props.storage, cls=MSSqlDataProperties
        )

        self._update_to_sql(
            table=props.table,
            filters={"id": process_id, "is_processed": 0},
            updates={"retries": retry + 1},
            storage_props=storage_props,
        )

    def __insert_process_summary(
        self,
        process_id: str,
        start_at: datetime,
        end_at: datetime,
        elapsed_time: float,
        cma_count: int,
        cs_count: int,
        product_count: int,
        event_data_count: int,
        loyalty_product_count: int,
        payment_activity_count: int,
        payment_event_count: int,
    ):
        process_summary_data = create_dataframe(
            spark=self.operation.get_current_spark_session(),
            schema=SUMMARY_RECORD_SCHEMA,
            datas=[
                {
                    "process_id": process_id.upper(),
                    "start_at": start_at.isoformat(),
                    "end_at": end_at.isoformat(),
                    "cma_count": cma_count,
                    "cs_count": cs_count,
                    "product_count": product_count,
                    "event_data_count": event_data_count,
                    "loyalty_product_count": loyalty_product_count,
                    "payment_activity_count": payment_activity_count,
                    "payment_event_count": payment_event_count,
                    "effective_date": self._execution_timestamp.isoformat(),
                    "process_duration": elapsed_time * 1000,
                }
            ],
        )

        self._write_destination_to_mssql(
            name="processSummary", data=process_summary_data, batch_size=1000
        )

    def __set_null_value(self):
        return F.lit(None).cast(StringType())

    def __create_delta_table_if_not_exists(self, path: str, scheme: StructType):
        """
        TODO: need to migrate to use from pyeqx.core instead.

        Args:
            path (str): path to the delta table
            scheme (StructType): schema of the delta table
        """
        spark = self.operation.get_current_spark_session()

        is_dt = DeltaTable.isDeltaTable(spark, path)

        if not is_dt:
            new_df = create_dataframe(spark=spark, schema=scheme)
            self.operation.get_writer().write_to(
                data=new_df, format="delta", mode="append", path=path
            )

    def __build_path(self, base_path: str, paths: list[str]) -> str:
        """
        Build path from base path and given list of paths

        TODO: migrate to use pyeqx-core

        Args:
            base_path (str): base path
            paths (list[str]): list of paths

        Returns:
            str: built path
        """
        assert all(
            isinstance(path, str) for path in paths
        ), "paths must be a list of strings."

        return "/".join([base_path, ("/".join(paths)).replace("//", "/")])

    def __get_container_name(self, storage_props: AzureAdlsGen2StorageDataProperties):
        abfs_pattern = r"abfss://([^@]+)@"

        container_match = re.search(abfs_pattern, storage_props.endpoint)
        if container_match:
            container_name = container_match.group(1)

        if not container_name:
            raise ValueError("container name not found.")

        return container_name

    def __clean_old_app_session_cache(self, time_hr: int = 24):
        session_data_config = self._get_data_config(name="appSession", cls=AppData)
        session_data_props = session_data_config.src.get_properties(
            FileDataModuleProperties
        )
        session_storage_props = self.operation.get_storage_properties(
            name=session_data_props.storage, cls=AzureAdlsGen2StorageDataProperties
        )
        time_threshold = datetime.now() - timedelta(hours=time_hr)
        condition = F.col("time_partition") < F.lit(
            time_threshold.strftime(self._datetime_format)
        )
        session_path = self.__build_path(
            base_path=session_storage_props.endpoint,
            paths=[session_storage_props.path, session_data_props.path],
        )
        spark = self.operation.get_current_spark_session()
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        self.logger.debug(f"path = {session_path}")
        try:
            dt = DeltaTable.forPath(spark, session_path)
            dt.delete(condition)

            dt.vacuum(0)
            self.logger.info(
                f"Old app session records older than {time_hr} hours deleted."
            )
        except Exception as e:
            self.logger.error(
                f"error cleaning old app session cache {e}", exc_info=True
            )

    # endregion
