from dataclasses import dataclass
from logging import Logger
from pprint import pformat
import re
import os

from adlfs import AzureBlobFileSystem
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureNamedKeyCredential
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

from pyeqx.core import Configuration, Operation
from pyeqx.core.models import AppData
from pyeqx.core.models.module.properties import FileDataModuleProperties
from pyeqx.core.models.storage.properties import AzureAdlsGen2StorageDataProperties

from app.constants import EVENT_DATA_SELECT_COLUMNS
from app.execution import ETLProcess
from app.parameters import ETLProcessExecuteParameters, ProcessParameters
from app.schemas import (
    PRODUCT_SRC_SCHEMA,
    PRODUCT_OFFERING_PRICE_SRC_SCHEMA,
    PRODUCT_OFFERING_SRC_SCHEMA,
    PRODUCT_SPECIFICATION_SRC_SCHEMA,
)


@dataclass
class Sources:
    product: DataFrame
    product_offering: DataFrame
    product_offering_price: DataFrame
    product_specification: DataFrame


class MigrationProcess(ETLProcess):
    __product_data: DataFrame
    __product_cs_data: DataFrame

    __account_name: str
    __account_key: str

    def __init__(self, config: Configuration, logger: Logger, operation: Operation):
        super().__init__(config=config, logger=logger, operation=operation)

    def configure(self, parameters: ProcessParameters):
        super().configure(parameters)

        spark_options = self.config.engine.spark_options
        spark_option_pattern = r"fs\.azure\.account\.key\.(.*?)\."

        for key, value in spark_options.items():
            match = re.match(spark_option_pattern, key)

            if match:
                account_name = match.group(1)
                account_key = value
                break

        if not account_name or not account_key:
            raise ValueError("Azure Storage Account Name and Key not found.")

        self.__account_key = account_key
        self.__account_name = account_name

    def _read_datas(self, parameters: ETLProcessExecuteParameters):
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

        self._sources = Sources(
            product=product_data,
            product_offering=product_offering_data,
            product_offering_price=product_offering_price_data,
            product_specification=product_specification_data,
        )

    def _process_datas(self, parameters: ETLProcessExecuteParameters):
        self.__product_data = self._process_product(parameters=parameters).cache()
        self.__product_cs_data = self.__transform_product_cs().cache()

        self._load_to(
            name="product",
            table="product",
            keys=["product_id"],
            data=self.__product_data,
            is_check_duplicate=True,
            is_repartition=False,
            batch_size=200000,
        )

        files = self.__list_files(path="*.csv")

        self.logger.info(f"detect files: \n {pformat(files)}")

        for file_name in files:
            self.logger.info(f"processing file: {file_name}")

            self.__do_process(
                parameters=parameters,
                file_name=file_name,
                is_check_duplicate=parameters.is_retry,
            )

    def __do_process(
        self,
        parameters: ETLProcessExecuteParameters,
        file_name: str,
        is_check_duplicate: bool = False,
    ):
        transformed_cma_data = None
        transformed_cs_data = None
        try:
            transformed_cma_data = self.__process_customer_mobile_activity(
                file_name=file_name,
                is_check_duplicate=is_check_duplicate,
                parameters=parameters,
            )
            transformed_cs_data = self.__process_customer_asset(
                cma_data=transformed_cma_data, parameters=parameters
            )

            self.__process_event_data(
                cma_data=transformed_cma_data,
                cs_data=transformed_cs_data,
                is_check_duplicate=is_check_duplicate,
                file_name=file_name,
                parameters=parameters,
            )

            self.__move_to(src_path=file_name, dest_path="success")
        except Exception as e:
            self.logger.error(f"error processing file: {file_name}, {e}")
            self.__move_to(src_path=file_name, dest_path="error")
        finally:
            if transformed_cma_data is not None:
                transformed_cma_data.unpersist()
            if transformed_cs_data is not None:
                transformed_cs_data.unpersist()

    def __process_customer_mobile_activity(
        self,
        file_name: str,
        parameters: ETLProcessExecuteParameters,
        is_check_duplicate: bool = False,
    ):
        cma_data = self.__read_source_csv(path=file_name)

        transformed_cma_data = (
            (self._transform_customer_mobile_activity(data=cma_data))
            .withColumn("created_at", F.to_timestamp("created_at"))
            .cache()
        )

        if not parameters.is_migration_to_json and not parameters.is_migration_to_delta:
            self._load_to(
                name="customerMobileActivity",
                table="customer_mobile_activity",
                keys=["cma_id"],
                data=transformed_cma_data,
                is_rollback_on_error=False,
                is_upsert=False,
                is_check_duplicate=is_check_duplicate,
                is_repartition=True,
                batch_size=200000,
            )

        return transformed_cma_data

    def __process_customer_asset(
        self, cma_data: DataFrame, parameters: ETLProcessExecuteParameters
    ):
        transformed_cs_data = self.__transform_customer_asset(
            customer_mobile_activity_data=cma_data
        ).cache()

        if not parameters.is_migration_to_json and not parameters.is_migration_to_delta:
            self._load_to(
                name="customerAsset",
                table="customer_asset",
                keys=["number"],
                data=transformed_cs_data,
                is_rollback_on_error=False,
                is_upsert=parameters.is_upsert,
                is_check_duplicate=True,
                is_repartition=False,
                batch_size=200000,
            )

        return transformed_cs_data

    def __process_event_data(
        self,
        cma_data: DataFrame,
        cs_data: DataFrame,
        file_name: str,
        parameters: ETLProcessExecuteParameters,
        is_check_duplicate: bool = False,
    ):
        event_data_data = self._process_event_data(
            cma_data=cma_data,
            cs_data=cs_data,
            product_data=self.__product_data,
        ).cache()

        selected_event_data_data = event_data_data.select(EVENT_DATA_SELECT_COLUMNS)

        if parameters.is_migration_to_json:
            self.__load_to_json(
                file_name=file_name,
                data=selected_event_data_data,
            )
        elif parameters.is_migration_to_delta:
            self.__load_to_delta(file_name=file_name, data=selected_event_data_data)
        else:
            self._load_to(
                name="eventData",
                table="event_data",
                keys=["cma_id"],
                data=selected_event_data_data,
                is_rollback_on_error=False,
                is_upsert=False,
                is_check_duplicate=is_check_duplicate,
                is_repartition=True,
                batch_size=200000,
            )

        event_data_data.unpersist()

    def __transform_product_cs(self):
        return (
            self._sources.product.withColumn(
                "number",
                F.explode(
                    F.expr(
                        "transform(filter(productCharacteristic, item -> item.name = 'number'), item -> item.value)"
                    )
                ),
            )
            .withColumn(
                "segment",
                F.explode(
                    F.when(
                        F.size(
                            F.expr(
                                "filter(productCharacteristic, item -> item.name = 'segment' and item.value is not null)"
                            )
                        )
                        > 0,
                        F.expr(
                            "transform(filter(productCharacteristic, item -> item.name = 'segment' and item.value is not null), item -> item.value)"
                        ),
                    ).otherwise(F.array(F.lit(None))),
                ),
            )
            .withColumn(
                "rating_type",
                F.explode(
                    F.when(
                        F.size(
                            F.expr(
                                "filter(productCharacteristic, item -> item.name = 'ratingType' and item.value is not null)"
                            )
                        )
                        > 0,
                        F.expr(
                            "transform(filter(productCharacteristic, item -> item.name = 'ratingType' and item.value is not null), item -> item.value)"
                        ),
                    ).otherwise(F.array(F.lit(None))),
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
                    F.col("register_date"),
                ]
            )
        )

    def __transform_customer_asset(self, customer_mobile_activity_data: DataFrame):
        joined_cma_ca_data = (
            customer_mobile_activity_data.filter(F.col("current_asset").isNotNull())
            .withColumn("number", F.col("current_asset"))
            .select(["number"])
            .distinct()
        )
        joined_cma_pi_data = (
            customer_mobile_activity_data.filter(F.col("public_id").isNotNull())
            .withColumn("number", F.col("public_id"))
            .select(["number"])
            .distinct()
        )

        transformed_cma_data = joined_cma_ca_data.union(joined_cma_pi_data).distinct()

        return (
            transformed_cma_data.alias("customer")
            .join(
                other=self.__product_cs_data.alias("product"),
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
                    F.col("register_date"),
                ]
            )
            .dropDuplicates(["number"])
        )

    def __read_source_csv(self, path: str) -> DataFrame:
        try:
            self.logger.debug("read source csv: started.")

            data_config = self._get_data_config(name="cassandraMigration", cls=AppData)
            data_props = data_config.src.get_properties(FileDataModuleProperties)
            storage_props = self.operation.get_storage_properties(
                name=data_props.storage, cls=AzureAdlsGen2StorageDataProperties
            )

            file_path = "/".join(
                [storage_props.path, data_props.path, os.path.basename(path)]
            ).replace("//", "/")
            actual_path = f"{storage_props.endpoint}{file_path}"

            spark = self.operation.get_current_spark_session()

            return (
                spark.read.format("csv")
                .option("quote", '"')
                .option("escape", '"')
                .option("header", "true")
                .load(actual_path)
            )
        except Exception as e:
            self.logger.error(f"read source csv: failed: {e}")
            raise

    def __list_files(self, path: str):
        data_config = self._get_data_config(name="cassandraMigration", cls=AppData)
        data_props = data_config.src.get_properties(FileDataModuleProperties)

        storage_props = self.operation.get_storage_properties(
            name=data_props.storage, cls=AzureAdlsGen2StorageDataProperties
        )

        container_name = self.__get_container_name(storage_props=storage_props)

        fs = AzureBlobFileSystem(
            account_name=self.__account_name, account_key=self.__account_key
        )

        file_path = "/".join([storage_props.path, data_props.path, path])

        actual_path = (f"{container_name}/{file_path}").replace("//", "/")

        self.logger.debug(f"list files in: {actual_path}")

        return fs.glob(actual_path, filesystem=fs)

    def __move_to(self, src_path: str, dest_path: str):
        data_config = self._get_data_config(name="cassandraMigration", cls=AppData)
        data_props = data_config.src.get_properties(FileDataModuleProperties)

        storage_props = self.operation.get_storage_properties(
            name=data_props.storage, cls=AzureAdlsGen2StorageDataProperties
        )

        actual_path = ("/".join([storage_props.path, data_props.path])).replace(
            "//", "/"
        )

        file_name = os.path.basename(src_path)

        actual_src_path = os.path.join(actual_path, file_name)
        actual_dest_path = os.path.join(actual_path, dest_path, file_name)

        container_name = self.__get_container_name(storage_props=storage_props)

        creds = AzureNamedKeyCredential(self.__account_name, self.__account_key)

        account_url = f"https://{self.__account_name}.blob.core.windows.net"

        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=creds
        )

        source_blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=actual_src_path
        )

        destination_blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=actual_dest_path
        )

        exists = source_blob_client.exists()

        if not exists:
            self.logger.info(
                f"source file does not exist, container: {container_name}, path: {src_path}"
            )
            return

        copy_props = destination_blob_client.start_copy_from_url(source_blob_client.url)

        properties = destination_blob_client.get_blob_properties()
        if properties.copy.status == "success":
            source_blob_client.delete_blob()
            self.logger.info(f"move file from: {src_path}, to: {actual_dest_path}")

        else:
            self.logger.info(f"move operation failed: {copy_props['copy_status']}")

    def __get_container_name(self, storage_props: AzureAdlsGen2StorageDataProperties):
        abfs_pattern = r"abfss://([^@]+)@"

        container_match = re.search(abfs_pattern, storage_props.endpoint)
        if container_match:
            container_name = container_match.group(1)

        if not container_name:
            raise ValueError("container name not found.")

        return container_name

    def __load_to_json(self, file_name: str, data: DataFrame):
        data_config = self._get_data_config(name="cassandraMigration", cls=AppData)
        data_props = data_config.src.get_properties(FileDataModuleProperties)

        storage_props = self.operation.get_storage_properties(
            name=data_props.storage, cls=AzureAdlsGen2StorageDataProperties
        )

        actual_file_name, _ = os.path.splitext(os.path.basename(file_name))

        path = ("/".join([storage_props.path, "outputs"])).replace("//", "/")
        actual_path = os.path.join(path, f"{actual_file_name}.json")

        tmp_file_path = f"/tmp/{actual_file_name}.json"

        self.logger.info(f"write to json: {tmp_file_path}")

        data = data.withColumn(
            "created_at", F.date_format("created_at", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        pandas_data = data.toPandas()
        pandas_data.to_json(tmp_file_path, orient="records", lines=True)

        creds = AzureNamedKeyCredential(self.__account_name, self.__account_key)

        account_url = f"https://{self.__account_name}.blob.core.windows.net"

        container_name = self.__get_container_name(storage_props=storage_props)

        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=creds
        )
        container_client = blob_service_client.get_container_client(
            container=container_name
        )

        self.logger.info(f"upload json file to storage: {actual_path}")

        with open(tmp_file_path, "rb") as data:
            container_client.upload_blob(name=actual_path, data=data, overwrite=True)

        os.remove(tmp_file_path)

    def __load_to_delta(self, file_name: str, data: DataFrame):
        data_config = self._get_data_config(name="cassandraMigration", cls=AppData)
        data_props = data_config.src.get_properties(FileDataModuleProperties)

        storage_props = self.operation.get_storage_properties(
            name=data_props.storage, cls=AzureAdlsGen2StorageDataProperties
        )

        actual_file_name, _ = os.path.splitext(os.path.basename(file_name))

        path = (
            "/".join([storage_props.path, "data", "event_data", actual_file_name])
        ).replace("//", "/")

        actual_path = f"{storage_props.endpoint}{path}"

        self.logger.info(f"write to delta format: {actual_path}")

        self.operation.get_writer().write_to(
            data=data, format="delta", mode="overwrite", path=actual_path
        )
