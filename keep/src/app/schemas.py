from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CUSTOMER_MOBILE_ACTIVITY_SCHEMA = StructType(
    [
        StructField("cma_id", StringType(), False),
        StructField("action", StringType(), True),
        StructField("app_name", StringType(), True),
        StructField("app_session", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("campaign_campaign", StringType(), True),
        StructField("campaign_content", StringType(), True),
        StructField("campaign_medium", StringType(), True),
        StructField("campaign_source", StringType(), True),
        StructField("campaign_term", StringType(), True),
        StructField("component_id", StringType(), True),
        StructField("component_type", StringType(), True),
        StructField("component_value", StringType(), True),
        StructField("component_index", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("current_asset", StringType(), True),
        StructField("current_usecase_name", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("device_brand", StringType(), True),
        StructField("device_location_last_updated", StringType(), True),
        StructField("device_location_latitude", StringType(), True),
        StructField("device_location_longitude", StringType(), True),
        StructField("device_model", StringType(), True),
        StructField("device_os", StringType(), True),
        StructField("device_os_version", StringType(), True),
        StructField("dynamic_id", StringType(), True),
        StructField("dynamic_value", StringType(), True),
        StructField("engagement_step", DoubleType(), True),
        StructField("engagement_time", DoubleType(), True),
        StructField("geo_hash", StringType(), True),
        StructField("keyword_search", StringType(), True),
        StructField("language", StringType(), True),
        StructField("location_source", StringType(), True),
        StructField("market_funnel_stage", StringType(), True),
        StructField("mylid", StringType(), True),
        StructField("network_ip", StringType(), True),
        StructField("network_isp", StringType(), True),
        StructField("network_connectivity_type", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("payment_item", StringType(), True),
        StructField("payment_item_name", StringType(), True),
        StructField("payment_item_type", StringType(), True),
        StructField("payment_item_value", StringType(), True),
        StructField("payment_amount", DoubleType(), True),
        StructField("payment_unit", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_payee_mobile", StringType(), True),
        StructField("payment_payer_mobile", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_method_id", StringType(), True),
        StructField("payment_method_detail", StringType(), True),
        StructField("payment_receipt_no", StringType(), True),
        StructField("payment_scratch_number", StringType(), True),
        StructField("payment_total_amount", DoubleType(), True),
        StructField("payment_total_unit", StringType(), True),
        StructField("product_item", StringType(), True),
        StructField("product_order_asset_ref", StringType(), True),
        StructField("product_payment_ref", StringType(), True),
        StructField("public_id", StringType(), True),
        StructField("screen_name", StringType(), True),
        StructField("section", StringType(), True),
        StructField("session", StringType(), True),
        StructField("time_stamp", StringType(), True),
        StructField("time_partition", StringType(), True),
        StructField("total_engagement_time", DoubleType(), True),
        StructField("usecase_step", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("previous_screen_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("detail", StringType(), True),
    ]
)

PRODUCT_SRC_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("startDate", StringType(), True),
        StructField(
            "productCharacteristic",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

PRODUCT_OFFERING_SRC_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField(
            "productOfferingPrice",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), False),
                        StructField("name", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "productSpecification",
            StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "category",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("name", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

PRODUCT_OFFERING_PRICE_TAX_SRC_SCHEMA = StructType(
    [
        StructField(
            "taxAmount",
            StructType(
                [
                    StructField("unit", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

PRODUCT_OFFERING_PRICE_SRC_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField(
            "price",
            StructType(
                [
                    StructField("unit", StringType(), True),
                    StructField("value", StringType(), True),
                ]
            ),
        ),
        StructField("tax", ArrayType(PRODUCT_OFFERING_PRICE_TAX_SRC_SCHEMA), True),
        StructField("recurringChargePeriodLength", IntegerType(), True),
        StructField("recurringChargePeriodType", StringType(), True),
    ]
)

PRODUCT_SPECIFICATION_CHARACTERISTIC_SRC_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("TORO_productSpecCharacteristicType", StringType(), True),
        StructField(
            "productSpecCharacteristicValue",
            ArrayType(
                StructType(
                    [
                        StructField("value", StringType(), True),
                        StructField("valueType", StringType(), True),
                        StructField("unitOfMeasure", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

PRODUCT_SPECIFICATION_SRC_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("TORO_productType", StringType(), True),
        StructField("productNumber", StringType(), True),
        StructField("brand", StringType(), True),
        StructField(
            "productSpecCharacteristic",
            ArrayType(PRODUCT_SPECIFICATION_CHARACTERISTIC_SRC_SCHEMA),
            True,
        ),
    ]
)

PROCESS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("start_at", StringType(), False),
        StructField("end_at", StringType(), False),
        StructField("is_processed", BooleanType(), False),
        StructField("effective_date", StringType(), False),
        StructField("retries", IntegerType(), False),
        StructField("cma_count", IntegerType(), False),
    ]
)

SUMMARY_RECORD_SCHEMA = StructType(
    [
        StructField("process_id", StringType(), False),
        StructField("start_at", StringType(), False),
        StructField("end_at", StringType(), False),
        StructField("cma_count", IntegerType(), False),
        StructField("cs_count", IntegerType(), False),
        StructField("product_count", IntegerType(), False),
        StructField("event_data_count", IntegerType(), False),
        StructField("effective_date", StringType(), False),
        StructField("process_duration", StringType(), False),
    ]
)

DAILY_SUMMARY_RECORD_SCHEMA = StructType(
    [
        StructField("date", StringType(), False),
        StructField("cma_count", IntegerType(), False),
        StructField("cs_count", IntegerType(), False),
        StructField("product_count", IntegerType(), False),
        StructField("event_data_count", IntegerType(), False),
        StructField("start_at", StringType(), False),
        StructField("end_at", StringType(), False),
        StructField("effective_date", StringType(), False),
        StructField("process_duration", StringType(), False),
    ]
)

VALUE_SCHEMA = StructType([StructField("value", StringType())])

LOYALTY_PROGRAM_PRODUCT_SPEC_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "category",
            ArrayType(
                StructType(
                    [
                        StructField("TORO_categoryType", StringType(), True),
                        StructField("name", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "productSpecCharacteristic",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField(
                            "productSpecCharacteristicValue",
                            ArrayType(
                                StructType([StructField("value", StringType(), True)])
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "validFor",
            StructType(
                [
                    StructField("endDateTime", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("brand", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

CACHE_APP_SESSION_SCHEMA = StructType(
    [
        StructField("time_partition", StringType(), True),
        StructField("app_session", StringType(), True),
        StructField("session", StringType(), True),
    ]
)

CATEGORY_SRC_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)
COMMUNICATION_MESSAGE_SRC_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("TORO_messageId", StringType(), True),
        StructField("subject", StringType(), True),
    ]
)
