import json

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope


class FrostObservationCheck(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.1.0"
        description = (
            "Checks timestamp of measurements in FROST server for given "
            "sensor name and timestamp"
        )
        dependencies = ['pandas']

    DBCP_SERVICE = PropertyDescriptor(
        name="Database Connection Pool Service",
        description="The Controller Service that is used to obtain a connection to the database.",
        required=True,
        controller_service_definition="org.apache.nifi.dbcp.DBCPService"
    )

    DATETIME_KEY = PropertyDescriptor(
        name="Datetime key",
        description="Flowfile record key with information about datetime",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    DATETIME_FORMAT = PropertyDescriptor(
        name="Datetime format",
        description="Format of flowfile datetime",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    SENSOR_ID_MAPPING = PropertyDescriptor(
        name="Sensor ID Map",
        description="Map between sensor names of flowfile and database",
        required=True,
        default_value="{}",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [DBCP_SERVICE, DATETIME_KEY, DATETIME_FORMAT, SENSOR_ID_MAPPING]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def transform(self, context, flowfile):
        import pandas as pd
        import traceback

        try:
            self.logger.info("-- Pandas debugging:")
            self.logger.info(f"   Version                : {pd.__version__}")
            self.logger.info(f"   Module                 : {repr(pd)}")
            self.logger.info(f"   pd.core.frame.DataFrame: {repr(pd.core.frame.DataFrame)}")
            self.logger.info(f"   pd.DataFrame           : {repr(pd.DataFrame)}")
        except Exception:
            self.logger.info(traceback.format_exc())

        self.logger.info("-- e/o Pandas debugging.")


        try:
            datetime_key = context.getProperty(self.DATETIME_KEY).evaluateAttributeExpressions(flowfile).getValue()
            date_time_format = context.getProperty(self.DATETIME_FORMAT).evaluateAttributeExpressions(flowfile).getValue()
            sensor_mapping = json.loads(context.getProperty(self.SENSOR_ID_MAPPING).evaluateAttributeExpressions(flowfile).getValue())

            contents_bytes = flowfile.getContentsAsBytes()
            contents = contents_bytes.decode('utf-8')
            data = json.loads(contents)
            df = pd.DataFrame(data)
            sql = f"""
                with sensors as (
                    select id, json_extract_scalar(properties, '$.id') as name from frost.public.sensors where description like 'ESE WS%'
                )
                SELECT s.name, cast(t.phenomenon_time_end as varchar) as phenomenon_time_end
                FROM frost.public.datastreams t inner join sensors s on s.id = t.sensor_id where t.phenomenon_time_end is not null
            """

            dbcp_service = context.getProperty(self.DBCP_SERVICE).asControllerService()
            conn = dbcp_service.getConnection()
            try:
                stmt = conn.prepareStatement(sql)
                try:
                    rs = stmt.executeQuery()
                    try:
                        # Spaltennamen extrahieren
                        meta = rs.getMetaData()
                        col_count = meta.getColumnCount()
                        columns = [meta.getColumnLabel(i + 1) for i in range(col_count)]

                        # Daten extrahieren
                        rows = []
                        while rs.next():
                            row = []
                            for i in range(col_count):
                                value = rs.getString(i + 1)
                                row.append(value)
                            rows.append(row)

                        df2 = pd.DataFrame(rows, columns=columns)
                    finally:
                        rs.close()
                finally:
                    stmt.close()
            finally:
                conn.close()

            df.rename(columns=sensor_mapping, inplace=True)
            value_vars = [val for val in sensor_mapping.values() if val in df.columns]
            df = df.melt(id_vars=[datetime_key], value_vars=value_vars,
                         var_name='Sensor', value_name='Value')
            df[datetime_key] = pd.to_datetime(df[datetime_key], format=date_time_format, utc=True)
            df2.phenomenon_time_end = pd.to_datetime(df2.phenomenon_time_end, utc=True)
            df = df.merge(df2, left_on=["Sensor"], right_on=["name"], how='left')
            df = df[(df.DateTime > df.phenomenon_time_end) & (df.Value.notna())]
            df.drop(columns=["phenomenon_time_end"], inplace=True)
            df.DateTime = df.DateTime.astype(str)
            result = df.to_dict(orient='records')

            result_contents = json.dumps(result, ensure_ascii=False)

            return FlowFileTransformResult(
                relationship="success",
                contents=result_contents.encode("utf-8"),
                attributes={
                    "mime.type": "application/json"
                }
            )

        except Exception as e:
            import traceback
            error_details = f"Error mapping datastream IDs: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            self.logger.error(error_details)
            return FlowFileTransformResult(
                relationship="failure",
                contents=error_details
            )
