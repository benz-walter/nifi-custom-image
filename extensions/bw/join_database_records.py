import json

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship


class JoinDatabaseRecords(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.1.0"
        description = (
            "Combines flowfile records with records from database"
            "using SQL query and comparing provided columns. Uses a DBCPConnectionPool."
        )
        dependencies = ['pandas']

    DBCP_SERVICE = PropertyDescriptor(
        name="Database Connection Pool Service",
        description="The Controller Service that is used to obtain a connection to the database.",
        required=True,
        controller_service_definition="org.apache.nifi.dbcp.DBCPService"
    )

    SQL_QUERY = PropertyDescriptor(
        name="SQL query",
        description="Query executed against database",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    MERGE_COLUMNS = PropertyDescriptor(
        name="Merge columns",
        description="Columns used for merge, must be comma separated and identical for both flowfile and database. "
        "If empty, full outer join is used.",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [
        DBCP_SERVICE,
        SQL_QUERY,
        MERGE_COLUMNS
    ]

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

        contents_bytes = flowfile.getContentsAsBytes()
        contents = contents_bytes.decode('utf-8')
        flow_data = json.loads(contents)
        df = pd.DataFrame(flow_data)

        sql = context.getProperty(self.SQL_QUERY).evaluateAttributeExpressions(flowfile).getValue()
        merge_column_string = context.getProperty(self.MERGE_COLUMNS).evaluateAttributeExpressions(flowfile).getValue()
        merge_columns = [m.strip() for m in merge_column_string.split(',')] if merge_column_string else None

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
                            row.append(rs.getObject(i + 1))
                        rows.append(row)
                    
                    df2 = pd.DataFrame(rows, columns=columns)
                finally:
                    rs.close()
            finally:
                stmt.close()
        finally:
            conn.close()

        if not merge_columns:
            df = df.merge(df2, how='cross')
        else:
            df = df.merge(df2, on=merge_columns, how='left')
        result = df.to_dict(orient='records')


        return FlowFileTransformResult(
            relationship="success",
            contents=json.dumps(result)
        )

        return None
