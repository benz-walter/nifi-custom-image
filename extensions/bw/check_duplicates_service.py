import json

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship


class CheckDuplicates(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.1.0"
        description = (
            "Checks whether given flowfile content is already contained in database "
            "using SQL query and comparing provided columns. Uses a DBCPConnectionPool."
        )

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

    COLUMN_MAPPING = PropertyDescriptor(
        name="Column mapping",
        description="JSON mapping between flowfile fields and database columns",
        required=True,
        default_value="{}",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [
        DBCP_SERVICE,
        SQL_QUERY,
        COLUMN_MAPPING
    ]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def getRelationships(self):
        return {
            Relationship("success", description="Flowfiles that are not duplicates are routed to this relationship"),
            Relationship("duplicate", description="Flowfiles that are duplicates are routed to this relationship"),
        }

    def transform(self, context, flowfile):
        contents_bytes = flowfile.getContentsAsBytes()
        contents = contents_bytes.decode('utf-8')
        flow_data = json.loads(contents)

        if isinstance(flow_data, dict):
            flow_data = [flow_data]

        sql = context.getProperty(self.SQL_QUERY).evaluateAttributeExpressions(flowfile).getValue()
        column_mapping = json.loads(
            context.getProperty(self.COLUMN_MAPPING)
            .evaluateAttributeExpressions(flowfile)
            .getValue()
        )
        flow_cols = list(column_mapping.keys())
        db_cols = list(column_mapping.values())

        # Schlüssel aller DB-Zeilen als Set -> O(1)-Lookup statt verschachtelter Schleife
        existing_keys = set()

        dbcp_service = context.getProperty(self.DBCP_SERVICE).asControllerService()
        conn = dbcp_service.getConnection()
        try:
            stmt = conn.prepareStatement(sql)
            try:
                stmt.setFetchSize(10000)
                rs = stmt.executeQuery()
                try:
                    meta = rs.getMetaData()
                    labels = [meta.getColumnLabel(i + 1) for i in range(meta.getColumnCount())]
                    # Nur die gemappten Spalten lesen; jeder getObject-Aufruf ist ein Py4J-Roundtrip
                    # Nicht vorhandene Spalten liefern None (wie zuvor db_row.get()).
                    col_indices = [labels.index(c) + 1 if c in labels else None for c in db_cols]

                    while rs.next():
                        existing_keys.add(tuple(
                            str(rs.getObject(idx)) if idx is not None else str(None)
                            for idx in col_indices
                        ))
                finally:
                    rs.close()
            finally:
                stmt.close()
        finally:
            conn.close()

        non_duplicates = []
        duplicates = []

        for item in flow_data:
            key = tuple(str(item.get(c)) for c in flow_cols)
            if key in existing_keys:
                duplicates.append(item)
            else:
                non_duplicates.append(item)

        if non_duplicates:
            return FlowFileTransformResult(
                relationship="success",
                contents=json.dumps(non_duplicates)
            )
        
        if duplicates:
            return FlowFileTransformResult(
                relationship="duplicate",
                contents=json.dumps(duplicates)
            )

        return None
