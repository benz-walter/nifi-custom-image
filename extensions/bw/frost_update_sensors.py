import json
import requests

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship


class FROSTSensorUpdate(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.1.0"
        description = (
            "Uses keycloak connection and datastream setup table to update sensor data and datastreams"
            "together with incoming flowfile records."
        )
        dependencies = ['pandas', 'requests']

    DBCP_SERVICE = PropertyDescriptor(
        name="Database Connection Pool Service",
        description="The Controller Service that is used to obtain a connection to the database.",
        required=True,
        controller_service_definition="org.apache.nifi.dbcp.DBCPService"
    )

    DATASTREAM_TABLE = PropertyDescriptor(
        name="Datastream setup table",
        description="Table used to gather information about datastreams",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    FROST_BASE_URL = PropertyDescriptor(
        name="FROST base URL",
        description="URL of FROST server",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    KEYCLOAK_USER = PropertyDescriptor(
        name="Keycloak user",
        description="User name to connect with keycloak",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    KEYCLOAK_PASSWORD= PropertyDescriptor(
        name="Keycloak password",
        description="Password to connect with keycloak",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    LOCATION_BY_DESCRIPTION = PropertyDescriptor(
        name="Location by description",
        description="Select whether to check location by description",
        default_value="false",
        allowable_values=["true", "false"],
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )


    properties = [
        DBCP_SERVICE,
        DATASTREAM_TABLE,
        FROST_BASE_URL,
        KEYCLOAK_USER,
        KEYCLOAK_PASSWORD,
        LOCATION_BY_DESCRIPTION
    ]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def getAuthHeader(self, username=None, password=None):
        keycloak_token = self.getKeycloakToken(username, password)
        post_headers = {"Content-Type": "application/json", "Authorization": "Bearer " + keycloak_token}
        return post_headers

    def getKeycloakToken(self, username, password):
        url = "https://login.freiburg.de/realms/freiburg/protocol/openid-connect/token"
        token_header = {"Content-Type": "application/x-www-form-urlencoded"}
        body = {
            "grant_type": "password",
            "client_id": "automation",
            "username": username,
            "password": password,
            "scope": "openid",
        }

        response = requests.post(url, data=body, headers=token_header)
        token = json.loads(response.content)["access_token"]
        return token

    def createObservedPropertyAndGetIds(self, df, base_url, post_header):
        property_ids = []
        for _, row in df.iterrows():
            property_name = row.property_name
            property_description = row.property_description
            property_definition = row.property_definition
            observed_property_technical_id = self.getObservedPropertyAndCreateIfMissing(property_name, property_definition, property_description, base_url, post_header)
            property_ids.append(observed_property_technical_id)
        return property_ids

    def getObservedPropertyAndCreateIfMissing(self, property_name, property_definition, property_description, base_url, post_header):
        property_id = self.getObservedPropertyId(property_definition, base_url)

        if property_id != -1:
            return property_id

        self.createObservedProperty(property_name, property_definition, property_description, base_url, post_header)
        return self.getObservedPropertyId(property_definition, base_url)

    def createObservedProperty(self, property_name, property_definition, property_description, base_url, post_header):
        request = base_url + "/ObservedProperties"
        body = {
            "name": property_name,
            "description": property_definition,
            "definition": property_description,
        }
        requests.post(request, data=json.dumps(body), headers=post_header)

    def getObservedPropertyId(self, definition, base_url):
        request = base_url + "/ObservedProperties?$filter=definition eq '{definition}'&$select=@iot.id,properties/disabled".format(
            definition=definition)
        return self.getIdFromEntity(request)

    @staticmethod
    def getIdFromEntity(request):
        response = requests.get(request)
        decoded = json.loads(response.content)
        value = decoded["value"]

        if value == []:
            return -1
        else:
            return value[0]["@iot.id"]

    def getLocationAndCreateIfMissing(self, location_name, location_by_description, description, lat, long, base_url, post_header):
        if location_by_description in (True, "true"):
            property_id = self.getLocationByDescription(description, base_url)
        else:
            property_id = self.getLocationByLatLong(lat, long, base_url)
        if property_id != -1:
            return property_id

        self.createLocation(location_name, description, lat, long, base_url, post_header)
        if location_by_description:
            return self.getLocationByDescription(description, base_url)
        else:
            return self.getLocationByLatLong(lat, long, base_url)

    @staticmethod
    def createLocation(locationName, description, lat, long, base_url, post_header):
        request = base_url + "/Locations"
        body = {
            "name": locationName,
            "description": description,
            "encodingType": "application/geo+json",
            "location": {"type": "Point", "coordinates": [long, lat]},
            "properties": {}
        }
        requests.post(request, data=json.dumps(body), headers=post_header)

    def getLocationByDescription(self, description, base_url):
        request = base_url + "/Locations?$filter=description eq '{description}'&$select=@iot.id,properties/disabled".format(
            description=description)
        return self.getIdFromEntity(request)

    def getLocationByLatLong(self, lat, long, base_url):
        request = base_url + "/Locations?$filter=st_within(location, geography'POINT({long} {lat})')".format(long=long,
                                                                                                            lat=lat)
        return self.getIdFromEntity(request)

    def buildSensorDatastreamRequest(self, row, observed_properties_id, df, location_id, base_url):
        requestBody = {
            "requests": []
        }

        sensor_id = row.sensor_id
        sensor_name = row.sensor_name
        sensor_description = row.sensor_description
        source_name = row.source_name
        sensor_properties = None
        if row.sensor_properties:
            sensor_properties = row.sensor_properties
            if isinstance(sensor_properties, str):
                sensor_properties = json.loads(row.sensor_properties.replace("'", '"').replace("None", 'null'))

        thing_id = row.thing_id

        thing_name = row.thing_name
        thing_description = row.thing_description

        thing_properties = None
        if row.thing_properties:
            thing_properties = row.thing_properties
            if isinstance(thing_properties, str):
                thing_properties = json.loads(row.thing_properties.replace("'", '"').replace("None", 'null').replace(
                    "False", 'false').replace("True", 'true'))

        sensor_id_reference = "$sensor1"
        sensor_technical_id = self.getSensorId(sensor_id, source_name, base_url)
        if sensor_technical_id != -1:
            sensor_id_reference = sensor_technical_id
        else:
            requestBody["requests"].append(
                self.buildSensorRequest(sensor_name, sensor_description, sensor_id, source_name, sensor_properties))

        thing_id_reference = "$thing1"
        thing_technical_id = self.getThingId(thing_id, source_name, base_url)
        if thing_technical_id != -1:
            thing_id_reference = thing_technical_id
        else:
            requestBody["requests"].append(
                self.buildThingRequest(thing_name, thing_description, thing_id, location_id, thing_properties, source_name))

        for i in range(len(observed_properties_id)):
            row = df.iloc[i]
            datastream_body = self.buildDatastreamBody(row, sensor_id, thing_id_reference, sensor_id_reference,
                                                 observed_properties_id[i], i, base_url)
            if datastream_body is not None:
                requestBody["requests"].append(datastream_body)
        return requestBody

    def getSensorId(self, sensor_id, source_name, base_url):
        request = base_url + "/Sensors?$filter=properties/id eq '{sensorFachId}' and properties/sourceName eq '{sourceName}'&$select=@iot.id,properties/disabled".format(
            sensorFachId=sensor_id, sourceName=source_name)
        return self.getIdFromEntity(request)

    def getThingId(self, thing_fach_id, source_name, base_url):
        request = base_url + "/Things?$filter=properties/id eq '{thingFachId}' and properties/sourceName eq '{sourceName}'&$select=@iot.id,properties/disabled".format(
            thingFachId=thing_fach_id, sourceName=source_name)
        return self.getIdFromEntity(request)

    @staticmethod
    def buildThingRequest(thing_name, thing_description, thing_id, location_id, thing_properties, source_name):
        properties = {"id": thing_id, "sourceInfo": thing_properties or {}}
        if source_name:
            properties["sourceName"] = source_name
        request = {
            "id": "thing1",
            "atomicityGroup": "group1",
            "method": "post",
            "url": "Things",
            "body": {
                "name": thing_name,
                "description": thing_description,
                "Locations": [{"@iot.id": location_id}],
                "properties": properties
            }
        }
        return request

    @staticmethod
    def buildSensorRequest(sensor_name, sensor_description, sensor_id, source_name, sensor_properties):
        properties = {"id": sensor_id, "sourceInfo": sensor_properties or {}}
        if source_name:
            properties["sourceName"] = source_name
        request = {
            "id": "sensor1",
            "atomicityGroup": "group1",
            "method": "post",
            "url": "Sensors",
            "body": {"name": sensor_name,
                     "description": sensor_description,
                     "encodingType": "",
                     "metadata": "",
                     "properties": properties
                     }
        }
        return request

    def buildDatastreamBody(self, data, sensor_id, thing_id_reference, sensor_id_reference, observed_properties_id, request_index, base_url):

        stream_description = data.stream_description
        stream_name = data.stream_name_header + ' ' + str(sensor_id)
        stream_observed_type = data.stream_observed_type
        stream_unit_name = data.stream_unit_name
        stream_unit_symbol = data.stream_unit_symbol
        stream_unit_definition = data.stream_unit_definition
        stream_measurement_type = data.stream_measurement_type

        if isinstance(sensor_id_reference, int) and isinstance(thing_id_reference, int):
            datastream_id = self.getDatastream(sensor_id_reference, stream_measurement_type, thing_id_reference, base_url)
            if datastream_id > 0:
                return None
        else:
            body = {
                "id": str(request_index),
                "atomicityGroup": "group1",
                "method": "post",
                "url": "Datastreams",
                "body": {
                    "name": stream_name,
                    "description": stream_description,
                    "observationType": stream_observed_type,
                    "unitOfMeasurement": {
                        "name": stream_unit_name,
                        "symbol": stream_unit_symbol,
                        "definition": stream_unit_definition
                    },
                    "properties": {"measurement_type": stream_measurement_type, "disabled": False},
                    "ObservedProperty": {"@iot.id": observed_properties_id},
                    "Sensor": {"@iot.id": sensor_id_reference},
                    "Thing": {"@iot.id": thing_id_reference}
                }
            }
            return body

    def getDatastream(self, sensor_id, measurement_type, thing_id, base_url):
        request = base_url + "/Sensors({sensorID})/Datastreams?$filter=properties/measurement_type eq '{measurementType}' and Thing/@iot.id eq {thingId}&$select=@iot.id,properties/disabled".format(
            sensorID=sensor_id, measurementType=measurement_type, thingId=thing_id)
        return self.getIdFromEntity(request)

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
        df_flow = pd.DataFrame(flow_data)

        ds_table = context.getProperty(self.DATASTREAM_TABLE).evaluateAttributeExpressions(flowfile).getValue()
        base_url = context.getProperty(self.FROST_BASE_URL).evaluateAttributeExpressions(flowfile).getValue()
        username = context.getProperty(self.KEYCLOAK_USER).evaluateAttributeExpressions(flowfile).getValue()
        password = context.getProperty(self.KEYCLOAK_PASSWORD).evaluateAttributeExpressions(flowfile).getValue()
        location_by_description = context.getProperty(self.LOCATION_BY_DESCRIPTION).evaluateAttributeExpressions(flowfile).getValue()

        sql = f"SELECT * from {ds_table}"

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
                    
                    df_ds = pd.DataFrame(rows, columns=columns)
                finally:
                    rs.close()
            finally:
                stmt.close()
        finally:
            conn.close()

        post_headers = self.getAuthHeader(username, password)
        property_ids = self.createObservedPropertyAndGetIds(df_ds, base_url, post_headers)

        status = []
        for _, row in df_flow.iterrows():
            location_name = row.location_name
            lat = row.location_lat
            long = row.location_lon
            location_description = row.location_description
            location_id = self.getLocationAndCreateIfMissing(location_name, location_by_description, location_description, lat, long, base_url, post_headers)
            self.logger.info(f"location_id: {location_id}")
            request_json = self.buildSensorDatastreamRequest(row, property_ids, df_ds, location_id, base_url)
            request = base_url + "/$batch"
            self.logger.info(json.dumps(request_json))
            try:
                response = requests.post(request, data=json.dumps(request_json), headers=post_headers)
                status.append(response.status_code)
            except Exception:
                # try again with new auth token
                post_headers = self.getAuthHeader(username, password)
                response = requests.post(request, data=json.dumps(request_json), headers=post_headers)
                status.append(response.status_code)

        if not 401 in status:
            return FlowFileTransformResult(
                relationship="success",
                contents=json.dumps({})
            )
        else:
            return FlowFileTransformResult(
                relationship="failure",
                contents=json.dumps({})
            )
