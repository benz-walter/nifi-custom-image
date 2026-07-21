from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import requests
from requests.auth import HTTPBasicAuth
import json

class InvokeInsecureHttp(FlowFileSource):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = "1.0.0"
        description = "Loads data from URL ignoring insecure SSL certificates"
        dependencies = ['requests']

    URL = PropertyDescriptor(
        name="URL",
        description="URL to data source (GET request)",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR]
    )

    USERNAME = PropertyDescriptor(
        name="User name",
        description="User name for basic authentication",
        required=False
    )

    PASSWORD = PropertyDescriptor(
        name="Password",
        description="Password for basic authentication",
        required=False,
        sensitive=True
    )

    properties = [URL, USERNAME, PASSWORD]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def load_data_from_url(self, url, username, password):

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        auth = None
        if username and password:
            auth = HTTPBasicAuth(username, password)

        response = requests.get(
            url,
            auth=auth,
            verify=False,
            headers=headers
        )

        response.raise_for_status()
        return response.json()

    def create(self, context):

        try:
            url = context.getProperty(self.URL).getValue()
            username = context.getProperty(self.USERNAME).getValue()
            password = context.getProperty(self.PASSWORD).getValue()

            result = self.load_data_from_url(url, username, password)

            return FlowFileSourceResult(
                relationship="success",
                contents=json.dumps(result)
            )

        except Exception as e:
            import traceback
            error = f"{str(e)}\n{traceback.format_exc()}"
            self.logger.error(error)

            return FlowFileSourceResult(
                relationship="failure",
                contents=error
            )