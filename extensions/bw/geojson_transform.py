from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
import json
from pyproj import Transformer
from shapely.geometry import shape
from shapely.wkt import dumps as wkt_dumps

class GeoJSONTransform(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.0.0"
        description = "Transforms GeoJSON column to WKT with coordinate system transformation"
        dependencies = ['pyproj', 'shapely']

    SOURCE_CRS = PropertyDescriptor(
        name="Source Coordinate System",
        description="Source coordinate system (e.g., EPSG:25832, EPSG:4326)",
        required=False,
        default_value="EPSG:4326",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    TARGET_CRS = PropertyDescriptor(
        name="Target Coordinate System",
        description="Target coordinate system (e.g., EPSG:25832, EPSG:4326)",
        required=True,
        default_value="EPSG:25832",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [SOURCE_CRS, TARGET_CRS]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def transform(self, context, flowfile):
        try:
            target_crs = context.getProperty(self.TARGET_CRS).evaluateAttributeExpressions(flowfile).getValue()

            contents_bytes = flowfile.getContentsAsBytes()
            contents = contents_bytes.decode('utf-8')
            data = json.loads(contents)
            if isinstance(data, list):
                data = data[0]

            # Check if it's a FeatureCollection
            if data.get('type') == 'FeatureCollection':
                features = data.get('features', [])
                source_crs = context.getProperty(self.SOURCE_CRS).evaluateAttributeExpressions(flowfile).getValue()
                source_crs = source_crs or data.get('crs', {}).get('properties', {}).get('name', 'EPSG:4326')
                if 'EPSG' not in source_crs.upper():
                    source_crs = 'EPSG:4326'

                # Flatten features into list of objects
                flattened = []
                for feature in features:
                    if 'geometry' in feature:
                        geom = shape(feature['geometry'])

                        if source_crs.upper() != target_crs.upper():
                            transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
                            geom = self._transform_geometry(geom, transformer)

                        # Create flattened object
                        flat_obj = {
                            'geometry': feature['geometry']
                        }
                        # Add all properties at root level
                        if 'properties' in feature:
                            flat_obj.update(feature['properties'])
                        # Add WKT
                        flat_obj['wkt'] = wkt_dumps(geom)

                        flattened.append(flat_obj)

                result_contents = json.dumps(flattened, ensure_ascii=False)

            # Single geometry or Feature
            else:
                geom = shape(data.get('geometry', data))

                source_crs = context.getProperty(self.SOURCE_CRS).evaluateAttributeExpressions(flowfile).getValue()
                source_crs = source_crs or data.get('crs', {}).get('properties', {}).get('name', 'EPSG:4326')
                if 'EPSG' not in source_crs.upper():
                    source_crs = 'EPSG:4326'

                if source_crs.upper() != target_crs.upper():
                    transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
                    geom = self._transform_geometry(geom, transformer)

                wkt = wkt_dumps(geom)
                data['wkt'] = wkt

                result_contents = json.dumps(data, ensure_ascii=False)

            return FlowFileTransformResult(
                relationship="success",
                contents=result_contents
            )

        except Exception as e:
            import traceback
            error_details = f"Error transforming GeoJSON to WKT: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            self.logger.error(error_details)
            return FlowFileTransformResult(
                relationship="failure",
                contents=error_details
            )

    def _transform_geometry(self, geom, transformer):
        from shapely.ops import transform
        return transform(transformer.transform, geom)