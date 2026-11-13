"""NiFi 2 processor with the embedded ocit_transform mapping."""

import datetime as dt
import decimal
import json
import math
import re
from xml.dom import Node, minidom
from xml.parsers.expat import ExpatError

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.relationship import Relationship


OCIT = "http://odg_und_partner/OCIT_C"
EXTERNAL = "http://odg_und_partner/external/"
XSI = "http://www.w3.org/2001/XMLSchema-instance"
XML = "http://www.w3.org/XML/1998/namespace"
SOAP = {"http://schemas.xmlsoap.org/soap/envelope/",
        "http://www.w3.org/2003/05/soap-envelope"}


class TransformError(ValueError):
    """Input cannot be converted without guessing or losing information."""


# Explicit object-type routing is necessary: multiple endpoints share XSD types.
ROUTES = {
    "IntersectionFeature": ("intersection_feature", "intersectiondata", "IsFeatureType"),
    "IntersectionStatus": ("intersection_status", "intersectiondata", "IntersectionStatus"),
    "IntersectionPredefined": ("intersection_predefined", "intersectiondata", "PredefCommand"),
    "SGTimes": ("sg_times", "intersectiondata", "SGTimes"),
    "OperatingMessages_Operatingmessage": ("operating_messages", "operatingmessages", "OperatingMessage"),
    "OperatingMessages_OperatingMessage": ("operating_messages", "operatingmessages", "OperatingMessage"),
    "Control_Values": ("control_values", "control", "ControlValue"),
    "TrafficData_detector_currentValue": ("traffic_detector_current_value", "trafficdata", "currentValueType"),
    "TrafficData_detectorGroup_currentValue": ("traffic_detector_group_current_value", "trafficdata", "currentValueType"),
    "TrafficData_detectorGroup_calculatedValue": ("traffic_detector_group_calculated_value", "trafficdata", "calculatedValueType"),
    "TrafficData_trafficSubSection__accumulatedValue": ("traffic_subsection_accumulated_value", "trafficdata", "accumulatedValueType"),
}
for _name in (
    "IntersectionDescription", "IntersectionGroupDescription",
    "TrafficData_detector_Description", "TrafficData_detectorGroup_Description",
    "TrafficData_trafficSubSection_Description", "Control_Description",
):
    ROUTES[_name] = ("object_descriptions", "global", "Description")


def children(node, name, namespace):
    if node is None:
        return []
    return [c for c in node.childNodes if c.nodeType == Node.ELEMENT_NODE
            and c.localName == name and c.namespaceURI == namespace]


def one(node, name, namespace):
    found = children(node, name, namespace)
    if len(found) > 1:
        raise TransformError("multiple %s elements where one was expected" % name)
    return found[0] if found else None


def scalar(node):
    if node is None:
        return None
    nil = node.getAttributeNS(XSI, "nil")
    if node.hasAttributeNS(XSI, "nil") and nil not in ("true", "1", "false", "0"):
        raise TransformError("invalid xsi:nil: %r" % nil)
    if nil in ("true", "1"):
        if any(c.nodeType == Node.ELEMENT_NODE or
               (c.nodeType in (Node.TEXT_NODE, Node.CDATA_SECTION_NODE) and c.data.strip())
               for c in node.childNodes):
            raise TransformError("xsi:nil element has content")
        return None
    if any(c.nodeType == Node.ELEMENT_NODE for c in node.childNodes):
        raise TransformError("expected scalar content in %s" % node.tagName)
    return "".join(c.data for c in node.childNodes
                   if c.nodeType in (Node.TEXT_NODE, Node.CDATA_SECTION_NODE))


def structure(node):
    if node is None:
        return None
    nil = node.getAttributeNS(XSI, "nil")
    if nil in ("true", "1"):
        scalar(node)
        return None
    if node.hasAttributeNS(XSI, "nil") and nil not in ("false", "0"):
        raise TransformError("invalid xsi:nil: %r" % nil)
    return node


def value(node, name, ns, kind="str"):
    return convert(scalar(one(node, name, ns)), kind, name)


def attr(node, name, kind="str"):
    text = node.getAttribute(name) if node is not None and node.hasAttribute(name) else None
    return convert(text, kind, "@" + name)


def convert(text, kind, label):
    if text is None:
        return None
    if kind == "str":
        return text
    s = text.strip()
    try:
        if kind in ("int", "bigint"):
            if not re.fullmatch(r"[+-]?[0-9]+", s):
                raise ValueError("not an integer")
            n = int(s)
            bits = 32 if kind == "int" else 64
            if not -(1 << (bits - 1)) <= n < (1 << (bits - 1)):
                raise ValueError("out of range")
            return n
        if kind == "double":
            if not re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", s):
                raise ValueError("not a finite decimal")
            n = float(s)
            if not math.isfinite(n) or (n == 0 and decimal.Decimal(s) != 0):
                raise ValueError("out of range")
            return n
        if kind == "bool":
            if s not in ("true", "false", "1", "0"):
                raise ValueError("not a boolean")
            return s in ("true", "1")
        if kind == "timestamp":
            match = re.fullmatch(
                r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
                r"(?:\.([0-9]+))?(Z|[+-][0-9]{2}:[0-9]{2})", s)
            if not match or (match.group(2) and any(c != "0" for c in match.group(2)[3:])):
                raise ValueError("requires timezone and exact millisecond precision")
            zone = match.group(3)
            if zone != "Z" and (int(zone[1:3]) > 14 or int(zone[4:]) > 59
                                or (int(zone[1:3]) == 14 and int(zone[4:]) != 0)):
                raise ValueError("invalid timezone")
            fraction = match.group(2)
            if fraction and len(fraction) > 3:
                s = match.group(1) + "." + fraction[:3] + zone
            dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return s
    except (ValueError, OverflowError) as exc:
        raise TransformError("invalid %s for %s: %r (%s)" % (kind, label, text, exc)) from exc
    raise TransformError("unsupported conversion: %s" % kind)


def scope(node):
    bindings = {"xml": XML}
    ancestors = []
    while node is not None and node.nodeType == Node.ELEMENT_NODE:
        ancestors.append(node)
        node = node.parentNode
    for element in reversed(ancestors):
        for i in range(element.attributes.length):
            a = element.attributes.item(i)
            if a.name == "xmlns":
                bindings[""] = a.value
            elif a.prefix == "xmlns":
                bindings[a.localName] = a.value
    return bindings


def elements(node):
    yield node
    for child in node.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            yield from elements(child)


def validate_types(ds, expected, overrides):
    for el in elements(ds):
        a = el.getAttributeNodeNS(XSI, "type")
        if not a:
            continue
        qname = a.value.strip()
        if not re.fullmatch(r"(?:[A-Za-z_][\w.-]*:)?[A-Za-z_][\w.-]*", qname):
            raise TransformError("invalid xsi:type QName: %r" % a.value)
        prefix, local = qname.split(":", 1) if ":" in qname else ("", qname)
        bindings = scope(el)
        uri = bindings.get(prefix)
        if prefix in overrides:
            uri = overrides[prefix]
        elif prefix and not uri:
            raise TransformError("unbound xsi:type prefix %r; supply an explicit repair" % prefix)
        if el.parentNode is ds and el.localName == "data" and el.namespaceURI == OCIT and (uri or "", local) != expected:
            raise TransformError("xsi:type {%s}%s does not match object type {%s}%s"
                                 % (uri or "", local, expected[0], expected[1]))


def field_map(node, ns, spec):
    return {out: value(node, source, ns, kind) for out, source, kind in spec}


def description(data, ns):
    row = field_map(data, ns, [
        ("id", "Id", "str"), ("name", "Name", "str"),
        ("description_text", "DescriptionText", "str"), ("subtype", "Subtype", "str"),
        ("subsystem", "Subsystem", "str")])
    row["rel_ids"] = [None if structure(x) is None else {"referenced_object_type": attr(x, "objecttype"),
                       "referenced_id": scalar(x)} for x in children(data, "Rel_Ids", ns)]
    loc = structure(one(data, "Location", ns))
    if loc is None:
        row["location"] = None
    else:
        road = structure(one(loc, "roaddescription", ns))
        coords = structure(one(loc, "co_description", ns))
        row["location"] = {
            "road_description": None if road is None else field_map(road, ns, [
                ("road", "road", "str"), ("nr_from", "nr_from", "int"),
                ("nr_to", "nr_to", "int"), ("roadinfo", "roadinfo", "bool"),
                ("direction", "direction", "str")]),
            "co_description": None if coords is None else {
                "projection_category": attr(coords, "projectionCategory"),
                "projection_type": attr(coords, "projectionType"),
                "coordinates": [None if structure(c) is None else field_map(c, ns, [("x", "x", "double"), ("y", "y", "double")])
                                for c in children(coords, "co", ns)]}}
    row["service_infos"] = []
    for item in children(data, "ServiceInfo", ns):
        if structure(item) is None:
            row["service_infos"].append(None)
            continue
        service = field_map(item, ns, [
            ("node_nr", "NodeNr", "int"), ("manufactor", "manufactor", "str"),
            ("category", "category", "str"), ("related_police", "related_police", "str"),
            ("service_control", "service_control", "str"), ("service_company", "service_company", "str"),
            ("contact_person", "contact_person", "str")])
        service["device_identification"] = value(structure(one(item, "device", ns)), "identification", ns)
        row["service_infos"].append(service)
    return row


def traffic_values(data, ns):
    return [None if structure(v) is None else {"vehicle": attr(v, "vehicle"), "count": attr(v, "count", "int"),
             "occ": attr(v, "occ", "double"), "speed": attr(v, "speed", "double")}
            for v in children(data, "value", ns)]


def business(table, data, ns):
    if table == "object_descriptions":
        return description(data, ns)
    if table == "operating_messages":
        m = structure(one(data, "Message", ns))
        result = field_map(m, ns, [
            ("system_type_id", "SystemTypeId", "str"), ("msg_def_id", "MsgDefId", "bigint"),
            ("category", "Category", "int"), ("degree", "Degree", "int"),
            ("sys_job_id", "SysJobId", "bigint"), ("message_timestamp", "Timestamp", "timestamp"),
            ("object_id", "ObjectId", "str"), ("referenced_obj_type", "ObjektType", "str")])
        result["hostname"] = value(data, "Hostname", ns)
        result["params"] = [None if structure(p) is None else field_map(p, ns, [("param_id", "ParamId", "str"),
                                                   ("param_value", "ParamValue", "str")])
                            for p in children(m, "Param", ns)]
        return result
    if table == "control_values":
        timeline = structure(one(data, "Timeline", ns))
        return {"id": value(data, "Id", ns), "param_value": value(data, "ParamValue", ns),
                "timeline_timestamp": value(timeline, "Timestamp", EXTERNAL + "global", "timestamp")}
    result = {"id": value(data, "id", ns)}
    if table == "intersection_feature":
        result["signal_plans"] = [None if structure(p) is None else field_map(p, ns, [
            ("plan_number", "number", "int"), ("name", "name", "str"),
            ("route_number", "routeNumber", "int"), ("cycle_time", "cycleTime", "int")])
                                  for p in children(data, "signalPlans", ns)]
        f = structure(one(data, "features", ns))
        result["features"] = None if f is None else {
            "has_individual_transport_priorization": attr(f, "hasIndividualTransportPriorization", "bool"),
            "has_local_traffic_actuation": attr(f, "hasLocalTrafficActuation", "bool"),
            "has_public_transport_priorization": attr(f, "hasPublicTransportPriorization", "bool"),
            "number_of_sub_intersections": attr(f, "numberOfSubIntersections", "int")}
    elif table == "intersection_status":
        result.update(field_map(data, ns, [
            ("event_time", "time", "timestamp"), ("state", "State", "str"),
            ("control_mode", "controlMode", "int"), ("intersection_on_off", "intersectionOnOff", "int"),
            ("signal_program", "signalProgram", "int"), ("pt_on_off", "ptOnOff", "int"),
            ("it_on_off", "itOnOff", "int"), ("ta_on_off", "taOnOff", "int"),
            ("sub_intersection_on_off", "subIntersectionOnOff", "int"), ("priority", "priority", "int")]))
        result["fault_statuses"] = [None if structure(f) is None else field_map(f, ns, [("text_nr", "TextNr", "int"),
                                                       ("params", "Params", "str")])
                                    for f in children(data, "faultStatus", ns)]
    elif table == "intersection_predefined":
        result.update(field_map(data, ns, [("command_id", "command_id", "str"),
                                          ("command", "command", "str")]))
    else:
        timeline = structure(one(data, "timeline", ns))
        result["timeline_timestamp"] = value(timeline, "Timestamp", EXTERNAL + "global", "timestamp")
        result["timeline_cycle"] = value(timeline, "Cycle", EXTERNAL + "global", "int")
        if table == "sg_times":
            result["tx"] = value(data, "tx", ns, "int")
            result["sg_states"] = None
        elif table in ("traffic_detector_current_value", "traffic_detector_group_current_value"):
            result["detector_state"] = value(data, "detectorState", ns)
            result["values"] = traffic_values(data, ns)
            if table == "traffic_detector_group_current_value":
                all_values = [v for v in result["values"] if v is not None and v["vehicle"] == "all"]
                if len(all_values) > 1:
                    raise TransformError("multiple vehicle=all values cannot be flattened unambiguously")
                for key in ("count", "occ", "speed"):
                    result[key + "_all"] = all_values[0][key] if all_values else None
        elif table == "traffic_detector_group_calculated_value":
            result["los"] = value(data, "los", ns, "int")
            result["values"] = traffic_values(data, ns)
        elif table == "traffic_subsection_accumulated_value":
            result["los"] = value(data, "los", ns, "int")
            result["predicted_los"] = value(data, "predictedLos", ns, "int")
    return result


def datasets(root):
    if root.namespaceURI == OCIT and root.localName == "ds":
        return [root]
    if root.namespaceURI == OCIT and root.localName == "dataList":
        listing = root
    else:
        if root.namespaceURI in SOAP and root.localName == "Envelope":
            body = one(root, "Body", root.namespaceURI)
            if body is None:
                raise TransformError("SOAP Body missing")
            faults = children(body, "Fault", root.namespaceURI)
            if faults:
                raise TransformError("SOAP Fault: " + faults[0].toxml())
            responses = [c for c in body.childNodes if c.nodeType == Node.ELEMENT_NODE]
            if len(responses) != 1:
                raise TransformError("expected exactly one SOAP response")
            root = responses[0]
        if root.namespaceURI != OCIT or root.localName not in ("getResponse", "inquireAllResponse"):
            raise TransformError("expected OCIT getResponse, inquireAllResponse, dataList or ds")
        code = scalar(one(root, "errorCode", OCIT))
        if code is None or not re.fullmatch(r"[+-]?[0-9]+", code.strip()):
            raise TransformError("missing or invalid OCIT errorCode")
        if int(code) not in (0, 41):
            text = scalar(one(root, "errorText", OCIT))
            if text is None:
                text = scalar(one(root, "errorTxt", OCIT))
            raise TransformError("OCIT errorCode %s: %s" % (code.strip(), text or ""))
        listing = one(root, "dataList", OCIT)
        if listing is None:
            raise TransformError("OCIT response has no dataList")
    if structure(listing) is None:
        raise TransformError("dataList is nil, not an empty list")
    for c in listing.childNodes:
        if c.nodeType == Node.ELEMENT_NODE and (c.namespaceURI != OCIT or c.localName != "ds"):
            raise TransformError("unexpected dataList child: " + c.tagName)
        if c.nodeType in (Node.TEXT_NODE, Node.CDATA_SECTION_NODE) and c.data.strip():
            raise TransformError("unexpected text in dataList")
    return children(listing, "ds", OCIT)


def _transform(xml, object_type, namespace_repairs, processed_at):
    if object_type not in ROUTES:
        raise TransformError("unsupported object type: %s" % object_type)
    repairs = dict(namespace_repairs or {})
    for prefix, uri in repairs.items():
        if not isinstance(prefix, str) or not re.fullmatch(r"[A-Za-z_][\w.-]*", prefix) or prefix in ("xml", "xmlns"):
            raise TransformError("invalid repair prefix: %r" % prefix)
        if (not isinstance(uri, str) or not uri or
                any(not (c in "\t\n\r" or "\x20" <= c <= "\ud7ff" or
                         "\ue000" <= c <= "\ufffd" or "\U00010000" <= c <= "\U0010ffff")
                    for c in uri)):
            raise TransformError("repair URI must be nonempty and contain valid XML characters")
    if not isinstance(xml, (str, bytes)):
        raise TransformError("XML must be str or bytes")
    # DTDs are unnecessary for OCIT and can expand entities or access external data.
    probe = xml if isinstance(xml, bytes) else xml.encode("utf-8")
    if b"\x00" in probe or re.search(br"<!\s*(?:DOCTYPE|ENTITY)\b", probe, re.I):
        raise TransformError("DTD, entity declarations and NUL/UTF-16 input are not supported")
    try:
        doc = minidom.parseString(xml)
    except (ExpatError, ValueError, UnicodeError, LookupError) as exc:
        raise TransformError("invalid XML: %s" % exc) from exc
    table, domain, typename = ROUTES[object_type]
    ns = EXTERNAL + domain
    if processed_at is None:
        processed_at = dt.datetime.now(dt.timezone.utc)
    if not isinstance(processed_at, dt.datetime) or processed_at.tzinfo is None or processed_at.utcoffset() is None:
        raise TransformError("processed_at must be a timezone-aware datetime")
    stamp = processed_at.astimezone(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    root = doc.documentElement
    is_response = ((root.namespaceURI in SOAP and root.localName == "Envelope") or
                   (root.namespaceURI == OCIT and root.localName in ("getResponse", "inquireAllResponse")))
    records = []
    for ds in datasets(doc.documentElement):
        if structure(ds) is None:
            raise TransformError("nil ds is not a dataset")
        state = value(ds, "objectState", OCIT)
        ident = value(structure(one(ds, "identifier", OCIT)), "ident", OCIT)
        if state == "deleted" and (ident is None or not ident.strip()):
            raise TransformError("deleted dataset requires a nonempty ident")
        data = one(ds, "data", OCIT)
        payload = structure(data)
        if payload is None and state != "deleted":
            raise TransformError("missing or nil data for non-deleted dataset")
        if payload is not None and not payload.hasAttributeNS(XSI, "type"):
            raise TransformError("data has no xsi:type; cannot validate object type")
        validate_types(ds, (ns, typename), repairs)
        row = {"ident": ident,
               "tstore": value(ds, "tstore", OCIT, "timestamp"),
               "object_state": state}
        if table in ("object_descriptions", "operating_messages"):
            row["ocit_object_type"] = object_type
        row.update(business(table, payload, ns))
        row["processed_at_utc"] = stamp
        records.append(row)
    return {table: records}, is_response


def transform(xml, object_type, *, namespace_repairs=None, processed_at=None):
    """Return {table_name: [records]}; response metadata is not returned.

    Use transform_envelope to retain the complete original input. Namespace repairs
    map xsi:type prefixes to authoritative URIs; processed_at must be aware.
    """
    return _transform(xml, object_type, namespace_repairs, processed_at)[0]


def transform_envelope(xml, object_type, *, namespace_repairs=None, processed_at=None):
    """Return tables and the exact original input (str or bytes), including metadata."""
    tables, is_response = _transform(xml, object_type, namespace_repairs, processed_at)
    return {"tables": tables, "original_xml": xml, "is_response": is_response}


class OcitIcebergTransform(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = "Convert supported OCIT-C XML responses into Iceberg JSON records."
        tags = ["ocit", "xml", "iceberg", "json"]

    def __init__(self, **kwargs):
        super().__init__()
        self.relationships = [
            Relationship(name="success", description="Converted JSON records."),
            Relationship(name="unmatched", description="FlowFiles with an unsupported object-type."),
        ]

    def getRelationships(self):
        return self.relationships

    def transform(self, context, flowfile):
        try:
            object_type = flowfile.getAttribute("object-type")
            if not isinstance(object_type, str) or not object_type.strip():
                raise TransformError("missing object-type")
            if object_type not in ROUTES:
                return FlowFileTransformResult(relationship="unmatched")
            xml = bytes(flowfile.getContentsAsBytes())
            result = transform_envelope(xml, object_type)
            table, records = next(iter(result["tables"].items()))
            contents = json.dumps(records, ensure_ascii=False, allow_nan=False).encode("utf-8")
            return FlowFileTransformResult(
                relationship="success", contents=contents,
                attributes={"mime.type": "application/json", "iceberg.table": table},
            )
        except (TransformError, ValueError, TypeError) as exc:
            # Do not log XML, cursor values, or other potentially sensitive data.
            if self.logger:
                self.logger.error("OCIT transformation failed (%s)" % type(exc).__name__)
            return FlowFileTransformResult(relationship="failure")
