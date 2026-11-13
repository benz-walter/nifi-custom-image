"""NiFi 2 processor for one OCIT-C synchronization request per FlowFile."""

import json
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union
import xml.etree.ElementTree as ET

import requests
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

# The synchronization client is embedded so NiFi can deploy this file alone.
# Keep it consistent with the independently runnable NiFi_logic.py.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("OCIT-C-Client")




# Fehlercodes laut OCIT-C protocol.xsd (Kapitel 2.7)
ERROR_NO_ERROR = 0
ERROR_BUFFER_OVERFLOW = 2          # Position zu alt -> Resync mit inquireAll
ERROR_SERVER_UNAVAILABLE = 20      # Server kurzzeitig überlastet (>60s) -> Alte Position behalten
ERROR_TIME_RANGE_COMPLETE = 41     # Kein Fehler
ERROR_MISSING_DATASETS = 42        # Datensätze fehlen -> Resync mit inquireAll




@dataclass
class OcitSyncResult:
    """Rückgabestruktur eines Abfragezyklus."""
    action_performed: str          # "inquireAll" (Kaltstart), "get" (Delta), "resync_inquireAll" (Fallback)
    is_full_snapshot: bool         # True wenn Vollabzug (DB-Replace/Full-Sync), False wenn Delta (DB-Upsert)
    new_position: Optional[str]    # Neue Position für den nächsten Aufruf
    new_last_start: Optional[str]  # Neuer lastStart-Zeitstempel des Servers
    error_code: int                # OCIT-C errorCode (0 = OK)
    error_text: str                # OCIT-C errorText
    records: List[Dict[str, Any]]  # Extrahierte Datenobjekte aus <dataList><ds>...</ds></dataList>
    raw_data_list_xml: str         # Optional: Das rohe <dataList> XML für direkte Weiterleitung als NiFi FlowFile
    raw_response_xml: Union[bytes, str] = b""  # Originalantwort mit allen Namespace-Deklarationen




class OcitCClient:
    """Kapselt die SOAP-Kommunikation und Positions-/Resynchronisationslogik für OCIT-C."""


    def __init__(
            self,
            url: str,
            username: str,
            password: str,
            soap_namespace: str = "http://odg_und_partner/OCIT_C",
            timeout: int = 65,
            verify_ssl: bool = True,
            session: Optional[requests.Session] = None,
            strict_response: bool = False
    ):
        self.url = url
        self.username = username
        self.password = password
        self.soap_ns = soap_namespace
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.http = session if session is not None else requests
        self.strict_response = strict_response


    # -------------------------------------------------------------------------
    # 1. Hauptmethode: Entscheidungslogik (Kaltstart vs. get vs. Resync)
    # -------------------------------------------------------------------------
    def fetch_and_sync(
            self,
            object_type: str,
            last_position: Optional[str] = None,
            last_start: Optional[str] = None
    ) -> OcitSyncResult:
        """
        Steuert den kompletten OCIT-C Abfragezyklus für einen Objekttyp:
        1. Kaltstart (keine position oder kein lastStart) -> inquireAll
        2. Normalbetrieb -> get(position)
        3. Falls lastStart geändert oder errorCode 2/42 (Position veraltet) -> Resync mit inquireAll
        """
        # Fall 1: Kaltstart (Initialsynchronisation)
        if not last_position or not last_start:
            logger.info(
                f"[{object_type}] Kaltstart erkannt (position={last_position}, "
                f"lastStart={last_start}). Rufe 'inquireAll' auf..."
            )
            resp = self._call_inquire_all(object_type)
            return self._build_result(
                action="inquireAll",
                is_snapshot=True,
                parsed_resp=resp,
                fallback_pos=last_position,
                fallback_last_start=last_start
            )


        # Fall 2: Reguläre Delta-Abfrage mit 'get'
        logger.info(
            f"[{object_type}] Starte Delta-Abfrage 'get' ab position='{last_position}' "
            f"(erwarteter lastStart='{last_start}')..."
        )
        get_resp = self._call_get(object_type, last_position)
        err_code = get_resp["error_code"]
        srv_last_start = get_resp["last_start"]


        # Prüfen auf temporäre Server-Überlastung (ErrorCode 20)
        if err_code == ERROR_SERVER_UNAVAILABLE:
            logger.warning(
                f"[{object_type}] Server meldet errorCode=20 (shortly unavailable). "
                f"Behalte alte position='{last_position}' für nächsten Zyklus bei."
            )
            return OcitSyncResult(
                action_performed="get_retry_later",
                is_full_snapshot=False,
                new_position=last_position,
                new_last_start=last_start,
                error_code=err_code,
                error_text=get_resp["error_text"],
                records=[],
                raw_data_list_xml=""
            )

        if self.strict_response and err_code not in (
                ERROR_NO_ERROR, ERROR_TIME_RANGE_COMPLETE, ERROR_BUFFER_OVERFLOW, ERROR_MISSING_DATASETS):
            return self._build_result(
                action="get", is_snapshot=False, parsed_resp=get_resp,
                fallback_pos=last_position, fallback_last_start=last_start
            )


        # Prüfen, ob eine Resynchronisation mit 'inquireAll' zwingend erforderlich ist
        server_restarted = bool(srv_last_start and srv_last_start != last_start)
        position_outdated = err_code in (ERROR_BUFFER_OVERFLOW, ERROR_MISSING_DATASETS)


        if server_restarted or position_outdated:
            reason = (
                f"lastStart geändert ('{last_start}' -> '{srv_last_start}')"
                if server_restarted
                else f"Position veraltet / Ringpuffer-Überlauf (errorCode={err_code}: {get_resp['error_text']})"
            )
            logger.warning(
                f"[{object_type}] Resynchronisation erforderlich! Grund: {reason}. "
                f"Verwerfe getResponse und führe 'inquireAll' aus..."
            )
            resync_resp = self._call_inquire_all(object_type)
            return self._build_result(
                action="resync_inquireAll",
                is_snapshot=True,
                parsed_resp=resync_resp,
                fallback_pos=None,
                fallback_last_start=None
            )


        # Fall 3: 'get' war erfolgreich und synchron
        logger.info(
            f"[{object_type}] 'get' erfolgreich. {len(get_resp['records'])} Änderungen erhalten. "
            f"Neue position='{get_resp['position']}'."
        )
        return self._build_result(
            action="get",
            is_snapshot=False,
            parsed_resp=get_resp,
            fallback_pos=last_position,
            fallback_last_start=last_start
        )


    # -------------------------------------------------------------------------
    # 2. SOAP Request Builder & Aufrufe (inquireAll und get)
    # -------------------------------------------------------------------------
    def _call_inquire_all(self, object_type: str) -> Dict[str, Any]:
        """Baut den SOAP-Request für inquireAll auf und sendet ihn."""
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ocit="{self.soap_ns}">
   <soapenv:Header/>
   <soapenv:Body>
      <ocit:inquireAll>
         <ocit:userName>{self._escape_xml(self.username)}</ocit:userName>
         <ocit:passWord>{self._escape_xml(self.password)}</ocit:passWord>
         <ocit:objectType>{self._escape_xml(object_type)}</ocit:objectType>
      </ocit:inquireAll>
   </soapenv:Body>
</soapenv:Envelope>"""
        xml_response = self._send_soap_request(soap_body, soap_action="inquireAll")
        return self._parse_ocit_response(xml_response, expected_response="inquireAllResponse")






    def _call_get(self, object_type: str, position: str) -> Dict[str, Any]:
        """Baut den SOAP-Request für get (mit position) auf und sendet ihn."""
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ocit="{self.soap_ns}">
   <soapenv:Header/>
   <soapenv:Body>
      <ocit:get>
         <ocit:userName>{self._escape_xml(self.username)}</ocit:userName>
         <ocit:passWord>{self._escape_xml(self.password)}</ocit:passWord>
         <ocit:objectType>{self._escape_xml(object_type)}</ocit:objectType>
         <ocit:position>{self._escape_xml(position)}</ocit:position>
      </ocit:get>
   </soapenv:Body>
</soapenv:Envelope>"""
        xml_response = self._send_soap_request(soap_body, soap_action="get")
        return self._parse_ocit_response(xml_response, expected_response="getResponse")


    def _send_soap_request(self, xml_payload: str, soap_action: str) -> bytes:
        """Sendet den HTTP POST an den OCIT-C SOAP-Endpunkt."""
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f'"{soap_action}"'
        }
        proxy_options = {}
        if self.http is not requests and self.http.proxies:
            proxy_options["proxies"] = self.http.proxies
        response = self.http.post(
            self.url,
            data=xml_payload.encode("utf-8"),
            headers=headers,
            timeout=self.timeout,
            verify=self.verify_ssl,
            **proxy_options
        )
        # Bei SOAP-Fault (HTTP 500) trotzdem Details loggen
        if response.status_code >= 400:
            raise RuntimeError(
                f"HTTP {response.status_code} Fehler vom OCIT-C Server bei '{soap_action}':\n"
                f"{response.text[:1000]}"
            )
        return response.content


    # -------------------------------------------------------------------------
    # 3. Namespace-agnostischer XML Parser für inquireAllResponse / getResponse
    # -------------------------------------------------------------------------
    def _parse_ocit_response(
            self, xml_text: Union[bytes, str], expected_response: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Parst inquireAllResponse bzw. getResponse unabhängig von XML-Namespace-Präfixen.
        Extrahiert: lastStart, errorCode, errorText, position und dataList/ds.
        """
        root = ET.fromstring(xml_text)


        # Prüfen, ob ein SOAP-Fault vorliegt
        fault = self._find_first_by_localname(root, "Fault")
        if fault is not None:
            fault_str = self._get_text_by_localname(fault, "faultstring") or ET.tostring(fault, encoding="unicode")
            raise RuntimeError(f"SOAP Fault erhalten: {fault_str}")


        if self.strict_response:
            soap_namespaces = (
                "http://schemas.xmlsoap.org/soap/envelope/",
                "http://www.w3.org/2003/05/soap-envelope",
            )
            bodies = [child for child in root if child.tag in
                      tuple(f"{{{ns}}}Body" for ns in soap_namespaces)]
            if root.tag not in tuple(f"{{{ns}}}Envelope" for ns in soap_namespaces) or len(bodies) != 1:
                raise ValueError("Invalid SOAP envelope")
            responses = [child for child in bodies[0] if child.tag == f"{{{self.soap_ns}}}{expected_response}"]
            if len(responses) != 1 or len(bodies[0]) != 1:
                raise ValueError("Unexpected SOAP response")
            response_root = responses[0]

            def response_element(name):
                matches = [child for child in response_root
                           if child.tag == f"{{{self.soap_ns}}}{name}"]
                if len(matches) > 1:
                    raise ValueError(f"Duplicate response element: {name}")
                return matches[0] if matches else None

            def response_text(name):
                element = response_element(name)
                return element.text.strip() if element is not None and element.text else None

            last_start = response_text("lastStart")
            error_code_str = response_text("errorCode")
            if error_code_str is None:
                raise ValueError("Missing response errorCode")
            error_text = response_text("errorText") or response_text("errorTxt") or ""
            position = response_text("position")
            data_list_elem = response_element("dataList")
        else:
            last_start = self._get_text_by_localname(root, "lastStart", case_insensitive=True)
            error_code_str = self._get_text_by_localname(root, "errorCode", case_insensitive=True) or "0"
            error_text = (
                    self._get_text_by_localname(root, "errorText", case_insensitive=True)
                    or self._get_text_by_localname(root, "errorTxt", case_insensitive=True)
                    or ""
            )
            position = self._get_text_by_localname(root, "position", case_insensitive=True)
            data_list_elem = self._find_first_by_localname(root, "dataList", case_insensitive=True)


        # Datenliste (<dataList>) extrahieren
        records = []
        raw_data_list_xml = ""


        if data_list_elem is not None:
            raw_data_list_xml = ET.tostring(data_list_elem, encoding="unicode")
            # Jedes <ds>-Element (Dataset) in der <dataList> auslesen
            for ds in data_list_elem:
                if self._localname(ds.tag).lower() == "ds":
                    ident = self._get_text_by_localname(ds, "ident", case_insensitive=True)
                    tstore = self._get_text_by_localname(ds, "tstore", case_insensitive=True)
                    obj_state = self._get_text_by_localname(ds, "objectState", case_insensitive=True)
                    data_elem = self._find_first_by_localname(ds, "data", case_insensitive=True)
                    data_xml = ET.tostring(data_elem, encoding="unicode") if data_elem is not None else ""


                    records.append({
                        "ident": ident,
                        "tstore": tstore,
                        "objectState": obj_state,
                        "data_xml": data_xml
                    })


        return {
            "last_start": last_start,
            "error_code": int(error_code_str),
            "error_text": error_text,
            "position": position,
            "records": records,
            "raw_data_list_xml": raw_data_list_xml,
            "raw_response_xml": xml_text
        }


    def _build_result(
            self,
            action: str,
            is_snapshot: bool,
            parsed_resp: Dict[str, Any],
            fallback_pos: Optional[str],
            fallback_last_start: Optional[str]
    ) -> OcitSyncResult:
        err_code = parsed_resp["error_code"]
        # Nur wenn kein kritischer Fehler vorliegt, übernehmen wir die neue Position
        valid_error_codes = (ERROR_NO_ERROR, ERROR_TIME_RANGE_COMPLETE)
        new_pos = parsed_resp["position"] if err_code in valid_error_codes else fallback_pos
        new_ls = parsed_resp["last_start"] if err_code in valid_error_codes else fallback_last_start


        return OcitSyncResult(
            action_performed=action,
            is_full_snapshot=is_snapshot,
            new_position=new_pos,
            new_last_start=new_ls,
            error_code=err_code,
            error_text=parsed_resp["error_text"],
            records=parsed_resp["records"],
            raw_data_list_xml=parsed_resp["raw_data_list_xml"],
            raw_response_xml=parsed_resp["raw_response_xml"]
        )


    # -------------------------------------------------------------------------
    # Kleine XML-Hilfsfunktionen
    # -------------------------------------------------------------------------
    @staticmethod
    def _localname(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag


    def _find_first_by_localname(
            self, root: ET.Element, target: str, case_insensitive: bool = False
    ) -> Optional[ET.Element]:
        t = target.lower() if case_insensitive else target
        for elem in root.iter():
            name = self._localname(elem.tag)
            if (name.lower() if case_insensitive else name) == t:
                return elem
        return None


    def _get_text_by_localname(
            self, root: ET.Element, target: str, case_insensitive: bool = False
    ) -> Optional[str]:
        elem = self._find_first_by_localname(root, target, case_insensitive)
        return elem.text.strip() if (elem is not None and elem.text) else None


    @staticmethod
    def _escape_xml(val: str) -> str:
        return (
            val.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
        )


class InputError(ValueError):
    pass


def parse_input(content):
    try:
        data = json.loads(bytes(content).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InputError("FlowFile muss ein UTF-8-JSON-Objekt enthalten") from exc
    if not isinstance(data, dict):
        raise InputError("FlowFile muss ein JSON-Objekt enthalten")
    object_type = data.get("object-type")
    if not isinstance(object_type, str) or not object_type.strip():
        raise InputError("object-type muss eine nichtleere Zeichenkette sein")
    values = []
    for key in ("position", "last-start"):
        value = data.get(key)
        if value is not None and not isinstance(value, str):
            raise InputError(f"{key} muss eine Zeichenkette oder null sein")
        values.append(value or None)
    return object_type.strip(), *values


class OcitCProcessor(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Queries one OCIT-C object type from JSON input and outputs the original "
            "SOAP XML response with synchronization attributes."
        )
        tags = ["ocit", "soap", "xml", "traffic"]
        dependencies = ["requests==2.32.5"]

    def __init__(self, **kwargs):
        super().__init__()
        self.descriptors = [
            PropertyDescriptor(
                name="URL", description="OCIT-C SOAP endpoint URL.",
                required=True, validators=[StandardValidators.URL_VALIDATOR],
            ),
            PropertyDescriptor(
                name="User", description="OCIT-C user name.",
                required=True, validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            ),
            PropertyDescriptor(
                name="Password", description="OCIT-C password.",
                required=True, sensitive=True,
                validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            ),
            PropertyDescriptor(
                name="Output", description="Filename attribute for the XML response.",
                required=True, default_value="ocit_response.xml",
                validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            ),
            PropertyDescriptor(
                name="Proxy URL", description="Optional HTTP or HTTPS proxy URL.",
                required=False, sensitive=True,
                validators=[StandardValidators.URL_VALIDATOR],
            ),
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        try:
            object_type, position, last_start = parse_input(flowfile.getContentsAsBytes())
            with requests.Session() as session:
                proxy = context.getProperty("Proxy URL").getValue()
                if proxy:
                    session.proxies = {"http": proxy, "https": proxy}
                client = OcitCClient(
                    url=context.getProperty("URL").getValue(),
                    username=context.getProperty("User").getValue(),
                    password=context.getProperty("Password").getValue(),
                    session=session,
                    strict_response=True,
                )
                result = client.fetch_and_sync(object_type, position, last_start)
            if result.error_code not in (ERROR_NO_ERROR, ERROR_TIME_RANGE_COMPLETE):
                raise InputError(f"OCIT-Server meldet Fehlercode {result.error_code}")
            if not result.raw_response_xml or not result.raw_data_list_xml:
                raise InputError("SOAP-Antwort oder dataList fehlt")
            xml = result.raw_response_xml
            if isinstance(xml, str):
                xml = xml.encode("utf-8")
            return FlowFileTransformResult(
                relationship="success",
                contents=xml,
                attributes={
                    "filename": context.getProperty("Output").getValue(),
                    "mime.type": "text/xml",
                    "object-type": object_type,
                    "position": result.new_position or "",
                    "last-start": result.new_last_start or "",
                    "ocit.action": result.action_performed,
                    "ocit.is-full-snapshot": str(result.is_full_snapshot).lower(),
                },
            )
        except InputError as exc:
            self.logger.error(f"OCIT-Verarbeitung fehlgeschlagen: {exc}")
        except (requests.RequestException, RuntimeError, ET.ParseError, ValueError) as exc:
            # HTTP errors and SOAP faults can contain credentials or server data.
            self.logger.error(f"OCIT-Abfrage fehlgeschlagen ({type(exc).__name__})")
        return FlowFileTransformResult(relationship="failure")
