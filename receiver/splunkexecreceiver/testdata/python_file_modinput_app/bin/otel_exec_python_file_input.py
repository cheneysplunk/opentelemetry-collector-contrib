#!/usr/bin/env python3
"""Minimal file-reading modular input with Splunk-style conf REST CRUD."""

import base64
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


SCHEME_XML = """<scheme>
  <title>OTel Exec Python File Input</title>
  <description>Minimal Python modular input fixture for splunkexecreceiver tests.</description>
  <use_external_validation>false</use_external_validation>
  <streaming_mode>xml</streaming_mode>
</scheme>
"""


def parse_input(stream):
    root = ET.parse(stream).getroot()
    metadata = {}
    inputs = {}
    for child in root:
        if child.tag == "configuration":
            for stanza in child.findall("stanza"):
                name = stanza.get("name") or ""
                params = {}
                for param in stanza.findall("param"):
                    param_name = param.get("name")
                    if param_name:
                        params[param_name] = (param.text or "").strip()
                inputs[name] = params
        else:
            metadata[child.tag] = (child.text or "").strip()
    return metadata, inputs


def write_event(stanza, params, data):
    event = ET.Element("event", {"unbroken": "1"})
    if stanza:
        event.set("stanza", stanza)
    for tag in ("source", "sourcetype", "index", "host"):
        if params.get(tag):
            ET.SubElement(event, tag).text = params[tag]
    ET.SubElement(event, "time").text = "%.3f" % time.time()
    ET.SubElement(event, "data").text = data
    ET.SubElement(event, "done")
    sys.stdout.write(ET.tostring(event, encoding="unicode"))


def request(method, url, form=None, token="", username="", password=""):
    data = None
    headers = {"User-Agent": "otel_exec_python_file_input/1.0"}
    if form is not None:
        data = urllib.parse.urlencode(form).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = "Splunk %s" % token
    elif username and password:
        raw = ("%s:%s" % (username, password)).encode("utf-8")
        headers["Authorization"] = "Basic %s" % base64.b64encode(raw).decode("ascii")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.read().decode("utf-8", "replace")


def login(root_url, username, password):
    body = request(
        "POST",
        root_url + "/services/auth/login",
        form={"username": username, "password": password},
    )
    root = ET.fromstring(body)
    session_key = root.find(".//sessionKey")
    if session_key is None or not session_key.text:
        raise RuntimeError("login response did not contain sessionKey")
    return session_key.text.strip()


def rest_crud(metadata, params):
    root_url = (params.get("rest_url") or "").rstrip("/")
    if not root_url:
        return "otel_exec_python_file_input_rest_status=skipped"

    username = params.get("rest_username", "")
    password = params.get("rest_password", "")
    token = params.get("rest_token") or metadata.get("session_key", "")
    if username and password:
        token = login(root_url, username, password)
        username = ""
        password = ""

    owner = urllib.parse.quote(params.get("rest_owner") or "nobody", safe="")
    app = urllib.parse.quote(params.get("rest_app") or "system", safe="")
    conf = urllib.parse.quote(params.get("rest_conf_name") or "otel_exec_python_file_input", safe="")
    stanza = urllib.parse.quote(params.get("rest_stanza") or "otel_exec_python_file_input", safe="")
    key = params.get("rest_key") or "marker"
    value = params.get("rest_value") or "ok"

    conf_url = "%s/servicesNS/%s/%s/configs/conf-%s" % (root_url, owner, app, conf)
    stanza_url = "%s/%s" % (conf_url, stanza)

    request("POST", conf_url + "?output_mode=json", form={"name": params.get("rest_stanza") or "otel_exec_python_file_input", key: value}, token=token, username=username, password=password)
    request("GET", stanza_url + "?output_mode=json", token=token, username=username, password=password)
    request("POST", stanza_url + "?output_mode=json", form={key: value + "_updated"}, token=token, username=username, password=password)
    request("DELETE", stanza_url + "?output_mode=json", token=token, username=username, password=password)

    return "otel_exec_python_file_input_rest_status=ok conf=%s stanza=%s python_executable=%s" % (
        urllib.parse.unquote(conf),
        urllib.parse.unquote(stanza),
        sys.executable,
    )


def main(argv):
    if len(argv) > 1 and argv[1] == "--scheme":
        sys.stdout.write(SCHEME_XML)
        return

    metadata, inputs = parse_input(sys.stdin)
    sys.stdout.write("<stream>")
    try:
        for stanza, params in inputs.items():
            with open(params["file_path"], "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    write_event(stanza, params, line.rstrip("\r\n"))
            write_event(stanza, params, rest_crud(metadata, params))
    finally:
        sys.stdout.write("</stream>")
        sys.stdout.flush()


if __name__ == "__main__":
    main(sys.argv)
