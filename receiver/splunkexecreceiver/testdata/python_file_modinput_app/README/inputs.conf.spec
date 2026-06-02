[otel_exec_python_file_input://<name>]
* Minimal Python modular input used by splunkexecreceiver integration tests.
* Reads a file line-by-line and optionally calls a Splunk-style conf REST API.

python.required = <string>
* Optional Splunk Python requirement used by native ExecProcessor for .py files.
* Example: 3.9.

rest_url = <url>
* Optional Splunk management root URL.
* When set, the input calls /services/auth/login and /servicesNS/.../configs/conf-*.

rest_username = <string>
* Optional username for /services/auth/login.

rest_password = <string>
* Optional password for /services/auth/login.

rest_owner = <string>
* servicesNS owner.
* Defaults to nobody.

rest_app = <string>
* servicesNS app.
* Defaults to system.

rest_conf_name = <string>
* Conf file name without .conf.
* Defaults to otel_exec_python_file_input.

rest_stanza = <string>
* Stanza name used for REST CRUD.
* Defaults to otel_exec_python_file_input.

rest_key = <string>
* Key used for REST CRUD.
* Defaults to marker.

rest_value = <string>
* Value used for REST CRUD.
* Defaults to ok.

file_path = <path>
* Absolute path to the file to read.
