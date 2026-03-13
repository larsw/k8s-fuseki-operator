#!/usr/bin/env python3

import base64
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


EXPECTED_AUTH = "Basic " + base64.b64encode(b"admin:secret").decode("ascii")

POLICIES = [
    {
        "service": "fuseki-default",
        "isEnabled": True,
        "policyType": 0,
        "resources": {
            "dataset": {"values": ["smoke"], "isRecursive": False, "isExcludes": False},
            "endpoint": {"values": ["query"], "isRecursive": False, "isExcludes": False},
        },
        "conditions": [
            {"type": "scriptConditionEvaluator", "values": ["RESOURCE.dataset == 'smoke'"]}
        ],
        "policyItems": [
            {
                "groups": ["ops"],
                "roles": ["analyst"],
                "accesses": [{"type": "query", "isAllowed": True}],
                "conditions": [
                    {
                        "type": "scriptConditionEvaluator",
                        "values": ["USER.department == 'data' && REQUEST.method == 'GET'"]
                    }
                ],
            }
        ],
    }
    ,
    {
        "service": "fuseki-default",
        "isEnabled": True,
        "policyType": 0,
        "resources": {
            "dataset": {"values": ["roles"], "isRecursive": False, "isExcludes": False},
            "endpoint": {"values": ["query"], "isRecursive": False, "isExcludes": False},
        },
        "policyItems": [
            {
                "roles": ["analyst"],
                "accesses": [{"type": "query", "isAllowed": True}],
                "conditions": [
                    {
                        "type": "scriptConditionEvaluator",
                        "values": ["REQUEST.method == 'GET'"]
                    }
                ],
            }
        ],
    }
]

USER_ROLES = {
    "alice": ["analyst"],
    "bob": [],
    "carol": ["analyst"],
}


class MockRangerHandler(BaseHTTPRequestHandler):
    server_version = "MockRanger/1.0"

    def do_GET(self):
        if self.path == "/":
            self._write_json(200, {"status": "ok"})
            return

        if self.headers.get("Authorization") != EXPECTED_AUTH:
            self._write_json(401, {"error": "unauthorized"})
            return

        if self.path.startswith("/public/v2/api/service/fuseki-default/policy"):
            self._write_json(200, POLICIES)
            return

        if self.path.startswith("/public/v2/api/roles/user/"):
            user_name = self.path.split("/public/v2/api/roles/user/", 1)[1].split("?", 1)[0]
            self._write_json(200, USER_ROLES.get(user_name, []))
            return

        self._write_json(404, {"error": "not found"})

    def log_message(self, format, *args):
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

    def _write_json(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18181
    server = ThreadingHTTPServer(("0.0.0.0", port), MockRangerHandler)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())