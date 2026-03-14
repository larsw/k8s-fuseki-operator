#!/usr/bin/env python3

import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


RANGER_ADMIN_URL = os.environ.get("RANGER_ADMIN_URL", "http://127.0.0.1:16080/service").rstrip("/")
RANGER_USERNAME = os.environ.get("RANGER_USERNAME", "admin")
RANGER_PASSWORD = os.environ.get("RANGER_PASSWORD", "rangerR0cks!")

SERVICE_DEF_NAME = os.environ.get("RANGER_SERVICE_DEF_NAME", "fuseki-smoke")
SERVICE_NAME = os.environ.get("RANGER_SERVICE_NAME", "fuseki-smoke")
ROLE_NAME = os.environ.get("RANGER_ROLE_NAME", "fuseki-analyst")
GROUP_NAME = os.environ.get("RANGER_GROUP_NAME", "fuseki-ops")

USERS = (
    {"name": "alice", "firstName": "Alice", "lastName": "Smoke", "emailAddress": "alice@example.test"},
    {"name": "bob", "firstName": "Bob", "lastName": "Smoke", "emailAddress": "bob@example.test"},
    {"name": "carol", "firstName": "Carol", "lastName": "Smoke", "emailAddress": "carol@example.test"},
)


class RangerApiError(RuntimeError):
    def __init__(self, status_code, body):
        super().__init__(f"HTTP {status_code}: {body}")
        self.status_code = status_code
        self.body = body


class RangerApi:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip("/")
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"

    def request(self, method, path, payload=None, params=None, expected_statuses=(200,)):
        url = self._url(path, params)
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(url, data=body, method=method)
        request.add_header("Accept", "application/json")
        request.add_header("Authorization", self.auth_header)
        if body is not None:
            request.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw_body = response.read().decode("utf-8")
                if response.status not in expected_statuses:
                    raise RangerApiError(response.status, raw_body)
                return self._decode_json(raw_body)
        except urllib.error.HTTPError as exc:
            raw_body = exc.read().decode("utf-8")
            if exc.code in expected_statuses:
                return self._decode_json(raw_body)
            raise RangerApiError(exc.code, raw_body) from exc

    def get_optional(self, path, params=None):
        try:
            return self.request("GET", path, params=params)
        except RangerApiError as exc:
            if exc.status_code == 404:
                return None
            raise

    def _url(self, path, params):
        url = f"{self.base_url}/{path.lstrip('/')}"
        if params:
            query = urllib.parse.urlencode(params)
            return f"{url}?{query}"
        return url

    @staticmethod
    def _decode_json(raw_body):
        if not raw_body.strip():
            return None
        return json.loads(raw_body)


def extract_items(payload, *keys):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    value = payload.get("list")
    if isinstance(value, list):
        return value
    return []


def find_named_item(items, name):
    for item in items:
        candidate = item.get("name") or item.get("loginId")
        if candidate == name:
            return item
    return None


def quoted(value):
    return urllib.parse.quote(value, safe="")


def service_def_payload():
    return {
        "name": SERVICE_DEF_NAME,
        "displayName": "Fuseki Smoke",
        "label": "Fuseki Smoke",
        "description": "Fuseki integration smoke service definition",
        "options": {"enableDenyAndExceptionsInPolicies": "true"},
        "configs": [
            {
                "itemId": 1,
                "name": "endpoint",
                "type": "string",
                "mandatory": False,
                "label": "Endpoint",
            }
        ],
        "resources": [
            {
                "itemId": 1,
                "name": "dataset",
                "type": "string",
                "level": 1,
                "mandatory": True,
                "isValidLeaf": False,
                "label": "Dataset",
            },
            {
                "itemId": 2,
                "name": "endpoint",
                "type": "string",
                "level": 2,
                "parent": "dataset",
                "mandatory": False,
                "isValidLeaf": True,
                "label": "Endpoint",
            },
            {
                "itemId": 3,
                "name": "graph",
                "type": "string",
                "level": 2,
                "parent": "dataset",
                "mandatory": False,
                "isValidLeaf": True,
                "label": "Graph",
            },
            {
                "itemId": 4,
                "name": "path",
                "type": "string",
                "level": 2,
                "parent": "dataset",
                "mandatory": False,
                "isValidLeaf": True,
                "label": "Path",
            },
        ],
        "accessTypes": [
            {"itemId": 1, "name": "query", "label": "query"},
            {"itemId": 2, "name": "read", "label": "read"},
            {"itemId": 3, "name": "update", "label": "update"},
            {"itemId": 4, "name": "write", "label": "write"},
        ],
        "policyConditions": [
            {
                "itemId": 1,
                "name": "expression",
                "evaluator": "org.apache.ranger.plugin.conditionevaluator.RangerScriptConditionEvaluator",
                "evaluatorOptions": {"engineName": "JavaScript", "ui.isMultiline": "true"},
                "uiHint": '{ "isMultiline":true }',
                "label": "Expression",
                "description": "Evaluate a request expression.",
            }
        ],
        "contextEnrichers": [],
        "enums": [],
    }


def service_def_has_expected_hierarchy(service_def):
    resources = {resource.get("name"): resource for resource in service_def.get("resources", [])}
    dataset = resources.get("dataset")
    endpoint = resources.get("endpoint")
    graph = resources.get("graph")
    path = resources.get("path")

    if not dataset or not endpoint or not graph or not path:
        return False

    return (
        dataset.get("level") == 1
        and dataset.get("isValidLeaf") is False
        and endpoint.get("level") == 2
        and endpoint.get("parent") == "dataset"
        and graph.get("level") == 2
        and graph.get("parent") == "dataset"
        and path.get("level") == 2
        and path.get("parent") == "dataset"
    )


def service_payload():
    return {
        "name": SERVICE_NAME,
        "displayName": SERVICE_NAME,
        "type": SERVICE_DEF_NAME,
        "description": "Fuseki integration smoke service",
        "isEnabled": True,
        "configs": {"endpoint": "http://fuseki.invalid"},
    }


def role_payload():
    return {
        "name": ROLE_NAME,
        "description": "Fuseki integration smoke role",
        "createdByUser": RANGER_USERNAME,
        "users": [
            {"name": "alice", "isAdmin": False},
            {"name": "carol", "isAdmin": False},
        ],
        "groups": [{"name": GROUP_NAME, "isAdmin": False}],
        "roles": [],
    }


def policy_payloads():
    return (
        {
            "name": "fuseki-smoke-query",
            "service": SERVICE_NAME,
            "description": "Allow smoke dataset reads when the request carries the expected department claim.",
            "isEnabled": True,
            "isAuditEnabled": False,
            "policyType": 0,
            "resources": {
                "dataset": {"values": ["smoke"], "isExcludes": False, "isRecursive": False},
                "endpoint": {"values": ["query"], "isExcludes": False, "isRecursive": False},
            },
            "policyItems": [
                {
                    "groups": [GROUP_NAME],
                    "roles": [ROLE_NAME],
                    "accesses": [{"type": "query", "isAllowed": True}],
                    "conditions": [
                        {"type": "expression", "values": ["USER.department == 'data' && REQUEST.method == 'GET'"]}
                    ],
                    "delegateAdmin": False,
                }
            ],
        },
        {
            "name": "fuseki-role-query",
            "service": SERVICE_NAME,
            "description": "Allow role-based reads against the roles dataset.",
            "isEnabled": True,
            "isAuditEnabled": False,
            "policyType": 0,
            "resources": {
                "dataset": {"values": ["roles"], "isExcludes": False, "isRecursive": False},
                "endpoint": {"values": ["query"], "isExcludes": False, "isRecursive": False},
            },
            "policyItems": [
                {
                    "roles": [ROLE_NAME],
                    "accesses": [{"type": "query", "isAllowed": True}],
                    "conditions": [
                        {"type": "expression", "values": ["REQUEST.method == 'GET'"]}
                    ],
                    "delegateAdmin": False,
                }
            ],
        },
    )


def ensure_service_definition(api):
    existing = api.get_optional(f"public/v2/api/servicedef/name/{quoted(SERVICE_DEF_NAME)}")
    payload = service_def_payload()
    if existing is None:
        created = api.request("POST", "public/v2/api/servicedef/", payload, expected_statuses=(200, 201))
        print(f"Created Ranger service definition {SERVICE_DEF_NAME}")
        return created

    if service_def_has_expected_hierarchy(existing):
        print(f"Using existing Ranger service definition {SERVICE_DEF_NAME}")
        return existing

    existing_service = api.get_optional(f"public/v2/api/service/name/{quoted(SERVICE_NAME)}")
    if existing_service is not None:
        api.request("DELETE", f"public/v2/api/service/{existing_service['id']}", expected_statuses=(200, 204))
        print(f"Deleted Ranger service {SERVICE_NAME} to recreate its service definition")

    api.request("DELETE", f"public/v2/api/servicedef/{existing['id']}", expected_statuses=(200, 204))
    print(f"Deleted Ranger service definition {SERVICE_DEF_NAME} to recreate it with resource hierarchy")

    created = api.request("POST", "public/v2/api/servicedef/", payload, expected_statuses=(200, 201))
    print(f"Created Ranger service definition {SERVICE_DEF_NAME}")
    return created


def ensure_service(api):
    existing = api.get_optional(f"public/v2/api/service/name/{quoted(SERVICE_NAME)}")
    if existing is not None:
        print(f"Using existing Ranger service {SERVICE_NAME}")
        return existing
    created = api.request("POST", "public/v2/api/service", service_payload(), expected_statuses=(200, 201))
    print(f"Created Ranger service {SERVICE_NAME}")
    return created


def list_users(api):
    return extract_items(api.request("GET", "xusers/users"), "vXUsers")


def list_groups(api):
    return extract_items(api.request("GET", "xusers/groups"), "vXGroups")


def list_group_users(api):
    return extract_items(api.request("GET", "xusers/groupusers"), "vXGroupUsers")


def find_user(api, user_name):
    return find_named_item(
        extract_items(api.request("GET", "xusers/users", params={"name": user_name}), "vXUsers"),
        user_name,
    )


def find_group(api, group_name):
    return find_named_item(
        extract_items(api.request("GET", "xusers/groups", params={"name": group_name}), "vXGroups"),
        group_name,
    )


def ensure_user(api, user_spec):
    existing = find_user(api, user_spec["name"])
    if existing is not None:
        print(f"Using existing Ranger user {user_spec['name']}")
        return existing

    payload = {
        "name": user_spec["name"],
        "firstName": user_spec["firstName"],
        "lastName": user_spec["lastName"],
        "emailAddress": user_spec["emailAddress"],
        "password": RANGER_PASSWORD,
        "userRoleList": ["ROLE_USER"],
        "status": 1,
    }
    created = api.request("POST", "xusers/secure/users", payload, expected_statuses=(200, 201))
    print(f"Created Ranger user {user_spec['name']}")
    return created


def ensure_group(api, group_name):
    existing = find_group(api, group_name)
    if existing is not None:
        print(f"Using existing Ranger group {group_name}")
        return existing
    payload = {"name": group_name}
    created = api.request("POST", "xusers/groups", payload, expected_statuses=(200, 201))
    print(f"Created Ranger group {group_name}")
    return created


def ensure_group_membership(api, group, user):
    group_users = list_group_users(api)
    for item in group_users:
        if item.get("parentGroupId") == group.get("id") and item.get("userId") == user.get("id"):
            print(f"Using existing Ranger group membership {group['name']} -> {user['name']}")
            return item
    payload = {"name": group["name"], "parentGroupId": group["id"], "userId": user["id"]}
    created = api.request("POST", "xusers/groupusers", payload, expected_statuses=(200, 201))
    print(f"Created Ranger group membership {group['name']} -> {user['name']}")
    return created


def list_roles(api):
    return extract_items(
        api.request(
            "GET",
            "public/v2/api/roles",
            params={"serviceName": SERVICE_NAME, "execUser": RANGER_USERNAME},
        ),
        "roles",
    )


def ensure_role(api):
    existing = find_named_item(list_roles(api), ROLE_NAME)
    payload = role_payload()
    if existing is None:
        created = api.request(
            "POST",
            "public/v2/api/roles",
            payload,
            params={"serviceName": SERVICE_NAME, "createNonExistUserGroup": "false"},
            expected_statuses=(200, 201),
        )
        print(f"Created Ranger role {ROLE_NAME}")
        return created

    payload["id"] = existing.get("id")
    updated = api.request("PUT", f"public/v2/api/roles/{existing['id']}", payload, expected_statuses=(200,))
    print(f"Updated Ranger role {ROLE_NAME}")
    return updated


def ensure_policy(api, desired_policy):
    existing_policies = extract_items(
        api.request("GET", f"public/v2/api/service/{quoted(SERVICE_NAME)}/policy"),
        "policies",
    )
    existing = find_named_item(existing_policies, desired_policy["name"])
    if existing is None:
        created = api.request("POST", "public/v2/api/policy", desired_policy, expected_statuses=(200, 201))
        print(f"Created Ranger policy {desired_policy['name']}")
        return created

    payload = dict(desired_policy)
    payload["id"] = existing.get("id")
    updated = api.request("PUT", f"public/v2/api/policy/{existing['id']}", payload, expected_statuses=(200,))
    print(f"Updated Ranger policy {desired_policy['name']}")
    return updated


def validate_user_roles(api):
    alice_roles = api.request("GET", f"public/v2/api/roles/user/{quoted('alice')}")
    carol_roles = api.request("GET", f"public/v2/api/roles/user/{quoted('carol')}")
    bob_roles = api.request("GET", f"public/v2/api/roles/user/{quoted('bob')}")

    if ROLE_NAME not in alice_roles:
        raise RuntimeError(f"expected alice to resolve role {ROLE_NAME}, got {alice_roles}")
    if ROLE_NAME not in carol_roles:
        raise RuntimeError(f"expected carol to resolve role {ROLE_NAME}, got {carol_roles}")
    if ROLE_NAME in bob_roles:
        raise RuntimeError(f"expected bob to remain outside role {ROLE_NAME}, got {bob_roles}")


def main():
    api = RangerApi(RANGER_ADMIN_URL, RANGER_USERNAME, RANGER_PASSWORD)

    ensure_service_definition(api)
    ensure_service(api)

    created_users = {spec["name"]: ensure_user(api, spec) for spec in USERS}
    group = ensure_group(api, GROUP_NAME)
    ensure_group_membership(api, group, created_users["alice"])

    ensure_role(api)
    for policy in policy_payloads():
        ensure_policy(api, policy)

    validate_user_roles(api)
    print("Ranger smoke bootstrap completed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise