"""Test access to SUMO using a DROGON-WRITE login.
    Shall only run in Github Actions as a specific user with 
    specific access rights. Running this test with your personal login
    will fail."""

import os
import sys
import json
import inspect
import pytest
from context import (
    Explorer,
)

if os.getenv("GITHUB_ACTIONS") == "true":
    RUNNING_OUTSIDE_GITHUB_ACTIONS = "False"
    print(
        "Found the GITHUB_ACTIONS env var, so I know I am running on Github now. Will run these tests."
    )
else:
    RUNNING_OUTSIDE_GITHUB_ACTIONS = "True"
    msg = "Skipping these tests since they can only run on Github Actions as a specific user"
    print("NOT running on Github now.", msg)
    pytest.skip(msg, allow_module_level=True)


@pytest.fixture(name="explorer")
def fixture_explorer(token: str) -> Explorer:
    """Returns explorer"""
    return Explorer("dev", token=token)


def test_admin_access(explorer: Explorer):
    """Test access to an admin endpoint"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    with pytest.raises(Exception, match="403*"):
        print("About to call an admin endpoint which should raise exception")
        explorer._sumo.get(
            f"/admin/make-shared-access-key?user=noreply%40equinor.com&roles=DROGON-READ&duration=111"
        )
        print("Execution should never reach this line")


def test_get_userpermissions(explorer: Explorer):
    """Test the userpermissions"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    response = explorer._sumo.get(f"/userpermissions")
    print("/Userpermissions response: ", response.text)
    userperms = json.loads(response.text)
    assert "Drogon" in userperms
    assert "write" in userperms.get("Drogon")
    assert 1 == len(userperms.get("Drogon"))
    assert 1 == len(userperms)


def test_get_cases(explorer: Explorer):
    """Test the get_cases method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))
    for case in cases:
        assert case.field.lower() == "drogon"
    assert len(cases) > 0


def test_write(explorer: Explorer):
    """Test a write method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases.filter(status="scratch")
    print("Number of cases: ", len(cases))
    assert len(cases) > 0
    case = cases[0]
    print("case uuid:", case.metadata.get("fmu").get("case").get("uuid"))
    print("About to write to a case")
    response = explorer._sumo.post(f"/objects", json=case.metadata)
    print(response.status_code)
    print(response.text)
    assert response.status_code == 200


def test_read_restricted_classification_data(explorer: Explorer):
    """Test if can read restriced data aka 'access:classification: restricted'"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))
    assert len(cases) > 0

    # A default Drogon iteration contains 2 restricted objects,
    # so in normal situations there should be some restricted objects
    response = explorer._sumo.get(
        "/search?%24query=access.classification%3Arestricted"
    )
    assert response.status_code == 200
    response_json = json.loads(response.text)
    hits = response_json.get("hits").get("total").get("value")
    print("Hits on restricted:", hits)
    assert hits > 0

# Remove or update this test when bulk aggregation is finalized
# @pytest.mark.skipif(not (sys.platform == "linux" and
#                          sys.version_info[:2] == (3, 11)),
#                     reason="Test only on single platform/version.")
# def test_aggregate_bulk(explorer: Explorer):
#     """Test a bulk aggregation method"""
#     print("Running test:", inspect.currentframe().f_code.co_name)
#     cases = explorer.cases.filter(status="scratch")
#     print("Number of cases: ", len(cases))
#     assert len(cases) > 0
#     case = None
#     for c in cases:
#         if len(c.realizations) > 1 and len(c.surfaces) > 40:
#             case = c
#             break
#     assert case
#     case_uuid = case.metadata.get("fmu").get("case").get("uuid")
#     print("About to trigger aggregation on case", case_uuid)
#     body = {
#         "operations": ["min"],
#         "case_uuid": case_uuid,
#         "class": "surface",
#         "iteration_name": case.iterations[0].name,
#     }
#     response = explorer._sumo.post(f"/aggregations", json=body)
#     print(response.status_code)
#     assert response.status_code in [200, 201, 202]


def test_aggregations_fast(explorer: Explorer):
    """Test a fast aggregation method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))
    assert len(cases) > 0
    case = None
    for c in cases:
        if (
            len(c.realizations) > 1
            and len(c.surfaces) > 40
            and len(c.iterations) == 1
            and len(
                c.surfaces.filter(
                    name="Therys Fm.", tagname="FACIES_Fraction_Calcite"
                )
            )
            > 2
        ):
            case = c
            break
    assert case
    case_uuid = case.metadata.get("fmu").get("case").get("uuid")
    print("About to trigger fast-aggregation on case", case_uuid)
    surface1 = case.surfaces.filter(
        name="Therys Fm.", realization=0, tagname="FACIES_Fraction_Calcite"
    )
    surface2 = case.surfaces.filter(
        name="Therys Fm.", realization=1, tagname="FACIES_Fraction_Calcite"
    )
    print("Len filtered: ", len(surface1))
    print("Len filtered: ", len(surface2))
    assert len(surface1) == 1
    assert len(surface2) == 1
    surface_uuids = [surface1[0].uuid, surface2[0].uuid]
    body = {
        "operations": ["min"],
        "object_ids": surface_uuids,
        "class": "surface",
        "iteration_name": case.iterations[0].name,
    }
    response = explorer._sumo.post(f"/aggregations", json=body)
    print("Response status code:", response.status_code)
    assert response.status_code == 200
    print("Length of returned aggregate object:", len(response.text))


def test_get_access_log(explorer: Explorer):
    """Test to get the access log method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    print("About to get access log")
    response = explorer._sumo.get("/access-log")
    print(response.status_code)
    print(len(response.text))
    # Currently all authenticated users have access
    assert response.status_code == 200


def test_get_key(explorer: Explorer):
    """Test to get key method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    print("About to get key, which should raise exception ")
    with pytest.raises(Exception, match="403*"):
        response = explorer._sumo.get("/key")
        print("Execution should never reach this line")
        print("Unexpected status: ", response.status_code)
        print("Unexpected response: ", response.text)


def test_get_purge(explorer: Explorer):
    """Test to get purge method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    print("About to get purge, which should raise exception ")
    with pytest.raises(Exception, match="403*"):
        response = explorer._sumo.get("/purge")
        print("Execution should never reach this line")
        print("Unexpected status: ", response.status_code)
        print("Unexpected response: ", response.text)


def test_get_message_log_truncate(explorer: Explorer):
    """Test to get msg log truncate method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    print("About to get msg log truncate, which should raise exception ")
    with pytest.raises(Exception, match="403*"):
        response = explorer._sumo.get("/message-log/truncate?cutoff=99")
        print("Execution should never reach this line")
        print("Unexpected status: ", response.status_code)
        print("Unexpected response: ", response.text)
