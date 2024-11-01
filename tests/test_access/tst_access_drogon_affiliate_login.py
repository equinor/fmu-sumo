"""Test access to SUMO using a DROGON-AFFILIATE login.
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
    assert "affiliate" in userperms.get("Drogon")
    assert 1 == len(userperms.get("Drogon"))
    assert 1 == len(userperms)


def test_get_cases(explorer: Explorer):
    """Test the get_cases method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))
    for case in cases:
        assert case.field.lower() == "drogon"
    assert len(cases) >= 1

    # We have set up 1 case in KEEP in Drogon DEV 
    # with affiliate-access and it has 2 children 
    # objects with affiliate access
    filtered_cases = cases.filter(uuid="2c2f47cf-c7ab-4112-87f9-b4797ec51cb6")
    assert len(filtered_cases) == 1
    case = filtered_cases[0]
    assert case.uuid == "2c2f47cf-c7ab-4112-87f9-b4797ec51cb6"
    assert len(case.surfaces) == 1
    assert len(case.polygons) == 1
    assert len(case.tables) == 0
    assert len(case.cubes) == 0
    assert len(case.dictionaries) == 0
    assert case.polygons[0].uuid == "a5f38286-5cf6-d85c-9b3c-03c72b5947d5"
    assert case.surfaces[0].uuid == "5f73b0c1-3bdc-2d0e-1a1d-271331615999"

    # Many Drogon cases might have been shared now, who knows. 
    # Ensure that all returned cases have correct metadata:
    for case in cases:
        affiliate_roles = case._metadata.get("access").get("affiliate_roles")
        assert "DROGON-AFFILIATE" in affiliate_roles


def test_get_object(explorer: Explorer):
    """Test the direct get on object by objectid method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))

    # We have set up a KEEP case in Drogon DEV with  
    # objects with affiliate-access

    # Read one child object
    child_object_uuid = "a5f38286-5cf6-d85c-9b3c-03c72b5947d5"
    response = explorer._sumo.get(f"/objects('{child_object_uuid}')")
    print ("child retval:", response)
    print("child retval.content:", response.content)
    assert response.status_code == 200
    response_json = json.loads(response.text)
    child_uuid = response_json.get("_id")
    print("child_uuid returned:", child_uuid)
    assert child_uuid == child_object_uuid
    classification = response_json.get("_source").get("access").get("classification")
    assert classification == "internal"

    # Read the other child object (which also have 
    # access.classification:restricted)
    child_object_uuid = "5f73b0c1-3bdc-2d0e-1a1d-271331615999"
    response = explorer._sumo.get(f"/objects('{child_object_uuid}')")
    print ("child retval:", response)
    print("child retval.content:", response.content)
    assert response.status_code == 200
    response_json = json.loads(response.text)
    child_uuid = response_json.get("_id")
    print("child_uuid returned:", child_uuid)
    assert child_uuid == child_object_uuid
    classification = response_json.get("_source").get("access").get("classification")
    assert classification == "restricted"

    # Read the case object
    case_object_uuid = "2c2f47cf-c7ab-4112-87f9-b4797ec51cb6"
    response = explorer._sumo.get(f"/objects('{case_object_uuid}')")
    print ("case retval:", response)
    print("case retval.content:", response.content)
    assert response.status_code == 200
    response_json = json.loads(response.text)
    case_uuid = response_json.get("_id")
    print("case_uuid returned:", case_uuid)
    assert case_uuid == case_object_uuid

    # Read an object which is NOT for affiliates
    child_wo_access = "6d9d222f-25d3-5029-07b8-f450dbd2be54"
    with pytest.raises(Exception, match="404*"):
        print("About to call a endpoint which should raise exception")
        response = explorer._sumo.get(f"/objects('{child_wo_access}')")
        print("Execution should never reach this line")
        print("Unexpected status: ", response.status_code)
        print("Unexpected response: ", response.text)

    # The exact number of shared Drogon cases cannot be known
    assert len(cases) >= 1

def test_delete(explorer: Explorer):
    """Test a delete method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))

    for case in cases:
        if case.uuid == "392c3c70-dd1a-41b5-ac49-0e369a0ac4eb":
            print("Found my case")
            surface_uuid = case.surfaces[0].uuid
            print("Surface:", surface_uuid)

            res = explorer._sumo.get(f"/objects('{surface_uuid}')")
            print("Get Result should be OK:", res.json())

            with pytest.raises(Exception, match="403*"):
                res = explorer._sumo.delete(f"/objects('{surface_uuid}')")
                print("Execution should never reach this line")
                print("Unexpected status: ", res.status_code)
                print("Unexpected response: ", res.text)

            with pytest.raises(Exception, match="403*"):
                res = explorer._sumo.delete(f"/objects('{case.uuid}')")
                print("Execution should never reach this line")
                print("Unexpected status: ", res.status_code)
                print("Unexpected response: ", res.text)


def test_write(explorer: Explorer):
    """Test a write method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))

    with open("./tests/data/test_case_080/case2.json") as json_file:
        metadata = json.load(json_file)
        with pytest.raises(Exception, match="403*"):
            print(
                "About to call a write endpoint which should raise exception"
            )
            response = explorer._sumo.post("/objects", json=metadata)
            print("Execution should never reach this line")
            print("Unexpected status: ", response.status_code)
            print("Unexpected response: ", response.text)


def test_read_restricted_classification_data(explorer: Explorer): 
    """Test if can read restricted data aka 'access:classification: restricted'"""
    print("Running test:", inspect.currentframe().f_code.co_name)

    # access.classification:restricted is available,
    # EVEN for this DROGON-AFFILIATE user 
    # (This differs from DROGON-READ which cannot read restricted)
    response = explorer._sumo.get(
        "/search?%24query=access.classification%3Arestricted"
    )
    assert response.status_code == 200
    response_json = json.loads(response.text)
    hits = response_json.get("hits").get("total").get("value")
    print("Hits on restricted:", hits)
    assert hits >= 1

# Remove or update this test when bulk aggregation is finalized
# @pytest.mark.skipif(not (sys.platform == "linux" and
#                          sys.version_info[:2] == (3, 11)),
#                     reason="Test only on single platform/version.")
# def test_aggregate_bulk(explorer: Explorer):
#     """Test a bulk aggregation method"""
#     print("Running test:", inspect.currentframe().f_code.co_name)
#     # Fixed test case ("Drogon_AHM_2023-02-22") in Sumo/DEV
#     # This user does not have WRITE and hence this should fail
#     # (In addition this case is not set up with affiliate access)
#     TESTCASE_UUID = "10f41041-2c17-4374-a735-bb0de62e29dc"
#     print("About to trigger bulk aggregation on case", TESTCASE_UUID)
#     body = {
#         "operations": ["min"],
#         "case_uuid": TESTCASE_UUID,
#         "class": "surface",
#         "iteration_name": "iter-0",
#     }
#     with pytest.raises(Exception, match="40*"):
#         response = explorer._sumo.post(f"/aggregations", json=body)
#         print("Execution should never reach this line")
#         print("Unexpected status: ", response.status_code)
#         print("Unexpected response: ", response.text)


def test_aggregations_fast(explorer: Explorer):
    """Test a fast aggregation method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    # Fixed test case ("Drogon_AHM_2023-02-22") in Sumo/DEV
    # This user has AFFILIATE and can READ, but this case
    # is not set up with AFFILIATE access, so should fail
    TESTCASE_UUID = "10f41041-2c17-4374-a735-bb0de62e29dc"
    print("About to trigger fast-aggregation on case", TESTCASE_UUID)
    SURFACE_UUID_1 = "ae6cf480-12ba-77ca-848e-92e707556b63"
    SURFACE_UUID_2 = "7189835b-cc8a-2a8e-4a34-dde2ceb2a69c"
    body = {
        "operations": ["min"],
        "object_ids": [SURFACE_UUID_1, SURFACE_UUID_2],
        "class": "surface",
        "iteration_name": "iter-0",
    }
    print("About to trigger fast-aggregation on hardcoded case", TESTCASE_UUID)
    print("using body", body)
    with pytest.raises(Exception, match="40*"):
        response = explorer._sumo.post(f"/aggregations", json=body)
        print("Execution should never reach this line")
        print("Unexpected status: ", response.status_code)
        print("Unexpected response: ", response.text)


# TODO: TBC: Consider setting up a case with affiliate access on all
# surfaces, so we can test successful fast aggregation. Need first 
# a clarification if affiliates are allowed fast aggregation or not


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
