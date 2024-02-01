"""Test access to SUMO using a no-access login.
    Shall only run in Github Actions as a specific user with 
    specific access rights. Running this test with your personal login
    will fail."""
import os
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
        assert True == False


def test_get_userpermissions(explorer: Explorer):
    """Test the userpermissions"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    response = explorer._sumo.get(f"/userpermissions")
    print("/Userpermissions response: ", response.text)
    userperms = json.loads(response.text)
    assert "Drogon" not in userperms
    assert 0 == len(userperms)


def test_get_cases(explorer: Explorer):
    """Test the get_cases method"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))
    for case in cases:
        assert case.field.lower() == "drogon"
    assert len(cases) == 0


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
            assert True == False


def test_read_restricted_classification_data(explorer: Explorer):
    """Test if can read restriced data aka 'access:classification: restricted'"""
    print("Running test:", inspect.currentframe().f_code.co_name)
    cases = explorer.cases
    print("Number of cases: ", len(cases))
    assert len(cases) > 0

    # A default Drogon iteration contains 2 restricted objects,
    # so in normal situations there should be some restricted objects
    # but never for this user
    response = explorer._sumo.get(
        "/search?%24query=access.classification%3Arestricted"
    )
    assert response.status_code == 200
    response_json = json.loads(response.text)
    hits = response_json.get("hits").get("total").get("value")
    print("Hits on restricted:", hits)
    assert hits == 0


# TODO: aggregate bulk operation should fail

# TODO: FAST aggregate operation should succeed
