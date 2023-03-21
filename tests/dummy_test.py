from fmu.sumo.explorer import Explorer

def test_uppercase():
    assert "loud noises".upper() == "LOUD NOISES"

sumo = Explorer("dev")
