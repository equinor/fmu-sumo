from fmu.sumo.explorer import Explorer

def test_uppercase():
    assert "loud noises".upper() == "LOUD NOISES"


def test_sumo():
    sumo = Explorer("dev", interactive=False)
    cases = sumo.cases.filter(asset="Drogon")
    assert cases.names > 0

