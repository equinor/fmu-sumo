from fmu.sumo.explorer import Explorer

def test_uppercase():
    assert "loud noises".upper() == "LOUD NOISES"

def test_sumo_perms():
    sumo = Explorer("dev", interactive=False)
    print("Before perms")
    # perms = sumo.get_permissions()
    # print("After perms")
    # print(perms)

def test_sumo_fail():
    raise Exception("dummyexc")
