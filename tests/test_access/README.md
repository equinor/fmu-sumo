# Testing access to SUMO: read, write, manage, no access, affiliate

Tests in this folder shall be run inside Github Actions as specific 
users with specific access. Each test file is tailored for a specific 
user with either no-access, DROGON-READ, DROGON-WRITE, DROGON-MANAGE 
or DROGON-AFFILIATE.
Since you as a developer have different accesses, many tests will fail
if you run them as yourself. 

There are pytest skip decorators to avoid running these tests
outside Github Actions. 
In addition, the file names use the non-standard 'tst' over 'test' to 
avoid being picked up by a call to pytest. 

Print statements are used to ensure the Github Actions run prints 
information that can be used for debugging. 

Using allow-no-subscriptions flag to avoid having to give the 
App Registrations access to some resource inside the subscription itself. 
Example: 

```
      - name: Azure Login
        uses: Azure/login@v2
        with:
          client-id: <relevant App Registration id here>
          tenant-id: 3aa4a235-b6e2-48d5-9195-7fcf05b459b0
          allow-no-subscriptions: true
```

## Run tests on your local laptop with your own identity

If you want to run the tests on your laptop as yourself, using bash:

```
export GITHUB_ACTIONS="true"
```

Note that since you have different access, most tests should fail

## Run tests on your local laptop as one of the App Registrations

To run these tests on your developer laptop _as the different 
App Registrations_, using bash and az cli:

* Create a secret for the relevant App Registration inside Azure portal, 
copy the secret. 
* Login as the App Registration:

```
az login --service-principal -t 3aa4a235-b6e2-48d5-9195-7fcf05b459b0 -u <Client-ID> -p <Client-secret> --allow-no-subscriptions
```

* Get a token and set it in the environment where sumo-wrapper-python will pick it up:

```
export ACCESS_TOKEN=$(az account get-access-token --scope api://88d2b022-3539-4dda-9e66-853801334a86/.default --query accessToken --output tsv)
```

* Set the env-var to mimick Github Actions: 
```
export GITHUB_ACTIONS=true
```
 
* Run the tests; preferably start with running userpermissions or similar to verify that you have the 
access you expect: 
```
pytest -s tests/test_access/tst_access_drogon_affiliate_login.py::test_get_userpermissions
```

It is good practice to delete the secret from the App Registration when you are finished.

Note that the ACCESS_TOKEN can be used to login to the Swagger page (Bearer) too. 


Relevant App Registrations:

* sumo-test-runner-no-access No access
* sumo-test-runner-drogon-read DROGON-READ
* sumo-test-runner-drogon-write DROGON-WRITE
* sumo-test-runner-drogon-manage DROGON-MANAGE
* sumo-test-runner-drogon-affiliate DROGON-AFFILIATE 

(Note that the sumo-test-runner-drogon-affiliate app-reg is added as member 
to Entra ID Group named 'Sumo admin' which have the DROGON-AFFILIATE role)

The Azure Entra ID 'App Registrations' blade named 'API permissions' is 
where the access is given. Remember that the access must be granted/consented 
for Equinor by a mail to AADAppConsent@equinor.com: 
"Please grant admin consent for Azure Entra ID App Registration sumo-test-runner-drogon-affiliate 
to the sumo-core-dev drogon-affiliate role" 
as explained [here](https://docs.omnia.equinor.com/governance/iam/App-Admin-Consent/)

## Test access using shared-key

Shared key authentication is also tested. 
The shared keys are manually created with the /admin/make-shared-access-key, 
then manually put into Github Actions Secrets. 
Note that these secrets must be replaced when they expire after a year. 

It is not possible to run a 'no-access' test with shared key. 

Example /admin/make-shared-access-key in Swagger:

* user: autotest@equinor.com
* roles: one of DROGON-READ, DROGON-WRITE, DROGON-MANAGE
* duration: 365

Then paste the response body into the corresponding secret in Github, Settings, Secrets and variables, Actions, edit repository secret. 

Relevant files:

.github\workflows\*_sharedkey.yaml

