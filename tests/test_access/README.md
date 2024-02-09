# Testing access to SUMO: read, write, manage, no access

Tests in this folder shall be run inside Github Actions as specific 
users with specific access. Each test file is tailored for a specific 
user with either no-access, DROGON-READ, DROGON-WRITE or DROGON-MANAGE.
Since you as a developer have different accesses, many tests will fail
if you run them as yourself. 

There are pytest skip decorators to avoid running these tests
outside Github Actions. 
In addition, the file names use the non-standard 'tst' over 'test' to avoid being picked 
up by a call to pytest. 

Print statements are used to ensure the Github Actions run prints 
information that can be used for debugging. 

Using allow-no-subscriptions flag to avoid having to give the App Registrations access to some resource inside the subscription itself. Example: 
```
      - name: Azure Login
        uses: Azure/login@v1
        with:
          client-id: <relevant App Registration id here>
          tenant-id: 3aa4a235-b6e2-48d5-9195-7fcf05b459b0
          allow-no-subscriptions: true
```

If you want to run the tests on your laptop, using bash:
export GITHUB_ACTIONS="true"

In theory you could run locally as the App Registration / Service Principal but I 
do not think the sumo-wrapper-python makes it possible:
```
az login --service-principal -u <app-id> -p <password-or-cert> --tenant <tenant> --allow-no-subscriptions
```

Relevant App Registrations:

* sumo-test-runner-no-access No access
* sumo-test-runner-drogon-read DROGON-READ
* sumo-test-runner-drogon-write DROGON-WRITE
* sumo-test-runner-drogon-manage DROGON-MANAGE

The Azure Entra ID 'App Registrations' blade named 'API permissions' is where the access is 
given. 

## Test access using shared-key

Shared key authentication is also tested. The shared keys are manually created with the /admin/make-shared-access-key, then manually put into Github Actions Secrets. Note that these secrets must be replaced when they expire after a year. 

It is not possible to run a 'no-access' test with shared key. 

Example /admin/make-shared-access-key in Swagger:

* user: autotest@equinor.com
* roles: one of DROGON-READ, DROGON-WRITE, DROGON-MANAGE
* duration: 365

Then paste the response body into the corresponding secret in Github, Settings, Secrets and variables, Actions, edit repository secret. 

Relevant files:

.github\workflows\*_sharedkey.yaml
