# Testing access to SUMO

Tests in this folder shall be run inside Github Actions as specific 
users with specific access. Each test file is tailored for a specific 
user with either no-access, DROGON-READ, DROGON-WRITE or DROGON-MANAGE.
Since you as a developer have different accesses, many tests will fail
if you run them as yourself. 

There are pytest skip decorators to avoid running these tests
outside Github Actions. 
In addition, the file names use the non-standard 'tst' over 'test' to avoid being picked 
up by a call to pytest. 

Print statements are used to ensure the Github Actions run provide 
information that can be used for debugging. 

Use allow-no-subscriptions flag to avoid having to give the App Registrations access to some resource inside the subscription itself. Example: 
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
