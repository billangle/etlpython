Each lambda that uses AWS SDK v3 has its own `package.json`.

Install dependencies before `cdk deploy`:

```bash
cd lambda/TransferIdp && npm install --omit=dev && cd ../..
cd lambda/SftpHealthCheck && npm install --omit=dev && cd ../..
cd lambda/HostedZoneProvider && npm install --omit=dev && cd ../..
cd lambda/SecretsProvider && npm install --omit=dev && cd ../..
```

`lambda/JenkinsWebHook` has no dependencies.
