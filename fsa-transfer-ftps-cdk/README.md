# fsa-transfer-ftps-cdk-complete

Complete AWS CDK v2 (TypeScript) solution that provisions:

1) AWS Transfer Family **FTPS** server (internet-facing via Elastic IP + Route53 DNS)  
2) S3 bucket used as the FTPS storage backend (+ a marker object for GUI directory listings)  
3) Custom Identity Provider (API Gateway + Lambda, Node.js 22, `index.mjs`) reading per-user secrets  
4) Health check Lambda (Node.js 22, `index.mjs`) publishing a CloudWatch metric for TCP reachability  
5) Unsecured Jenkins webhook endpoint (API Gateway POST) that echoes the POST body (`lambda/JenkinsWebHook/index.mjs`)  

## Prereqs

- Node.js 18+ locally (for CDK tooling)
- AWS CDK v2 bootstrapped: `npx cdk bootstrap`
- A Route53 public hosted zone for your domain (e.g. `yourdomain.com`) so CDK can create an ACM certificate via DNS validation.

## REQUIRED context values

Update these in `cdk.json` OR pass via `-c`:

- `ftpsDomainName`  (e.g. `ftps.yourdomain.com`)
- `hostedZoneDomain` (e.g. `yourdomain.com`)

Example:

```bash
npx cdk deploy \
```

## Lambda dependencies

Install lambda deps before deploy:

```bash
cd lambda/TransferIdp && npm install --omit=dev && cd ../..
cd lambda/SftpHealthCheck && npm install --omit=dev && cd ../..
cd lambda/HostedZoneProvider && npm install --omit=dev && cd ../..
cd lambda/SecretsProvider && npm install --omit=dev && cd ../..
```

`lambda/JenkinsWebHook` has no deps.

## Deploy

```bash
npm install
npx cdk bootstrap
npx cdk deploy
```

## Outputs

- `FtpsPublicIp` - Elastic IP you can connect to
- `FtpsHostname` - DNS hostname (matches `ftpsDomainName`)
- `FtpsPort` - 21
- `TransferIdpApiUrl` - API Gateway base URL used by Transfer Family custom IdP
- `JenkinsWebHookUrl` - Unsecured POST endpoint that echoes request body
- `SecretName` - Secret holding `FSA-CERT-Secrets` JSON
- `TransferUserSecretName` - per-user secret used by the custom IdP

## FTPS Client settings (Cyberduck)

- Protocol: **FTP-SSL (Explicit AUTH TLS)**
- Host: `FtpsHostname` (recommended) or `FtpsPublicIp`
- Port: `21`
- Mode: Passive

## DNS delegation (required for public reachability)


This stack **creates** the Route53 public hosted zone for `hostedZoneDomain` and outputs the zone name servers.
For `ftpsDomainName` to resolve publicly and for ACM validation to complete, you must delegate the domain at its registrar
to these Route53 name servers.

If your domain is already hosted in Route53 in a different account, either deploy in that account or delegate appropriately.
