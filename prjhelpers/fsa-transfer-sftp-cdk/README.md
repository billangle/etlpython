# FSA CERT Transfer Family SFTP (CDK / TypeScript)

This CDK app provisions:

- AWS Transfer Family **SFTP** server (public endpoint)
- **Password-based auth** using a **custom identity provider** (API Gateway -> Lambda)
- An **S3 bucket** used as the Transfer Family storage backend
- Secrets:
  - `FSA-CERT-Secrets` (your JSON, populated with SFTP DNS + port + creds)
  - `transfer/<serverId>/S_DART-CERT` (per-user auth/config secret consumed by IdP Lambda)
- Health check:
  - EventBridge scheduled Lambda that attempts a TCP connect to `<sftp-dns>:22`
  - Publishes CloudWatch custom metric `FSA/TransferHealth:SftpPort22`
  - CloudWatch alarm if health != 1 for 2 consecutive periods

## Deploy

```bash
npm install
npx cdk synth
npx cdk deploy
```

## Outputs

- `SftpEndpoint` - DNS hostname to use as `echo_ip`
- `SftpBucket` - S3 bucket created for SFTP storage

## Connect (example)

```bash
sftp -o PreferredAuthentications=password -P 22 S_DART-CERT@<SftpEndpoint>
```

> Note: SFTP password auth is enabled here via the custom identity provider.
