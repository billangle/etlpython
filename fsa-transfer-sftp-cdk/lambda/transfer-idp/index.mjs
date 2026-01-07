import { Buffer } from "node:buffer";
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";

const sm = new SecretsManagerClient({});

function parsePath(path = "") {
  const m = path.match(/\/servers\/([^/]+)\/users\/([^/]+)\/config$/);
  if (!m) return { serverId: null, username: null };
  return { serverId: m[1], username: decodeURIComponent(m[2]) };
}

function header(headers, name) {
  if (!headers) return undefined;
  return headers[name] ?? headers[name.toLowerCase()];
}

function decodePassword(headers) {
  const pw = header(headers, "Password");
  if (pw) return pw;

  const pwB64 = header(headers, "PasswordBase64");
  if (!pwB64) return null;

  try {
    return Buffer.from(pwB64, "base64").toString("utf-8");
  } catch {
    return null;
  }
}

function ok(bodyObj) {
  return {
    statusCode: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(bodyObj),
  };
}

export const handler = async (event) => {
  try {
    const path = event?.path || "";
    const headers = event?.headers || {};

    const { serverId, username } = parsePath(path);
    const password = decodePassword(headers);

    console.log(JSON.stringify({ path, serverId, username, hasPassword: !!password }));

    if (!serverId || !username) {
      // Reject
      return ok({});
    }

    const bucket = "dartsftpcertstack-dartsftpbucket7b9f3ef1-zkbnudzsbsjw";
    const bucketprefix = "s_dart_cert";
    const prefix = process.env.USER_SECRET_PREFIX || "transfer";
    const secretId = `${prefix}/${serverId}/${username}`;

    const resp = await sm.send(new GetSecretValueCommand({ SecretId: secretId }));
    if (!resp.SecretString) return ok({});

    const cfg = JSON.parse(resp.SecretString);

    if (cfg.Password && password !== cfg.Password) return ok({});
    if (!cfg.Role) return ok({});

    const transferResponse = {
        Role: cfg.Role,
        HomeDirectoryType: "LOGICAL",
        HomeDirectoryDetails: JSON.stringify([
            { Entry: "/", Target: `/${bucket}/${bucketprefix}` }
        ])
    };
    if (cfg.HomeDirectory) transferResponse.HomeDirectory = cfg.HomeDirectory;
    if (cfg.Policy) transferResponse.Policy = cfg.Policy;
    if (cfg.PublicKeys) transferResponse.PublicKeys = cfg.PublicKeys;

    console.log("Accept:", JSON.stringify(transferResponse));
    return ok(transferResponse);
  } catch (err) {
    console.error("IdP exception:", err);
    // Never throw to API GW; return reject
    return ok({});
  }
};
