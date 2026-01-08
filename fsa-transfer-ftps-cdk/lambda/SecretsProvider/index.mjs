import crypto from "node:crypto";
import { SecretsManagerClient, DescribeSecretCommand, CreateSecretCommand, PutSecretValueCommand, TagResourceCommand } from "@aws-sdk/client-secrets-manager";

const sm = new SecretsManagerClient({});

async function secretExists(name) {
  try {
    const d = await sm.send(new DescribeSecretCommand({ SecretId: name }));
    return d?.ARN ? { arn: d.ARN } : null;
  } catch (e) {
    if (e?.name === "ResourceNotFoundException") return null;
    throw e;
  }
}

export const handler = async (event) => {
  console.log("Event:", JSON.stringify(event));
  const reqType = event.RequestType;
  const props = event.ResourceProperties || {};
  const name = props.Name;
  const secretString = props.SecretString ?? "";
  const tags = Array.isArray(props.Tags) ? props.Tags : [];
  if (!name) throw new Error("Name is required");

  const physicalId = event.PhysicalResourceId || `SecretsProvider-${crypto.createHash("sha256").update(name).digest("hex").slice(0, 16)}`;

  if (reqType === "Delete") {
    // We do NOT delete secrets automatically; keep for safety
    return { PhysicalResourceId: physicalId, Data: { Name: name } };
  }

  const existing = await secretExists(name);
  if (!existing) {
    const create = await sm.send(new CreateSecretCommand({
      Name: name,
      SecretString: secretString,
      Tags: tags
    }));
    return { PhysicalResourceId: physicalId, Data: { Name: name, Arn: create.ARN } };
  }

  // Update value
  await sm.send(new PutSecretValueCommand({ SecretId: name, SecretString: secretString }));
  // Ensure tags (best-effort)
  try {
    if (tags.length) {
      await sm.send(new TagResourceCommand({ SecretId: name, Tags: tags }));
    }
  } catch (e) {
    console.log("TagResource ignored:", e?.name || e);
  }

  return { PhysicalResourceId: physicalId, Data: { Name: name, Arn: existing.arn } };
};
