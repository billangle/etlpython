import crypto from "node:crypto";
import { Route53Client, ListHostedZonesByNameCommand, CreateHostedZoneCommand, DeleteHostedZoneCommand, GetHostedZoneCommand } from "@aws-sdk/client-route-53";

const r53 = new Route53Client({});

function normZoneName(z) {
  if (!z) return "";
  return z.endsWith(".") ? z : `${z}.`;
}

export const handler = async (event) => {
  console.log("Event:", JSON.stringify(event));
  const reqType = event.RequestType;
  const props = event.ResourceProperties || {};
  const zoneName = normZoneName(props.ZoneName);
  if (!zoneName) throw new Error("ZoneName is required");

  const stableId = `HostedZoneProvider-${crypto.createHash("sha256").update(zoneName).digest("hex").slice(0, 16)}`;
  let physicalId = event.PhysicalResourceId || stableId;

  if (reqType === "Delete") {
    const deleteOnRemove = String(props.DeleteOnRemove ?? "false").toLowerCase() === "true";
    if (!deleteOnRemove) return { PhysicalResourceId: physicalId };

    const hostedZoneId = physicalId.startsWith("/hostedzone/") ? physicalId : (physicalId.startsWith("Z") ? `/hostedzone/${physicalId}` : null);
    if (!hostedZoneId) return { PhysicalResourceId: physicalId };

    try {
      await r53.send(new DeleteHostedZoneCommand({ Id: hostedZoneId }));
    } catch (e) {
      console.log("DeleteHostedZone (ignored):", e?.name || e);
    }
    return { PhysicalResourceId: physicalId };
  }

  const list = await r53.send(new ListHostedZonesByNameCommand({ DNSName: zoneName, MaxItems: 10 }));
  const match = (list.HostedZones || []).find(z => normZoneName(z.Name) === zoneName && !z.Config?.PrivateZone);

  let hostedZoneId;
  if (match?.Id) {
    hostedZoneId = match.Id;
  } else {
    const create = await r53.send(new CreateHostedZoneCommand({
      Name: zoneName,
      CallerReference: crypto.randomUUID(),
      HostedZoneConfig: { PrivateZone: false, Comment: "Managed by CDK custom resource" }
    }));
    hostedZoneId = create.HostedZone?.Id;
  }

  if (!hostedZoneId) throw new Error("Failed to determine HostedZoneId");

  physicalId = hostedZoneId;

  const hz = await r53.send(new GetHostedZoneCommand({ Id: hostedZoneId }));
  const nameServers = hz.DelegationSet?.NameServers || [];

  return {
    PhysicalResourceId: physicalId,
    Data: {
      HostedZoneId: hostedZoneId.replace("/hostedzone/", ""),
      HostedZoneIdFull: hostedZoneId,
      NameServers: nameServers.join(",")
    }
  };
};
