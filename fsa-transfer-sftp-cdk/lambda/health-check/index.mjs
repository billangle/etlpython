import net from "net";
import { CloudWatchClient, PutMetricDataCommand } from "@aws-sdk/client-cloudwatch";

const cw = new CloudWatchClient({});

function tcpCheck(host, port, timeoutMs) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let done = false;

    const finish = (ok) => {
      if (done) return;
      done = true;
      try { socket.destroy(); } catch {}
      resolve(ok);
    };

    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish(true));
    socket.once("timeout", () => finish(false));
    socket.once("error", () => finish(false));

    socket.connect(port, host);
  });
}

export const handler = async () => {
  const host = process.env.SFTP_HOST;
  const port = Number(process.env.SFTP_PORT || "22");
  const namespace = process.env.METRIC_NAMESPACE || "FSA/TransferHealth";
  const metricName = process.env.METRIC_NAME || "SftpPort22";
  const serverId = process.env.SERVER_ID || "unknown";

  const ok = host ? await tcpCheck(host, port, 5000) : false;

  await cw.send(new PutMetricDataCommand({
    Namespace: namespace,
    MetricData: [{
      MetricName: metricName,
      Value: ok ? 1 : 0,
      Unit: "Count",
      Dimensions: [
        { Name: "ServerId", Value: serverId },
        { Name: "Host", Value: host || "unset" }
      ]
    }]
  }));

  return { ok, host, port };
};
