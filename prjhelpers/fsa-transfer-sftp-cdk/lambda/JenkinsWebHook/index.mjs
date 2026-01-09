import { Buffer } from "node:buffer";

export const handler = async (event) => {
  // event.body is a string for APIGW proxy integrations
  // If API Gateway base64-encodes it, decode it.
  let body = event?.body ?? "";

  if (event?.isBase64Encoded) {
    body = Buffer.from(body, "base64").toString("utf-8");
  }

  // Echo back EXACT payload received
  return {
    statusCode: 200,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      // optional, but helpful for Jenkins / testing
      "Access-Control-Allow-Origin": "*",
    },
    body,
  };
};
