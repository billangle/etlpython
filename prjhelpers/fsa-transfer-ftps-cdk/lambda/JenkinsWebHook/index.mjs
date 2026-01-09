import { Buffer } from "node:buffer";

export const handler = async (event) => {
  let body = event?.body ?? "";
  if (event?.isBase64Encoded) {
    body = Buffer.from(body, "base64").toString("utf-8");
  }

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Access-Control-Allow-Origin": "*"
    },
    body
  };
};
