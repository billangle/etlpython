import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import crypto from "crypto";

const TABLE_NAME = process.env.TABLE_NAME;
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

function safeJsonParse(maybeJson) {
  if (typeof maybeJson !== "string" || maybeJson.length === 0) return null;
  try {
    return JSON.parse(maybeJson);
  } catch {
    return null;
  }
}

export async function handler(event) {
  if (!TABLE_NAME) {
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: "TABLE_NAME not set" }) };
  }

  const id = crypto.randomUUID();
  const createdAt = new Date().toISOString();

  // event.body is usually a string from API Gateway
  const parsed = safeJsonParse(event?.body);
  const body = parsed ?? event?.body ?? null;

  const total_file_count =
    (parsed && typeof parsed.total_file_count === "number") ? parsed.total_file_count : null;

  const function_name =
    (parsed && typeof parsed.function_name === "string") ? parsed.function_name : null;

  // Prefer match.remote_path if present; else first matches[0].remote_path; else null
  let remote_path = null;
  if (parsed && parsed.match && typeof parsed.match.remote_path === "string") {
    remote_path = parsed.match.remote_path;
  } else if (parsed && Array.isArray(parsed.matches) && parsed.matches[0] && typeof parsed.matches[0].remote_path === "string") {
    remote_path = parsed.matches[0].remote_path;
  }

  const matches = (parsed && Array.isArray(parsed.matches)) ? parsed.matches : null;

  const item = {
    id,
    createdAt,
    body,              // raw posted body (stored as object if JSON, else string)
    function_name,
    total_file_count,
    remote_path,
    matches,
  };

  await ddb.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));

  return {
    statusCode: 200,
    body: JSON.stringify({ ok: true, id }),
  };
}
