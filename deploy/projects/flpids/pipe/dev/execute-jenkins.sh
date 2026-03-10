#!/usr/bin/env bash
set -euo pipefail

# Exact URL requested by user.
JENKINS_URL="https://jenkinsfsa-dev.dl.usda.gov/job/BuildProcess/job/DeployFlpids/build?token=72305AD4-A19D-462C-9CEB-275E01BDFAFD"

# Optional first arg: path to JSON file to use as POST buffer.
POST_BUFFER_FILE="${1:-post-buffer.json}"

if [[ ! -f "$POST_BUFFER_FILE" ]]; then
  cat > "$POST_BUFFER_FILE" <<'JSON'
{
  "eventKey": "repo:refs_changed",
  "repository": {
    "project": { "key": "FP" },
    "slug": "DeployFlpids"
  },
  "changes": [
    {
      "ref": { "displayId": "main" }
    }
  ],
  "source": "aws-cloudshell"
}
JSON
fi

POST_BUFFER_CONTENT="$(cat "$POST_BUFFER_FILE")"

echo "Calling Jenkins URL: $JENKINS_URL"
echo "POST buffer file: $POST_BUFFER_FILE"
echo "POST buffer bytes: $(wc -c < "$POST_BUFFER_FILE" | tr -d ' ')"

# Send raw JSON body to the exact /build URL.
RAW_CODE=$(curl -sS -o /tmp/jenkins_raw_body.out -w "%{http_code}" \
  -X POST "$JENKINS_URL" \
  -H "Content-Type: application/json" \
  --data-binary @"$POST_BUFFER_FILE")

echo "Raw JSON POST HTTP status: $RAW_CODE"

# Also send form data to the exact same URL so Jenkins can expose POST_BUFFER/payload as parameters in compatible setups.
FORM_CODE=$(curl -sS -o /tmp/jenkins_form_body.out -w "%{http_code}" \
  -X POST "$JENKINS_URL" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "POST_BUFFER=$POST_BUFFER_CONTENT" \
  --data-urlencode "payload=$POST_BUFFER_CONTENT")

echo "Form POST HTTP status: $FORM_CODE"
echo "Done. Check Jenkins console for payload length / postBuffer length and archived bitbucket-webhook-post-buffer.json"
