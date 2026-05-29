import os
import json
import random
import string
from datetime import datetime, timezone

import boto3
from flask import Flask, request, redirect, jsonify, render_template_string
from boto3.dynamodb.conditions import Key

# ── Config ─────────────────────────────────────────────────────────────────────
AWS_REGION      = os.environ.get("AWS_REGION", "us-east-1")
DYNAMODB_TABLE  = os.environ.get("DYNAMODB_TABLE", "url_mappings")
CLICKS_TABLE    = os.environ.get("CLICKS_TABLE", "click_events")
S3_BUCKET       = os.environ.get("S3_BUCKET", "")           # set in .env
SNS_TOPIC_ARN   = os.environ.get("SNS_TOPIC_ARN", "")       # set in .env
CLICK_THRESHOLD = int(os.environ.get("CLICK_THRESHOLD", "10"))

app = Flask(__name__)

# ── AWS clients (credentials via IAM Role on EC2, or ~/.aws/credentials locally)
dynamodb    = boto3.resource("dynamodb", region_name=AWS_REGION)
s3_client   = boto3.client("s3",        region_name=AWS_REGION)
sns_client  = boto3.client("sns",       region_name=AWS_REGION)
logs_client = boto3.client("logs",      region_name=AWS_REGION)

url_table   = dynamodb.Table(DYNAMODB_TABLE)
click_table = dynamodb.Table(CLICKS_TABLE)

# ── CloudWatch log setup ────────────────────────────────────────────────────────
LOG_GROUP  = "/url-shortener/app"
LOG_STREAM = "flask-app"

def _ensure_log_stream():
    try:
        logs_client.create_log_group(logGroupName=LOG_GROUP)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass
    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

def cw_log(message: str):
    try:
        logs_client.put_log_events(
            logGroupName=LOG_GROUP,
            logStreamName=LOG_STREAM,
            logEvents=[{
                "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "message": message,
            }],
        )
    except Exception:
        pass  # never crash the app over a log failure

_ensure_log_stream()

# ── Helpers ────────────────────────────────────────────────────────────────────
def _short_code(length=6) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/shorten", methods=["POST"])
def shorten():
    data = request.get_json(silent=True) or request.form
    original_url = (data.get("url") or "").strip()

    if not original_url or not original_url.startswith("http"):
        return jsonify({"error": "Please provide a valid URL starting with http(s)"}), 400

    code = _short_code()
    now  = datetime.now(timezone.utc).isoformat()

    url_table.put_item(Item={
        "short_code":   code,
        "original_url": original_url,
        "created_at":   now,
        "click_count":  0,
    })

    cw_log(f"SHORTEN | {code} -> {original_url}")
    return jsonify({
        "short_url":  f"{request.host_url}r/{code}",
        "short_code": code,
    })


@app.route("/r/<code>")
def redirect_url(code):
    resp = url_table.get_item(Key={"short_code": code})
    item = resp.get("Item")
    if not item:
        return "Short URL not found.", 404

    now        = datetime.now(timezone.utc).isoformat()
    user_agent = request.headers.get("User-Agent", "unknown")

    click_table.put_item(Item={
        "short_code": code,
        "timestamp":  now,
        "user_agent": user_agent,
        "ip":         request.remote_addr,
    })

    updated = url_table.update_item(
        Key={"short_code": code},
        UpdateExpression="SET click_count = click_count + :inc, last_clicked = :ts",
        ExpressionAttributeValues={":inc": 1, ":ts": now},
        ReturnValues="ALL_NEW",
    )
    click_count = int(updated["Attributes"].get("click_count", 0))

    cw_log(f"CLICK | {code} | total={click_count} | ua={user_agent[:80]}")

    if click_count == CLICK_THRESHOLD and SNS_TOPIC_ARN:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[URL Shortener] Alert: /{code} reached {CLICK_THRESHOLD} clicks",
            Message=(
                f"Short code : /r/{code}\n"
                f"Original   : {item['original_url']}\n"
                f"Clicks     : {click_count}\n"
                f"Time       : {now}"
            ),
        )
        cw_log(f"SNS_ALERT | {code} | threshold={CLICK_THRESHOLD}")

    return redirect(item["original_url"], code=302)


@app.route("/analytics")
def analytics():
    result = url_table.scan()
    items  = sorted(
        result.get("Items", []),
        key=lambda x: int(x.get("click_count", 0)),
        reverse=True,
    )
    return jsonify(items)


@app.route("/analytics/<code>")
def analytics_detail(code):
    url_resp   = url_table.get_item(Key={"short_code": code})
    click_resp = click_table.query(
        KeyConditionExpression=Key("short_code").eq(code)
    )
    return jsonify({
        "url_info": url_resp.get("Item", {}),
        "clicks":   click_resp.get("Items", []),
    })


@app.route("/export", methods=["POST"])
def export_report():
    if not S3_BUCKET:
        return jsonify({"error": "S3_BUCKET not configured"}), 500

    items    = url_table.scan().get("Items", [])
    filename = f"report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    report   = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_urls":   len(items),
        "urls": [
            {
                "short_code":   i["short_code"],
                "original_url": i["original_url"],
                "click_count":  int(i.get("click_count", 0)),
                "last_clicked": i.get("last_clicked", "never"),
                "created_at":   i.get("created_at", ""),
            }
            for i in items
        ],
    }

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=f"reports/{filename}",
        Body=json.dumps(report, indent=2, default=str),
        ContentType="application/json",
    )

    cw_log(f"EXPORT | s3://{S3_BUCKET}/reports/{filename}")
    return jsonify({"message": "Report exported to S3", "s3_key": f"reports/{filename}"})


# ── Frontend ───────────────────────────────────────────────────────────────────
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>URL Shortener</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #f0f4f8; color: #1a202c; min-height: 100vh; padding: 32px 16px; }
  h1 { font-size: 2rem; font-weight: 700; margin-bottom: 8px; }
  .subtitle { color: #718096; margin-bottom: 32px; }
  .card { background: white; border-radius: 12px; padding: 24px;
          box-shadow: 0 1px 3px rgba(0,0,0,.1); margin-bottom: 24px; max-width: 900px; margin-left: auto; margin-right: auto; }
  .card h2 { font-size: 1.1rem; font-weight: 600; margin-bottom: 16px; color: #2d3748; }
  .row { display: flex; gap: 8px; }
  input[type=text] { flex: 1; padding: 10px 14px; border: 1px solid #e2e8f0;
                     border-radius: 8px; font-size: 15px; outline: none; }
  input[type=text]:focus { border-color: #4299e1; box-shadow: 0 0 0 3px rgba(66,153,225,.15); }
  .btn { padding: 10px 20px; border: none; border-radius: 8px; cursor: pointer;
         font-size: 15px; font-weight: 500; transition: opacity .15s; }
  .btn:hover { opacity: .85; }
  .btn-blue  { background: #4299e1; color: white; }
  .btn-green { background: #48bb78; color: white; }
  #result { margin-top: 14px; font-size: 15px; }
  #result a { color: #4299e1; font-weight: 500; }
  #export-msg { margin-top: 10px; font-size: 13px; color: #48bb78; }
  table { width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 14px; }
  th { background: #ebf8ff; color: #2b6cb0; padding: 10px 12px; text-align: left;
       font-weight: 600; border-bottom: 2px solid #bee3f8; }
  td { padding: 10px 12px; border-bottom: 1px solid #e2e8f0; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f7fafc; }
  .badge { display: inline-block; background: #ebf8ff; color: #2b6cb0;
           border-radius: 999px; padding: 2px 10px; font-weight: 600; }
  .url-cell { max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
</head>
<body>

<div style="max-width:900px;margin:0 auto">
  <h1>URL Shortener</h1>
  <p class="subtitle">Shorten links, track clicks, export analytics.</p>

  <div class="card">
    <h2>Shorten a URL</h2>
    <div class="row">
      <input type="text" id="urlInput" placeholder="https://example.com/some/long/url">
      <button class="btn btn-blue" onclick="shorten()">Shorten</button>
    </div>
    <div id="result"></div>
  </div>

  <div class="card">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
      <h2 style="margin:0">Analytics Dashboard</h2>
      <div style="display:flex;gap:8px">
        <button class="btn btn-blue" onclick="loadAnalytics()">Refresh</button>
        <button class="btn btn-green" onclick="exportReport()">Export to S3</button>
      </div>
    </div>
    <div id="export-msg"></div>
    <table>
      <thead>
        <tr><th>Short URL</th><th>Original URL</th><th>Clicks</th><th>Last Clicked</th></tr>
      </thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<script>
async function shorten() {
  const url = document.getElementById('urlInput').value.trim();
  if (!url) return;
  const res  = await fetch('/shorten', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({url}),
  });
  const data = await res.json();
  const el   = document.getElementById('result');
  if (data.short_url) {
    el.innerHTML = `Short URL: <a href="${data.short_url}" target="_blank">${data.short_url}</a>`;
    loadAnalytics();
  } else {
    el.textContent = data.error || 'Something went wrong.';
    el.style.color = '#e53e3e';
  }
}

async function loadAnalytics() {
  const res   = await fetch('/analytics');
  const rows  = await res.json();
  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="4" style="color:#a0aec0;text-align:center;padding:20px">No URLs yet.</td></tr>';
    return;
  }
  rows.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><a href="/r/${row.short_code}" target="_blank">/r/${row.short_code}</a></td>
      <td class="url-cell" title="${row.original_url}">${row.original_url}</td>
      <td><span class="badge">${row.click_count || 0}</span></td>
      <td style="color:#718096">${row.last_clicked || 'never'}</td>`;
    tbody.appendChild(tr);
  });
}

async function exportReport() {
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = 'Exporting…';
  const res  = await fetch('/export', {method: 'POST'});
  const data = await res.json();
  document.getElementById('export-msg').textContent =
    data.message ? `✓ ${data.message} → ${data.s3_key}` : data.error;
  btn.disabled = false;
  btn.textContent = 'Export to S3';
}

loadAnalytics();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
