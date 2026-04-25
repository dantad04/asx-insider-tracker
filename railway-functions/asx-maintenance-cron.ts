const baseUrl = process.env.APP_BASE_URL ?? "https://asx-insider.up.railway.app";
const token = process.env.JOB_TRIGGER_TOKEN;

if (!token) {
  throw new Error("JOB_TRIGGER_TOKEN is not configured");
}

const response = await fetch(`${baseUrl}/api/jobs/maintenance`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "X-Job-Token": token,
  },
});

const body = await response.text();
console.log(`ASX maintenance response: ${response.status} ${body}`);

if (!response.ok) {
  throw new Error(`ASX maintenance failed with HTTP ${response.status}`);
}
