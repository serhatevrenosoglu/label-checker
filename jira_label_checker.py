import os
import json
import requests
import pyodbc
from datetime import date

DB_SERVER = os.environ.get("DB_SERVER", "EU-DB-DEMO")
DB_NAME = os.environ.get("DB_NAME", "CostETL")


def get_db_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

JIRA_EMAIL = os.environ["JIRA_EMAIL"]
JIRA_TOKEN = os.environ["JIRA_TOKEN"]

JIRA_BASE = "https://invent.atlassian.net"
SLACK_CHANNEL = "U029NHG8EPQ"
JQL = "project in (TCS,BCS,ATCS,LP2CS,IPEKYOLMD,MMCS) AND created >= -90d AND status != \"Won't Fix\""

VALID_COMBOS = {
    "Service/Operational Work Log": ["ETL","OperationalRequest","ProcessFollowups","Parameter/Configuration","RunReview","RunTrigger","RunError","Dagfails","Maintenance"],
    "Development": ["Enhancement","NewFeature","Configuration","Forecasting/AccuracyImprovement","UI","DataTransfer"],
    "Extension": ["Enhancement","VersionUpgrade","NewFeature","Revision/Configuration","Implementation"],
    "Analysis": ["Diagnostic Analysis","Simulation","KPIFollowups","InsightAnalysis","PairControl","SpecialDayEffect","Seasonality","Extension","Cost","Ext_Assessment/DeepDive","Int_Assessment/DeepDive"],
    "Reporting": ["ETL","LogicBug","DataBug","UIBug","Bug","New","Revision","Revision/Configuration"],
    "Documentation": ["Presentation/MeetingNotes","NewDocuments/Revision"],
    "Bug": ["LogicBug","DataBug","UIBug","Bug","Dagfails","RunError","Debug"],
    "Incident": ["DataQuality","UI","Result"],
    "Handover": ["Meeting","Documentation","QualityControls","LeftOver"],
    "Int Call": ["PlanningMeeting","PlanningMeetings","Meeting"],
    "Client Call": ["ClientMeetings","ClientMeeting","ClientTrainingSessions"],
    "VersionUpgrade": ["Meeting","Test","Bug","Documentation/Analysis"],
    "Service Work Log (Customer)": ["ETL","OperationalRequest","ProcessFollowups","RunReview","RunTrigger","Revision","RunError"],
    "Service Work Log (Int)": ["ETL","OperationalRequest","ProcessFollowups","RunReview","RunTrigger","RunError"],
    "Operational": ["ETL","OperationalRequest","ProcessFollowups","Parameter/Configuration","RunReview","RunTrigger","RunError","Dagfails","Maintenance"],
}
SKIP_TYPES = {"Sub-task"}


def fetch_all_issues():
    auth = (JIRA_EMAIL, JIRA_TOKEN)
    issues = []
    next_page_token = None
    while True:
        body = {"jql": JQL, "maxResults": 100,
                "fields": ["summary", "issuetype", "labels", "project", "created"]}
        if next_page_token:
            body["nextPageToken"] = next_page_token
        resp = requests.post(
            f"{JIRA_BASE}/rest/api/3/search/jql",
            auth=auth,
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("issues", [])
        issues.extend(batch)
        if data.get("isLast", True) or not batch:
            break
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
    return issues


def check_issues(issues):
    missing_labels = []
    wrong_labels = []
    unknown_types = []
    for issue in issues:
        fields = issue["fields"]
        issue_type = fields["issuetype"]["name"]
        if issue_type in SKIP_TYPES:
            continue
        labels = fields.get("labels", [])
        valid = VALID_COMBOS.get(issue_type)
        if valid is None:
            unknown_types.append({
                "project": fields["project"]["key"],
                "key": issue["key"],
                "summary": fields["summary"],
                "issue_type": issue_type,
                "labels": labels,
            })
            continue
        if not labels:
            missing_labels.append({
                "project": fields["project"]["key"],
                "key": issue["key"],
                "summary": fields["summary"],
                "issue_type": issue_type,
            })
            continue
        for label in labels:
            if label not in valid:
                wrong_labels.append({
                    "project": fields["project"]["key"],
                    "key": issue["key"],
                    "summary": fields["summary"],
                    "issue_type": issue_type,
                    "label": label,
                })
    return missing_labels, wrong_labels, unknown_types


def send_slack(text):
    url = os.environ["SLACK_WEBHOOK_URL"]
    payload = {"payload": json.dumps({"text": text})}
    resp = requests.post(url, data=payload)
    print(f"Slack status: {resp.status_code}")
    print(f"Slack response: {resp.text}")


def main():
    test_mode = os.environ.get("TEST_MODE", "").lower() in ("1", "true", "yes")
    today = date.today().strftime("%Y-%m-%d")

    print(f"Fetching Jira issues...")
    issues = fetch_all_issues()
    print(f"Total issues fetched: {len(issues)}")

    missing_labels, wrong_labels, unknown_types = check_issues(issues)
    total_issues = len(missing_labels) + len(wrong_labels) + len(unknown_types)

    if total_issues == 0:
        text = f"✅ *Günlük Label Kontrol (Son 3 Ay) — {today}*\nTüm boardlar temiz, uyumsuzluk bulunamadı."
        print(text)
    else:
        lines = [f"🔍 *Günlük Label Kontrol (Son 3 Ay) — {today}*\n"]
        if missing_labels:
            lines.append(f"*— Label Eksik ({len(missing_labels)} adet) —*")
            for m in missing_labels:
                lines.append(
                    f"🏷️ *{m['project']}* | {m['key']} — {m['summary']}\n"
                    f"Type: `{m['issue_type']}` | Label: _yok_"
                )
        if wrong_labels:
            lines.append(f"\n*— Yanlış Label ({len(wrong_labels)} adet) —*")
            for m in wrong_labels:
                lines.append(
                    f"⚠️ *{m['project']}* | {m['key']} — {m['summary']}\n"
                    f"Type: `{m['issue_type']}` | Label: `{m['label']}`"
                )
        if unknown_types:
            lines.append(f"\n*— Tanımsız Issue Type ({len(unknown_types)} adet) —*")
            for m in unknown_types:
                lbl = ", ".join(m["labels"]) if m["labels"] else "_yok_"
                lines.append(
                    f"❓ *{m['project']}* | {m['key']} — {m['summary']}\n"
                    f"Type: `{m['issue_type']}` | Labels: {lbl}"
                )
        text = "\n\n".join(lines)
        print(text)

    if test_mode:
        print(f"\n[TEST MODE] Slack skipped. Eksik: {len(missing_labels)}, Yanlış: {len(wrong_labels)}, Tanımsız tip: {len(unknown_types)}")
        return

    send_slack(text)
    print(f"Done. Eksik: {len(missing_labels)}, Yanlış: {len(wrong_labels)}, Tanımsız tip: {len(unknown_types)}")


if __name__ == "__main__":
    main()
