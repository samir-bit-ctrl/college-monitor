"""
alerts.py — Send change notifications to Telegram and Microsoft Teams
"""

import os
import requests
from datetime import datetime


def _format_telegram_changes(changes: list[dict]) -> str:
    """Format changes into clean readable Telegram lines."""
    lines = []
    for c in changes:
        ct = c["change_type"]
        rk = c["row_key"].split("||")[0]  # just the category/particular name

        if ct == "row_added":
            lines.append(f"  ➕ *New row:* {rk}")
        elif ct == "row_removed":
            lines.append(f"  ➖ *Removed:* {rk}")
        elif ct == "value_changed":
            # old_value and new_value are like "Statistics (2025): ₹ 6.68 L"
            old = c.get("old_value", "").split(": ", 1)[-1]
            new = c.get("new_value", "").split(": ", 1)[-1]
            col = c.get("old_value", "").split(":")[0]
            lines.append(f"  ✏️ *{rk}* — {col}\n      `{old}` → `{new}`")
        elif "rank_threshold" in ct:
            direction = "📈 Improved" if "improved" in ct else "📉 Dropped"
            lines.append(f"  {direction} *{rk}*: {c['old_value']} → {c['new_value']}")

    return "\n".join(lines)


def send_telegram_alert(college_name: str, silo: str,
                        changes: list[dict], total_changes: int):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id   = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        print("  [SKIP] Telegram: credentials not set")
        return

    silo_emoji = "🎓" if silo == "placement" else "🏆"
    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    details = _format_telegram_changes(changes)

    message = (
        f"🔔 *College Monitor Alert*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🏛 *{college_name}*\n"
        f"{silo_emoji} *{silo.capitalize()}* · {total_changes} change(s)\n"
        f"🕐 {now}\n\n"
        f"{details}"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }, timeout=15)
        if resp.status_code == 200:
            print(f"  ✅ Telegram alert sent")
        else:
            print(f"  ❌ Telegram failed: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")


def send_telegram_summary(summary_lines: list[str]):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id   = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return

    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    body = "\n".join(summary_lines) if summary_lines else "✅ No changes detected today."
    message = f"📋 *College Monitor — Daily Run*\n🕐 {now}\n\n{body}"

    try:
        requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json={
            "chat_id": chat_id, "text": message, "parse_mode": "Markdown"
        }, timeout=15)
    except Exception as e:
        print(f"  ❌ Telegram summary error: {e}")


# ─────────────────────────────────────────────
# MICROSOFT TEAMS
# ─────────────────────────────────────────────

def _format_teams_changes(changes: list[dict]) -> str:
    lines = []
    for c in changes:
        ct = c["change_type"]
        rk = c["row_key"].split("||")[0]

        if ct == "row_added":
            lines.append(f"➕ New row: {rk}")
        elif ct == "row_removed":
            lines.append(f"➖ Removed: {rk}")
        elif ct == "value_changed":
            old = c.get("old_value", "").split(": ", 1)[-1]
            new = c.get("new_value", "").split(": ", 1)[-1]
            col = c.get("old_value", "").split(":")[0]
            lines.append(f"✏️ {rk} [{col}]: {old} → {new}")
        elif "rank_threshold" in ct:
            direction = "📈 Improved" if "improved" in ct else "📉 Dropped"
            lines.append(f"{direction} {rk}: {c['old_value']} → {c['new_value']}")

    return "\n".join(lines)


def send_teams_alert(college_name: str, silo: str,
                     changes: list[dict], total_changes: int,
                     college_url: str = ""):
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    if not webhook_url:
        print("  [SKIP] Teams: webhook not set")
        return

    silo_emoji = "🎓" if silo == "placement" else "🏆"
    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    details = _format_teams_changes(changes)

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"🔔 College Monitor Alert",
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": "accent",
                        "wrap": True
                    },
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "College", "value": college_name},
                            {"title": "Silo", "value": f"{silo_emoji} {silo.capitalize()}"},
                            {"title": "Changes", "value": str(total_changes)},
                            {"title": "Time", "value": now}
                        ]
                    },
                    {"type": "TextBlock", "text": "Changes", "weight": "Bolder", "spacing": "Medium"},
                    {"type": "TextBlock", "text": details, "wrap": True, "fontType": "Monospace", "size": "Small"}
                ],
                "actions": ([{"type": "Action.OpenUrl", "title": "View on Site", "url": college_url}] if college_url else [])
            }
        }]
    }

    try:
        resp = requests.post(webhook_url, json=card, timeout=15)
        if resp.status_code in (200, 202):
            print(f"  ✅ Teams alert sent")
        else:
            print(f"  ❌ Teams failed: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        print(f"  ❌ Teams error: {e}")


def send_teams_summary(summary_lines: list[str]):
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    if not webhook_url:
        return

    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    body = "\n".join(summary_lines) if summary_lines else "✅ No changes detected today."

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {"type": "TextBlock", "text": f"📋 College Monitor — Daily Run ({now})", "weight": "Bolder", "size": "Medium", "wrap": True},
                    {"type": "TextBlock", "text": body, "wrap": True}
                ]
            }
        }]
    }

    try:
        requests.post(webhook_url, json=card, timeout=15)
    except Exception as e:
        print(f"  ❌ Teams summary error: {e}")
