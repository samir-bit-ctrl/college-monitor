"""
alerts.py — Send change notifications to Telegram and Microsoft Teams
"""

import os
import json
import requests
from datetime import datetime


# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────

def send_telegram_alert(college_name: str, silo: str,
                        changes_summary: str, total_changes: int):
    """
    Send a Telegram message to the configured chat.
    Uses BOT_TOKEN and TELEGRAM_CHAT_ID from environment.
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id   = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("  [SKIP] Telegram: BOT_TOKEN or CHAT_ID not set")
        return

    silo_emoji = "🎓" if silo == "placement" else "🏆"
    now = datetime.now().strftime("%d %b %Y, %I:%M %p")

    message = (
        f"🔔 *Shiksha College Monitor*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🏛️ *College:* {college_name}\n"
        f"{silo_emoji} *Silo:* {silo.capitalize()}\n"
        f"📊 *Changes:* {total_changes} detected\n"
        f"🕐 *Time:* {now}\n\n"
        f"*Details:*\n"
        f"`{changes_summary}`"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            print(f"  ✅ Telegram alert sent ({total_changes} changes)")
        else:
            print(f"  ❌ Telegram failed: {resp.status_code} — {resp.text}")
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")


def send_telegram_summary(summary_lines: list[str]):
    """Send an end-of-run summary message to Telegram."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id   = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        return

    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    body = "\n".join(summary_lines) if summary_lines else "No changes detected today."

    message = (
        f"📋 *Shiksha Monitor — Daily Run*\n"
        f"🕐 {now}\n\n"
        f"{body}"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": chat_id, "text": message, "parse_mode": "Markdown"
        }, timeout=15)
    except Exception as e:
        print(f"  ❌ Telegram summary error: {e}")


# ─────────────────────────────────────────────
# MICROSOFT TEAMS
# via Incoming Webhook (Adaptive Card)
# ─────────────────────────────────────────────

def send_teams_alert(college_name: str, silo: str,
                     changes_summary: str, total_changes: int,
                     college_url: str = ""):
    """
    Send an Adaptive Card to Microsoft Teams via Incoming Webhook.
    Uses TEAMS_WEBHOOK_URL from environment.
    """
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")

    if not webhook_url:
        print("  [SKIP] Teams: TEAMS_WEBHOOK_URL not set")
        return

    silo_emoji = "🎓" if silo == "placement" else "🏆"
    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    color = "accent"  # Teams Adaptive Card accent color

    # Adaptive Card payload
    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"🔔 Shiksha College Monitor — Change Detected",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": color,
                            "wrap": True
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "College", "value": college_name},
                                {"title": "Silo", "value": f"{silo_emoji} {silo.capitalize()}"},
                                {"title": "Changes", "value": str(total_changes)},
                                {"title": "Detected at", "value": now}
                            ]
                        },
                        {
                            "type": "TextBlock",
                            "text": "Change Details",
                            "weight": "Bolder",
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": changes_summary,
                            "wrap": True,
                            "fontType": "Monospace",
                            "size": "Small"
                        }
                    ],
                    "actions": (
                        [
                            {
                                "type": "Action.OpenUrl",
                                "title": f"View on Shiksha",
                                "url": college_url
                            }
                        ] if college_url else []
                    )
                }
            }
        ]
    }

    try:
        resp = requests.post(webhook_url, json=card, timeout=15)
        if resp.status_code in (200, 202):
            print(f"  ✅ Teams alert sent ({total_changes} changes)")
        else:
            print(f"  ❌ Teams failed: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        print(f"  ❌ Teams error: {e}")


def send_teams_summary(summary_lines: list[str]):
    """Send an end-of-run summary card to Teams."""
    webhook_url = os.getenv("TEAMS_WEBHOOK_URL")
    if not webhook_url:
        return

    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    body = "\n".join(summary_lines) if summary_lines else "✅ No changes detected today."

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"📋 Shiksha Monitor — Daily Run ({now})",
                            "weight": "Bolder",
                            "size": "Medium",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": body,
                            "wrap": True
                        }
                    ]
                }
            }
        ]
    }

    try:
        requests.post(webhook_url, json=card, timeout=15)
    except Exception as e:
        print(f"  ❌ Teams summary error: {e}")
