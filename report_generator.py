# report_generator.py
import json
import logging
import smtplib
import requests
from pathlib import Path
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import pandas as pd
import yaml

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


class ReportGenerator:

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.export_dir = Path(self.config["storage"]["export_dir"])
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def weekly_excel_brief(self, trend_classifications, ad_briefs, breakouts, rising_queries) -> Path:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        filename = self.export_dir / f"trends_intel_brief_{date_str}.xlsx"

        google_rows, meta_rows, tiktok_rows, yt_rows = [], [], [], []
        for brief in ad_briefs:
            base = {"keyword": brief["keyword"], "geo": brief["geo"], "trend": brief["trend"]}
            p = brief.get("platforms", {})
            if "google_ads" in p:
                g = p["google_ads"]
                google_rows.append({**base,
                    "intent":         g.get("intent"),
                    "bid_adjustment": g.get("bid_adjustment"),
                    "urgency":        g.get("urgency"),
                    "match_types":    ", ".join(g.get("match_types", [])),
                    "formats":        ", ".join(g.get("recommended_formats", [])),
                    "negative_kws":   ", ".join(g.get("negative_kws", [])),
                })
            if "meta" in p:
                m = p["meta"]
                meta_rows.append({**base,
                    "creative_direction": m.get("creative_direction"),
                    "audience_strategy":  m.get("audience_strategy"),
                    "ad_formats":         ", ".join(m.get("ad_formats", [])),
                    "cbo":                m.get("cbo_recommended"),
                })
            if "tiktok" in p:
                t = p["tiktok"]
                tiktok_rows.append({**base,
                    "content_hook": t.get("content_hook"),
                    "video_length": t.get("video_length_sec"),
                    "formats":      ", ".join(t.get("recommended_formats", [])),
                    "hashtags":     t.get("hashtag_strategy"),
                    "activate":     t.get("activate"),
                })
            if "youtube" in p:
                y = p["youtube"]
                yt_rows.append({**base,
                    "target_channels": y.get("target_channels"),
                    "ad_formats":      ", ".join(y.get("ad_formats", [])),
                    "hook_seconds":    y.get("video_hook_seconds"),
                })

        exec_summary = breakouts.copy() if not breakouts.empty else pd.DataFrame()

        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            exec_summary.to_excel(writer,              sheet_name="Executive Summary",      index=False)
            trend_classifications.to_excel(writer,     sheet_name="Trend Classifications",  index=True)
            pd.DataFrame(google_rows).to_excel(writer, sheet_name="Google Ads",             index=False)
            pd.DataFrame(meta_rows).to_excel(writer,   sheet_name="Meta",                   index=False)
            pd.DataFrame(tiktok_rows).to_excel(writer, sheet_name="TikTok",                 index=False)
            pd.DataFrame(yt_rows).to_excel(writer,     sheet_name="YouTube",                index=False)
            if not rising_queries.empty:
                rising_queries.to_excel(writer,        sheet_name="Rising Queries",         index=False)
            if not breakouts.empty:
                breakouts.to_excel(writer,             sheet_name="Breakout Log",           index=False)

        logger.info(f"Weekly brief saved: {filename}")
        return filename

    def to_csv(self, df: pd.DataFrame, name: str) -> Path:
        path = self.export_dir / f"{name}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.csv"
        df.to_csv(path)
        return path

    def send_slack_alert(self, message: str, webhook_url: str = ""):
        url = webhook_url or self.config["alerts"].get("slack_webhook", "")
        if not url:
            logger.warning("Slack webhook not configured")
            return
        try:
            requests.post(url, json={"text": message}, timeout=10).raise_for_status()
            logger.info("Slack alert sent")
        except Exception as e:
            logger.error(f"Slack failed: {e}")

    def send_email_alert(self, subject: str, body: str, attachment_path: Path = None):
        cfg = self.config["alerts"]["email"]
        if not cfg.get("from_addr") or not cfg.get("to_addrs"):
            logger.warning("Email not configured")
            return
        msg = MIMEMultipart()
        msg["From"]    = cfg["from_addr"]
        msg["To"]      = ", ".join(cfg["to_addrs"])
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        if attachment_path and attachment_path.exists():
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={attachment_path.name}")
            msg.attach(part)
        try:
            with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
                server.starttls()
                server.sendmail(cfg["from_addr"], cfg["to_addrs"], msg.as_string())
            logger.info(f"Email sent: {subject}")
        except Exception as e:
            logger.error(f"Email failed: {e}")

    def format_breakout_slack_msg(self, keyword, geo, z, pct):
        return (f"Breakout Detected!\nKeyword: `{keyword}` | Geo: `{geo}`\n"
                f"Z-Score: {z:.1f} | % Above Mean: +{pct:.0f}%")
