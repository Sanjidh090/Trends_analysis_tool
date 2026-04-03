# jobs.py
import logging
import yaml
from pathlib import Path
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logger     = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def job_daily_collection():
    logger.info("Job: daily_collection")
    from geo_orchestrator import GeoOrchestrator
    cfg  = load_config()
    orch = GeoOrchestrator(config_path=CONFIG_PATH)
    orch.run(keywords=cfg["collection"]["keywords_seed"], timeframes=["now 7-d", "today 1-m"])
    logger.info("daily_collection complete")


def job_breakout_check():
    logger.info("Job: breakout_check")
    from db import TrendsDB
    from signal_processor import detect_breakout
    from report_generator import ReportGenerator
    cfg      = load_config()
    db       = TrendsDB(cfg["storage"]["db_path"])
    reporter = ReportGenerator(config_path=CONFIG_PATH)
    webhook  = cfg["alerts"].get("slack_webhook", "")

    for geo_cfg in cfg["geos"]:
        geo = geo_cfg["code"]
        for keyword in cfg["collection"]["keywords_seed"]:
            history = db.get_interest_history(keyword, geo, days=30)
            if history.empty or "value" not in history.columns:
                continue
            result = detect_breakout(history["value"])
            if result["is_breakout"]:
                logger.warning(f"Breakout | keyword={keyword} geo={geo} z={result['z_score']}")
                db.log_breakout(keyword, geo, result["z_score"],
                                result["current_value"], result["pct_above_mean"])
                if webhook:
                    msg = reporter.format_breakout_slack_msg(
                        keyword, geo, result["z_score"], result["pct_above_mean"])
                    reporter.send_slack_alert(msg, webhook)
    logger.info("breakout_check complete")


def job_weekly_report():
    logger.info("Job: weekly_report")
    import pandas as pd
    from db import TrendsDB
    from signal_processor import classify_all
    from targeting_engine import full_platform_brief
    from report_generator import ReportGenerator
    cfg      = load_config()
    db       = TrendsDB(cfg["storage"]["db_path"])
    reporter = ReportGenerator(config_path=CONFIG_PATH)

    all_cls, all_briefs = [], []
    for geo_cfg in cfg["geos"]:
        geo      = geo_cfg["code"]
        kw_series = {}
        for kw in cfg["collection"]["keywords_seed"]:
            h = db.get_interest_history(kw, geo, days=90)
            if not h.empty and "value" in h.columns:
                kw_series[kw] = h["value"]
        if not kw_series:
            continue
        df  = pd.DataFrame(kw_series)
        cls = classify_all(df, geo=geo)
        cls["geo"] = geo
        all_cls.append(cls)
        for kw, row in cls.iterrows():
            rising = db.get_rising_queries(geo=geo, limit=5)
            rising_list = rising[rising["keyword"] == kw]["query"].tolist() if not rising.empty else []
            brief = full_platform_brief(kw, row["label"], row["momentum"], geo, related_rising=rising_list)
            all_briefs.append(brief)
            db.save_ad_brief(brief)

    cls_df   = pd.concat(all_cls) if all_cls else pd.DataFrame()
    breakouts = db.get_breakout_log(days=7)
    excel_path = reporter.weekly_excel_brief(cls_df, all_briefs, breakouts, pd.DataFrame())
    reporter.send_slack_alert(
        f"Weekly Brief ready | {datetime.utcnow().strftime('%Y-%m-%d')} | "
        f"{len(cfg['collection']['keywords_seed'])} keywords | {len(cfg['geos'])} geos"
    )
    reporter.send_email_alert(
        subject=f"Weekly Trends Brief — {datetime.utcnow().strftime('%Y-%m-%d')}",
        body="Attached: weekly Google Trends Ads Intelligence Brief.",
        attachment_path=excel_path,
    )
    logger.info("weekly_report complete")


def run_scheduler():
    cfg       = load_config()
    sched_cfg = cfg["scheduler"]
    scheduler = BlockingScheduler(timezone="UTC")

    h, m = sched_cfg["daily_collection"].split(":")
    scheduler.add_job(job_daily_collection, CronTrigger(hour=int(h), minute=int(m)),
                      id="daily_collection", max_instances=1)

    scheduler.add_job(job_breakout_check, "interval",
                      minutes=sched_cfg.get("breakout_check_interval_min", 30),
                      id="breakout_check")

    day, ts = sched_cfg["weekly_report"].split(" ")
    wh, wm  = ts.split(":")
    day_map = {"Monday":0,"Tuesday":1,"Wednesday":2,"Thursday":3,
               "Friday":4,"Saturday":5,"Sunday":6}
    scheduler.add_job(job_weekly_report,
                      CronTrigger(day_of_week=day_map[day], hour=int(wh), minute=int(wm)),
                      id="weekly_report")

    logger.info("Scheduler started")
    scheduler.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    run_scheduler()
