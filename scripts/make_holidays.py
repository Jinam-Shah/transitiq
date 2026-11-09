"""Generate dbt/seeds/holidays.csv from the rules (not typed by hand).

US federal holidays (5 U.S.C. 6103) with the federal "observed" rule (Saturday -> Friday,
Sunday -> Monday), Juneteenth from 2021, plus Massachusetts Patriots' Day (third Monday of
April), when MBTA service and ridership differ. Run: python scripts/make_holidays.py
"""

import csv
from datetime import date, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "holidays.csv"


def nth_weekday(year, month, weekday, n):
    d = date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def last_weekday(year, month, weekday):
    d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def observed(d):
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def holidays(year):
    rows = [
        ("New Year's Day", observed(date(year, 1, 1)), "federal"),
        ("Martin Luther King Jr. Day", nth_weekday(year, 1, 0, 3), "federal"),
        ("Washington's Birthday", nth_weekday(year, 2, 0, 3), "federal"),
        ("Patriots' Day", nth_weekday(year, 4, 0, 3), "massachusetts"),
        ("Memorial Day", last_weekday(year, 5, 0), "federal"),
        ("Independence Day", observed(date(year, 7, 4)), "federal"),
        ("Labor Day", nth_weekday(year, 9, 0, 1), "federal"),
        ("Columbus Day", nth_weekday(year, 10, 0, 2), "federal"),
        ("Veterans Day", observed(date(year, 11, 11)), "federal"),
        ("Thanksgiving Day", nth_weekday(year, 11, 3, 4), "federal"),
        ("Christmas Day", observed(date(year, 12, 25)), "federal"),
    ]
    if year >= 2021:
        rows.append(("Juneteenth", observed(date(year, 6, 19)), "federal"))
    return rows


def main():
    rows = sorted((d, name, kind) for y in range(2018, 2029) for name, d, kind in holidays(y))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["holiday_date", "holiday_name", "holiday_type"])
        for d, name, kind in rows:
            w.writerow([d.isoformat(), name, kind])
    print(f"wrote {len(rows)} rows to {OUT}")


if __name__ == "__main__":
    main()
