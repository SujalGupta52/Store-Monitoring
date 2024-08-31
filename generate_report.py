from datetime import timedelta, datetime
from db import db, cur


# This part is not functional
def generate_report():
    def datetime_range(start, end, delta):
        while start <= end:
            yield start
            start += delta
        if start_time > end:
            yield end

    cur.execute("SELECT DISTINCT store_id from store_status")
    stores = cur.fetchall()
    current_time = datetime(2023, 1, 25, 0, 0, 0)
    store_uptime_downtime = []
    for store in stores[1]:
        store_detail_weekly = {
            "uptime": 0,
            "downtime": 0,
        }
        for date in datetime_range(
            current_time - timedelta(days=7),
            current_time - timedelta(days=1),
            timedelta(days=1),
        ):
            cur.execute(
                f"""SELECT day, start_time_utc, end_time_utc 
                        FROM menu_hours 
                        WHERE store_id = '{store[0]}' 
                        AND day = {date.weekday()} 
                        ORDER BY start_time_utc ASC"""
            )
            store_menu_time = cur.fetchall()
            if len(store_menu_time) == 0:
                store_menu_time.append(
                    (
                        0,
                        datetime(2024, 1, 1, 0, 0, 0).time(),
                        datetime(2024, 1, 2, 23, 59, 59).time(),
                    )
                )
            for menu_hours in store_menu_time:
                last_status = "active"
                start_time = datetime.combine(date, menu_hours[1])
                last_time = start_time
                if start_time >= datetime.combine(date, menu_hours[2]):
                    end_time = datetime.combine(date + timedelta(days=1), menu_hours[2])
                else:
                    end_time = datetime.combine(date, menu_hours[2])
                cur.execute(
                    f"""SELECT * FROM store_status
                                WHERE timestamp_utc::date = date '{date}'"""
                )
                test = cur.fetchall()
                print(f"{test}")
