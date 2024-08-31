import pandas as pd
from datetime import datetime
import pytz
import io
from db import db, cur


def load_data():
    # Read CSV
    store_status = pd.read_csv("data\store_status.csv")
    menu_hours = pd.read_csv("data\menu_hours.csv")
    timezone = pd.read_csv("data\\timezone.csv")

    # Converts local time to utc
    def convertLocaltoUTC(s, tz):
        if pd.isna(tz):
            tz = "America/Chicago"
        local = pytz.timezone(tz)
        naive = datetime.strptime(s, "%H:%M:%S")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        return utc_dt.strftime("%H:%M:%S")

    # Merge menu hours and timezone...
    menu_hours_UTC = pd.merge(menu_hours, timezone, how="left", on="store_id")

    # Convert all time to UTC using timezone table...
    menu_hours_UTC["start_time_UTC"] = menu_hours_UTC.apply(
        lambda x: convertLocaltoUTC(x["start_time_local"], x["timezone_str"]), axis=1
    )

    menu_hours_UTC["end_time_UTC"] = menu_hours_UTC.apply(
        lambda x: convertLocaltoUTC(x["end_time_local"], x["timezone_str"]), axis=1
    )

    # Drop unwanted local time and timezone table
    menu_hours_UTC.drop(
        columns=["start_time_local", "end_time_local", "timezone_str"],
        axis=1,
        inplace=True,
    )

    # Create required table
    try:
        cur.execute("DROP TABLE IF EXISTS menu_hours")
        cur.execute("DROP TABLE IF EXISTS store_status")
        cur.execute(
            """CREATE TABLE menu_hours(
        store_id VARCHAR(50),
        day INT, start_time_utc TIME, 
        end_time_utc TIME NOT NULL)"""
        )
        cur.execute(
            """CREATE TABLE store_status(
        store_id VARCHAR(50), 
        status VARCHAR(10), 
        timestamp_utc TIMESTAMP NOT NULL)"""
        )
        # Fast inserts for medium sized data
        values_tuples = []
        for i in range(0, len(menu_hours_UTC)):
            query = "INSERT INTO menu_hours (store_id, day, start_time_utc, end_time_utc) VALUES {0}"
            values = (
                str(menu_hours_UTC["store_id"][i]),
                int(menu_hours_UTC["day"][i]),
                menu_hours_UTC["start_time_UTC"][i],
                menu_hours_UTC["end_time_UTC"][i],
            )
            values_tuples.append(values)
        print(values_tuples[1])
        string = ",".join(map(str, values_tuples))
        cur.execute(query.format(string))
        db.commit()
    except:
        db.rollback()

    try:
        # Super fast inserts for large data
        output = io.StringIO()
        for i in range(0, len(store_status)):
            query = (
                "INSERT INTO store_status(store_id, status, timestamp_utc) VALUES {0}"
            )
            output.write(
                f'{str(store_status["store_id"][i])},{store_status["status"][i]},{store_status["timestamp_utc"][i]}\n'
            )
        output.seek(0)
        with cur.copy(
            "COPY store_status(store_id, status, timestamp_utc) FROM STDIN WITH (FORMAT csv)"
        ) as copy:
            copy.write(output.getvalue())
        db.commit()
    except:
        db.rollback()
