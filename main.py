from datetime import date
from deta import app
import pandas as pd
from send_email import send_email

SHEET_ID = "<GOOGLE_SHEET_ID>"  # !!! CHANGE ME !!!
SHEET_NAME = "<SHEET_NAME>"  # !!! CHANGE ME !!!
URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'


def load_df(url):
    parse_dates = ["due_date", "reminder_date"]
    df = pd.read_csv(url, parse_dates=parse_dates)
    return df


def query_data_and_send_emails(df):
    present = date.today()
    email_counter = 0
    for _, row in df.iterrows():
        if (present >= row["reminder_date"].date()) and (row["has_paid"] == "no"):
            send_email(
                subject=f'Invoice: {row["invoice"]}',
                receiver_email=row["email"],
                name=row["name"],
                due_date=row["due_date"].strftime("%d, %b %Y"),
                invoice=row["invoice"],
                amount=row["amount"],
            )
            email_counter += 1
    return f'Total Emails Sent: {email_counter}'


@app.lib.cron()
def cron_job(event):
    df = load_df(URL)
    result = query_data_and_send_emails(df)
    return result
