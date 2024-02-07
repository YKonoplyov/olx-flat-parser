import os


from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


load_dotenv()


def get_creds():
    creds = Credentials.from_service_account_file(
        os.getenv("GOOGLE_CREDENTIALS_JSON")
        )
    scoped = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return scoped

    