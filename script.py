import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.schedules import Cron
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
from prefect.blocks.system import Secret
from prefect_github import GitHubRepository
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import pickle
import subprocess

github_block = GitHubRepository(
    name="tvc2-repo",
    repository_url="https://github.com/pdetoni/daily_stock_overflow.git",
    reference="main"
)
github_block.save("tvc2-repo", overwrite=True)

def commit_and_push_changes():
    subprocess.run(["git", "add", "*"], check=True)
    subprocess.run(["git", "commit", "-m", "Atualização automática pelo Prefect"], check=True)
    subprocess.run(["git", "push"], check=True)


tickers = [
    "ABEV3.SA", "ALPA4.SA", "AMER3.SA", "ARZZ3.SA", "ASAI3.SA", "AZUL4.SA",
    "B3SA3.SA", "BBAS3.SA", "BBDC3.SA", "BBDC4.SA", "BBSE3.SA", "BEEF3.SA",
    "BPAC11.SA", "BPAN4.SA", "BRAP4.SA", "BRFS3.SA", "BRKM5.SA", "BRML3.SA",
    "CASH3.SA", "CCRO3.SA", "CIEL3.SA", "CMIG4.SA", "CMIN3.SA", "COGN3.SA",
    "CPFE3.SA", "CPLE6.SA", "CRFB3.SA", "CSAN3.SA", "CSNA3.SA", "CVCB3.SA",
    "CYRE3.SA", "DXCO3.SA", "ECOR3.SA", "EGIE3.SA", "ELET3.SA", "ELET6.SA",
    "EMBR3.SA", "ENBR3.SA", "ENGI11.SA", "ENEV3.SA", "EQTL3.SA", "EZTC3.SA",
    "FLRY3.SA", "GGBR4.SA", "GOAU4.SA", "GOLL4.SA", "HAPV3.SA", "HGTX3.SA",
    "HYPE3.SA", "IGTI11.SA", "IRBR3.SA", "ITSA4.SA", "ITUB4.SA", "JBSS3.SA",
    "JHSF3.SA", "KLBN11.SA", "LAME4.SA", "LCAM3.SA", "LIGT3.SA", "LINX3.SA",
    "LREN3.SA", "MGLU3.SA", "MOVI3.SA", "MRFG3.SA", "MRVE3.SA", "MULT3.SA",
    "MYPK3.SA", "NTCO3.SA", "PCAR3.SA", "PETR3.SA", "PETR4.SA", "POSI3.SA",
    "PRIO3.SA", "QUAL3.SA", "RADL3.SA", "RAIL3.SA", "RENT3.SA", "RRRP3.SA",
    "SANB11.SA", "SBSP3.SA", "SULA11.SA", "SUZB3.SA", "TAEE11.SA", "TIMS3.SA",
    "TOTS3.SA", "UGPA3.SA", "USIM5.SA", "VALE3.SA", "VBBR3.SA", "VIVT3.SA",
    "VVAR3.SA", "WEGE3.SA", "YDUQ3.SA"
]


def authenticate_google_drive():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('drive', 'v3', credentials=creds)

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def fetch_stock_data(ticker):
    logger = get_run_logger()
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        data = yf.download(ticker, start=start_date, end=end_date)
        logger.info(f"Data fetched successfully for {ticker}")
        return data
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        raise

@task
def calculate_indicators(data):
    data['MA'] = data['Close'].rolling(window=5).mean()
    data['Volatility'] = data['Close'].rolling(window=5).std()
    return data

@task
def save_daily_data(data, ticker):
    logger = get_run_logger()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        data_folder = os.path.join("data", today)
        os.makedirs(data_folder, exist_ok=True)
        filename = os.path.join(data_folder, f"{ticker}_{today}.csv")
        data.to_csv(filename)
        logger.info(f"Data saved locally as {filename}")
        return filename
    except Exception as e:
        logger.error(f"Error saving data for {ticker}: {e}")
        raise

@task
def upload_to_google_drive(filename):
    logger = get_run_logger()
    try:
        drive_service = authenticate_google_drive()
        file_metadata = {'name': os.path.basename(filename)}
        media = MediaFileUpload(filename, mimetype='text/csv')
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info(f"File {filename} uploaded to Google Drive with ID: {file.get('id')}")
    except Exception as e:
        logger.error(f"Error uploading {filename} to Google Drive: {e}")
        raise

@task
def generate_report(data, ticker):
    plt.figure(figsize=(10, 5))
    plt.plot(data['Close'], label='Close Price')
    plt.plot(data['MA'], label='Moving Average')
    plt.title(f"{ticker} Stock Price")
    plt.legend()
    today = datetime.now().strftime('%Y-%m-%d')
    data_folder = os.path.join("data", today)
    os.makedirs(data_folder, exist_ok=True)
    plt.savefig(os.path.join(data_folder, f"{ticker}_report.png"))
    plt.close()

@task
def log_top_movers(data, ticker):
    data['Daily_Return'] = data['Close'].pct_change()
    top_gainers = data.nlargest(3, 'Daily_Return')
    top_losers = data.nsmallest(3, 'Daily_Return')
    
    # Convert Timestamp to string for JSON serialization
    create_table_artifact(
        key="top-movers",
        table={
            "columns": ["Ticker", "Date", "Daily Return"],
            "data": [
                [ticker, date.strftime('%Y-%m-%d'), return_value]
                for date, return_value in zip(top_gainers.index, top_gainers['Daily_Return'])
            ]
        }
    )

@flow(name="Stock Analysis Workflow")
def stock_analysis_flow():
    for ticker in tickers:
        data = fetch_stock_data(ticker)
        data = calculate_indicators(data)
        filename = save_daily_data(data, ticker)
        upload_to_google_drive(filename)
        generate_report(data, ticker)
        log_top_movers(data, ticker) 
    commit_and_push_changes()


if __name__ == "__main__":
    stock_analysis_flow().deploy(
        name="daily-stock-analysis",
        work_pool_name="tvc2",
        job_variables={"pip_packages": ["yfinance", "pandas", "matplotlib", "google-auth", "google-auth-oauthlib", "google-auth-httplib2", "google-api-python-client", "oauthlib", "requests", "prefect-github", "google-auth-oauthlib", "google-auth-httplib2", "google-api-python-client", "oauthlib", "requests"]},
        schedules = [Cron("0 22 * * *")]
    )