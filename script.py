import os
import datetime
import pandas as pd
import yfinance as yf
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact

# Lista de tickers – em produção, esta lista pode ser armazenada em um Block do Prefect (Variável)
TICKERS = [
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

@task(retries=3, retry_delay_seconds=10, log_prints=True)
def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Coleta os dados de um ticker utilizando a API do Yahoo Finance.
    Em caso de falha, a tarefa será reexecutada (retry).
    """
    logger = get_run_logger()
    logger.info(f"Iniciando coleta de dados para {ticker}")
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            logger.warning(f"Nenhum dado encontrado para {ticker}")
        else:
            data.reset_index(inplace=True)
            data["Ticker"] = ticker
            logger.info(f"Dados coletados para {ticker} com sucesso.")
        return data
    except Exception as e:
        logger.error(f"Erro ao coletar dados para {ticker}: {e}")
        raise

@task(log_prints=True)
def process_stock_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Processa os dados coletados:
     - Calcula média móvel de 5 períodos para o fechamento
     - Calcula retornos diários e volatilidade (desvio padrão dos retornos)
    """
    logger = get_run_logger()
    if data.empty:
        logger.warning("Dados vazios para processar.")
        return data
    data["Close_MA5"] = data["Close"].rolling(window=5).mean()
    data["Return"] = data["Close"].pct_change()
    data["Volatility"] = data["Return"].rolling(window=5).std()
    logger.info("Processamento dos dados concluído.")
    return data

@task(log_prints=True)
def save_daily_data(all_data: pd.DataFrame):
    """
    Salva os dados dos últimos 7 dias particionados por dia em arquivos CSV.
    Os arquivos são salvos na pasta 'data', com o nome no formato YYYY-MM-DD.csv.
    """
    logger = get_run_logger()
    output_folder = "data"
    os.makedirs(output_folder, exist_ok=True)
    
    if not pd.api.types.is_datetime64_any_dtype(all_data["Date"]):
        all_data["Date"] = pd.to_datetime(all_data["Date"])
    
    for day, group in all_data.groupby(all_data["Date"].dt.date):
        filename = os.path.join(output_folder, f"{day}.csv")
        group.to_csv(filename, index=False)
        logger.info(f"Dados do dia {day} salvos em {filename}.")

@task(log_prints=True)
def generate_daily_report(all_data: pd.DataFrame):
    """
    Gera um relatório simples baseado nos dados do último dia disponível.
    Calcula a variação percentual dos fechamentos e identifica:
     - Top 3 ações em alta
     - Top 3 ações em baixa
    Além disso, registra o resultado utilizando um artifact table do Prefect.
    """
    logger = get_run_logger()
    all_data["Date"] = pd.to_datetime(all_data["Date"])
    last_date = all_data["Date"].max().date()
    data_last_day = all_data[all_data["Date"].dt.date == last_date]
    
    # Cálculo da variação percentual dos fechamentos para cada ticker
    data_last_day = data_last_day.copy()
    data_last_day["Pct_Change"] = data_last_day.groupby("Ticker")["Close"].pct_change()
    data_last_day = data_last_day.dropna(subset=["Pct_Change"])
    
    top_gainers = data_last_day.sort_values("Pct_Change", ascending=False).head(3)
    top_losers = data_last_day.sort_values("Pct_Change", ascending=True).head(3)
    
    report = f"Relatório Diário - {last_date}\n\n"
    report += "Top 3 Ações em Alta:\n"
    report += top_gainers[["Ticker", "Pct_Change"]].to_string(index=False) + "\n\n"
    report += "Top 3 Ações em Baixa:\n"
    report += top_losers[["Ticker", "Pct_Change"]].to_string(index=False)
    
    logger.info("Relatório diário gerado com sucesso:\n" + report)
    
    # Registro do artifact com os dados das ações
    table_data = pd.concat([top_gainers, top_losers])
    create_table_artifact(
        key="daily_top_stocks",
        table=table_data,
        description="Top 3 ações que mais subiram e top 3 que mais desceram no último dia."
    )
    return report

@flow(name="Workflow de Ações - Prefect")
def stock_data_flow():
    """
    Fluxo principal que:
     1. Define o intervalo (últimos 7 dias)
     2. Coleta e processa os dados para cada ticker
     3. Salva os dados particionados por dia
     4. Gera um relatório diário e registra os artifacts
    """
    logger = get_run_logger()
    end_date = datetime.date.today() + datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=7)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    logger.info(f"Coletando dados de {start_date_str} até {end_date_str}")
    
    all_stock_data = []
    for ticker in TICKERS:
        data = fetch_stock_data(ticker, start_date_str, end_date_str)
        processed_data = process_stock_data(data)
        all_stock_data.append(processed_data)
    
    if all_stock_data:
        combined_data = pd.concat(all_stock_data, ignore_index=True)
    else:
        logger.error("Nenhum dado coletado para nenhum ticker.")
        return
    
    save_daily_data(combined_data)
    report = generate_daily_report(combined_data)
    logger.info("Workflow concluído com sucesso.")
    return report

if __name__ == '__main__':
    stock_data_flow()
