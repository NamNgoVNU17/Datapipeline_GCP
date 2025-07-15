import requests
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
sys.path.insert(0, os.path.abspath("/opt/airflow"))
from src.utils.logger import setup_logger
from config.config import EXCHANGE_API_KEY, FILE_SAVE_CSV
import os
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger(__name__)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def fetch_exchange_rates_api(start_date, end_date, endpoint='timeframe'):
    url = f"https://api.exchangerate.host/{endpoint}"
    access_key = EXCHANGE_API_KEY
    symbols = 'VND,CNY,JPY,KRW,EUR,INR,GBP,HKD,AUD'

    params = {
        'start_date': start_date,
        'end_date': end_date,
        'currencies': symbols,
        'access_key': access_key
    }
    logger.info(f"Requesting API: {url}")
    logger.info(f"Request params: {params}")

    try:
        response = requests.get(url, params=params, timeout=10)
        logger.info(f"Response status code : {response.status_code}")
        response.raise_for_status()

        data = response.json()
        logger.debug(f"Response JSON: {data}")

        if data.get('success'):
            return data.get('quotes')
        else:
            logger.error(f"API error: {data.get('error')}")
    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        logger.debug(f"Failed response (if any): {getattr(e.response, 'text', None)}")
    return None


def save_to_csv(rates, filename, append=False):
    if not rates:
        logger.error("No data to save")
        return
    df_data = []
    for date, currencies in rates.items():
        row = {'date': date}
        for currency_pair, rate in currencies.items():
            currency_code = currency_pair.replace('USD', '')
            row[currency_code] = rate
        df_data.append(row)

    df = pd.DataFrame(df_data)
    columns = ['date', 'VND', 'CNY', 'JPY', 'KRW', 'EUR', 'INR', 'GBP', 'HKD', 'AUD']
    df = df[columns]

    mode = 'a' if append and os.path.exists(filename) else 'w'
    df.to_csv(filename, mode=mode, index=False, header=not append)
    logger.info(f"Saved data to {filename} (append={append})")


def generate_date_ranges(start_date, end_date, days_per_request=365):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    ranges = []
    current_start = start
    while current_start <= end:
        current_end = min(current_start + timedelta(days=days_per_request - 1), end)
        ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
        current_start = current_end + timedelta(days=1)
    return ranges


def fetch_exchange_rates(**kwargs):
    logger.warning("Begin run Task 1")
    filename = FILE_SAVE_CSV
    start_date = '2015-01-01'
    end_date = '2025-07-08'
    logger.info(f"Fetching exchange rates from {start_date} to {end_date}")

    existing_dates = set()
    if os.path.exists(filename):
        try:
            df_existing = pd.read_csv(filename)
            existing_dates = set(df_existing['date'])
            logger.info(f"Found {len(existing_dates)} existing dates in {filename}")
        except Exception as e:
            logger.warning(f"Error reading existing CSV: {e}")
    # Do giới hạn request chỉ được trong khoảng 365 ngày tối đa 100 request/tháng
    date_ranges = generate_date_ranges(start_date, end_date)
    logger.info(f"Generated {len(date_ranges)} date ranges for API requests")

    for i, (start, end) in enumerate(date_ranges, 1):
        range_dates = set((datetime.strptime(start, '%Y-%m-%d') + timedelta(days=x)).strftime('%Y-%m-%d')
                          for x in range((datetime.strptime(end, '%Y-%m-%d') - datetime.strptime(start, '%Y-%m-%d')).days + 1))

        if range_dates.issubset(existing_dates):
            logger.info(f"Skipping {start} to {end}: data already exists")
            continue

        logger.info(f"Fetching exchange rates for {start} to {end} ({i}/{len(date_ranges)})")
        rates = fetch_exchange_rates_api(start, end)

        if rates:
            save_to_csv(rates, filename, append=(i > 1 or os.path.exists(filename)))
        else:
            logger.error(f"Failed to fetch data for {start} to {end}")
