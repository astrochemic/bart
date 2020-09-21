import os
from datetime import datetime, timedelta
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = BASE_DIR + '/cache'
SQL_DIR = BASE_DIR + '/sql'
REPORTS_DIR = BASE_DIR + '/reports'

load_dotenv(BASE_DIR + '/.env')


class DTFormat:
    Y_M_D = '%Y-%m-%d'
    H_M = '%H:%M'


def get_today():
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    return today


def get_dates():
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today - timedelta(days=today.weekday())
    start_date = end_date - timedelta(days=7)
    return start_date, end_date


def make_local_filename(start_date, end_date, country, extension):
    output_dir = '{}/{}_{}'.format(
        REPORTS_DIR,
        start_date.strftime(DTFormat.Y_M_D),
        end_date.strftime(DTFormat.Y_M_D)
    )
    if start_date == end_date:
        output_dir = '{}/{}'.format(REPORTS_DIR, start_date.strftime(DTFormat.Y_M_D))
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    return '{}/{}.{}'.format(output_dir, country, extension)


def make_gdrive_filename(start_date, end_date):
    return 'Broadcast Report {} - {}'.format(
        start_date.strftime(DTFormat.Y_M_D),
        end_date.strftime(DTFormat.Y_M_D)
    )
