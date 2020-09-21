from openpyxl import Workbook

from gdrive import download_broadcast_config, upload_to_google_drive, send_email
from helper import get_dates, make_local_filename, make_gdrive_filename
from homer import Country


def main():
    print('BART waking up...')
    start_date, end_date = get_dates()

    broadcast_config = download_broadcast_config()
    countries = broadcast_config['country_code'].unique()

    wb = Workbook()
    for country in countries:
        print('\n\nBeginning review for {}...'.format(country))
        x = Country(country, start_date, end_date, broadcast_config)
        x.get_active_users()
        x.get_redshift_transactions()
        x.set_active_with_trx()
        x.print_summary()
        x.run_analysis()
        x.write_csv()
        x.write_excel(wb)
    wb.remove(wb['Sheet'])

    print('\nSaving XLSX to file and uploading to Google Drive...')
    local_filename = make_local_filename(start_date, end_date, 'all', 'xlsx')
    gdrive_filename = make_gdrive_filename(start_date, end_date)
    wb.save(local_filename)
    file_id = upload_to_google_drive(local_filename, gdrive_filename)
    send_email(start_date, local_filename, file_id)

    print('\nBART going back to sleep...')


if __name__ == "__main__":
    main()
