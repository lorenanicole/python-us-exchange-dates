#!/usr/bin/env python3

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import fastparquet
import pandas as pd
import requests



__author__ = "Lorena Mesa"
__email__ = "me@lorenamesa.com"

class ExchangeRateAPI:
    API = 'https://api.fiscaldata.treasury.gov/services/api/'
    PROJECT_ROOT = Path(__file__).parent.absolute()
    DATE_STR_FORMAT = '%Y-%m-%d'
    
    def __init__(self, since_date_str: str=None) -> None:
        self.since_date_str = self.__generate_quarterly_date_str__(since_date_str)

    def get_quarter(self, since_date: datetime.date) -> int:
        """
        Function that finds quarter that the since_date belongs to.
        
        :param since_date: a datetime date object
        :return: an int representing which quarter since_date belongs to
        """
        return (since_date.month - 1) // 3 + 1

    def get_first_day_of_the_quarter(self, since_date: datetime.date) -> datetime:
        """
        Function that finds that first date of a quarter.
        
        :param since_date: a datetime date object
        :return: a datetime date object
        """
        return datetime(since_date.year, 3 * ((since_date.month - 1) // 3) + 1, 1)

    def get_last_day_of_the_quarter(self, since_date: datetime.date) -> datetime:
        """
        Function that finds that last date of the curent quarter from since_data.

        :param since_date: a datetime date object
        :return: a datetime date object
        """
        quarter = self.get_quarter(since_date)
        return datetime(since_date.year + 3 * quarter // 12, 3 * quarter % 12 + 1, 1) + timedelta(days=-1)
    
    def get_last_day_of_previous_quarter(self, since_date: datetime.date) -> datetime:
        """
        Function that finds that last date of the previous quarter from since_data.

        :param since_date: a datetime date object
        :return: a datetime date object
        """
        quarter = self.get_quarter(since_date)
        return datetime(since_date.year, 3 * quarter - 2, 1) + timedelta(days=-1)
    
    def __is_date_in_future__(self, since_date_str: str) -> bool:
        """
        Function that checks if date is in the future.

        :param since_date_str: a datetime str, if provided, in ExchangeRateAPI.DATE_STR_FORMAT
        :return: a bool indicating if the datetime str represents a future date
        """
        year, month, day = since_date_str.split('-')
        since_date = datetime(int(year), int(month), int(day))
        return since_date > datetime.now()
    
    def __generate_quarterly_date_str__(self, since_date_str: str=None) -> str:
        """
        Function that has to check the following conditions:
        - If since_date_str provided is in the future, raise an error since no exchange data exists for the future
        - If since_date_str happened in the past we need to capture get_last_day_of_the_quarter
        to process the proper quartely data exchange reports.
        - If since_date_str is not provided, default to using today's date so the quarter is still ongoing 
        so we will want the get_last_day_of_the_previous_quarter.

        :param since_date_str: a datetime str, if provided, in ExchangeRateAPI.DATE_STR_FORMAT
        :return: a datetime string in the format of ExchangeRateAPI.DATE_STR_FORMAT
        """
        if since_date_str and self.__is_date_in_future__(since_date_str):
            raise ValueError(f'Date {since_date_str} provided is in the future, cannot retrieve exchange data.')

        if since_date_str:
            year, month, day = since_date_str.split('-')
            since_date = datetime(int(year), int(month), int(day))
            since_date = self.get_last_day_of_the_quarter(since_date.date())
        else:
            since_date = datetime.now()
            since_date = self.get_last_day_of_previous_quarter(since_date)
        return since_date.strftime(ExchangeRateAPI.DATE_STR_FORMAT)
    
    def generate_parquet_quarterly_files(self) -> None:
        """
        Function that makes an external API call to the US Exchange Data API, filtering on record_date and selecting only
        the country_currency_desc, exchange_rate, record_date data. If there's multiple pages of data - as the default
        is each page holds 100 rows of data, it paginating through all the records available.

        The data is grouped by date, as exchange rate data is only generated at the end of each quarter. All data retrieve
        is stored in parquet files using the quarterly data field record_date value.

        :return: None
        """
        # Build query URL
        date_str = self.since_date_str
        parameter_str = f'?fields=country_currency_desc,exchange_rate,record_date&filter=record_date:gte:{date_str}'
        endpoint = f'{ExchangeRateAPI.API}/fiscal_service/v1/accounting/od/rates_of_exchange{parameter_str}'

        # Instantiate initial request into responses
        responses, response = [], requests.get(endpoint)

        # If first request fails, bail!
        if not response.ok:
            raise Exception(f'Unable to process request, please try again later. Error code: {response.status_code}.')
        
        responses.append(response)
        expected_data_count = response.json()['meta']['total-count']

        # Paginate through results and collect, if any request fails we bail!
        next_pg = response.json()['links']['next']
        while next_pg and response.ok:
            next_pg = response.json()['links']['next']
            if next_pg is None:
                break
            next_endpoint_pg = f'{ExchangeRateAPI.API}/fiscal_service/v1/accounting/od/rates_of_exchange{parameter_str}{next_pg}'
            response = requests.get(next_endpoint_pg)
            if not response.ok:
                raise Exception(f'Unable to process request, please try again later. Error code: {response.status_code}.')
            responses.append(response)

        # For each response data a list of Python dicts with the keys country_currency_desc, exchange_rate, record_date is generated
        exchange_data = [entry for response in responses for entry in response.json()['data']]
        df = pd.json_normalize(exchange_data)
        df.reset_index(drop=True, inplace=True)
        grouped_df = df.groupby(['record_date'])

        # Validate there is no missing rows of data
        total_row_count = df.shape[0]
        if total_row_count != expected_data_count:
            raise Exception(f'There is missing data, expected {data_count} rows of data but only processed {total_records}.')
        
        parquet_files_created = []
        for date, group in grouped_df:
            # Default mode is 'w', which overwrites an existing *.parquet file of same name
            # Documentation: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html
            # We persist the record_date column as well, even though files scoped to individual parquet files 
            # by date; unclear if will be using parquet files as a larger collection so keeping date is a 
            # choice to keep the data clear.
            group.to_parquet(f'{date[0]}.parquet', engine='fastparquet', index='False')
            parquet_files_created.append(f'{date[0]}.parquet')

        print(f'Successfully created: {", ".join(filename for filename in parquet_files_created)}.')


    def read_parquet_files(self):
        """
        Utility function to read in existing parquet files on the local filesystem store at project root.

        :return: a list of Pandas dataframes for each parquet file stored in the PROJECT_ROOT
        """
        return [
            pd.read_parquet(parquet_file, engine='fastparquet') 
            for parquet_file in ExchangeRateAPI.PROJECT_ROOT.glob('*.parquet')
        ]
        
        

if __name__ == '__main__':
    # User command line interface with validation logic defined below
    parser = argparse.ArgumentParser(
        description="Retrieve exchange rate data from US Treasury.", 
        usage="""Example: python main.py will retrieve the most recent US Treasury rate data available, which is generated quarterly. 
        Alternatively the script and use the optional -s or the more verbose --script flag to specify a specific date, inclusive,
        to retrieve US Treasure exchange rate data. This would be python main.py -s='YYYY-MM-DD'."""
    )

    def validate_date_str(date_str: str) -> str:
        """
        If user provides a since parameter from the command line, this function validates the date_str provided is
        a valid date.

        :param date_str: a str representing a date in the format 'YYYY-MM-DD'
        """
        try:
            is_valid_date = bool(datetime.strptime(date_str, ExchangeRateAPI.DATE_STR_FORMAT))
            return date_str
        except ValueError:
            raise argparse.ArgumentTypeError("Invalid since date string provided, must be in format of YYYY-MM-DD.")
        
    parser.add_argument(
        "-s",
        "--since",
        type=validate_date_str,
        required=False,
        help="Date string, inclusive, to retrieve the most recent US Treasury Exchange Rate quarterly data in the format of YYYY-MM-DD.",
    )
    args = parser.parse_args()

    # Use API interface below to generate parquet quarterly files
    api = ExchangeRateAPI(args.since)
    api.generate_parquet_quarterly_files()

    # Can uncomment below to confirm can read back parquet files if need!
    # api.read_parquet_files()
