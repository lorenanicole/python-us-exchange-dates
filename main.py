#!/usr/bin/env python3

import argparse
import datetime
from pathlib import Path

import fastparquet
import pandas as pd
import requests



__author__ = "Lorena Mesa"
__email__ = "me@lorenamesa.com"

class ExchangeRateAPI:
    API = 'https://api.fiscaldata.treasury.gov/services/api/'
    PROJECT_ROOT = Path(__file__).parent.absolute()
    
    def __init__(self, since_date_str: str=None) -> None:
        self.since_date_str = self.__generate_quarterly_date_str__(since_date_str)
    
    def __generate_quarterly_date_str__(self, since_date_str: str=None) -> str:
        if since_date_str:
            year, month, day = since_date_str.split('-')
            since_date = pd.Timestamp(int(year), int(month), int(day))
        else:
            since_date = pd.Timestamp.today()

        # # obtaing the quarter of the given year
        # print("The given date can be found in quarter ", my_date.quarter)

        if since_date.month < 4:
            since_date = datetime.date(since_date.year - 1, 12, 31)
        elif since_date.month < 7:
            since_date = datetime.date(since_date.year, 3, 31)
        elif since_date.month < 10:
            since_date = datetime.date(since_date.year, 6, 30)
        else:
            since_date = datetime.date(since_date.year, 9, 30)

        return since_date.strftime("%Y-%m-%d")
    
    def generate_parquet_quarterly_files(self):
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

    def validate_date_str(date_str):
        try:
            datetime.date.fromisoformat(date_str)
            return date_str
        except ValueError:
            raise argparse.ArgumentTypeError("Incorrect provided since date string provided, must be in format of YYYY-MM-DD.")
        
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
