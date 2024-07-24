#!/usr/bin/env python3

import argparse
import io

import fastparquet
import pandas as pd
import requests



__author__ = "Lorena Mesa"
__email__ = "me@lorenamesa.com"

class ExchangeRateAPI:
    API = 'https://api.fiscaldata.treasury.gov/services/api/'
    def __init__(self, since_date_str):
        self.since_date_str = since_date_str #self.__generate_quarterly_date(since_date_str)
    
    # def __generate_quarterly_date__(self):
    #     quarter_start = pd.to_datetime(pd.datetime.today() - pd.tseries.offsets.QuarterBegin(startingMonth=1)).date()
    #     return quarter_start
    
    def generate_quarterly_files(self):
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
        data_count = response.json()['meta']['total-count']
        # TODO: Validate retrieving all the responses! Should match data count above!
        # Paginate through results and collect
        next_pg = response.json()['links']['next']
        while next_pg and response.ok:
            next_pg = response.json()['links']['next']
            # import pdb; pdb.set_trace()
            if next_pg is None:
                # import pdb; pdb.set_trace()
                break
            next_endpoint_pg = f'{ExchangeRateAPI.API}/fiscal_service/v1/accounting/od/rates_of_exchange{parameter_str}{next_pg}'
            response = requests.get(next_endpoint_pg)
            if not response.ok:
                raise Exception(f'Unable to process request, please try again later. Error code: {response.status_code}.')
            responses.append(response)

        # For eah response data a list of Python dicts with the keys country_currency_desc, exchange_rate, record_date is generated
        exchange_data = [entry for response in responses for entry in response.json()['data']]
        
        df = pd.json_normalize(exchange_data)
        grouped_df = df.groupby(['record_date'])

        for idx, (date, group) in enumerate(grouped_df):
            # TODO: Ask - do we want files for every day SINCE date or just persist the dates data into those number of files
            print(date[0])
            print(group)
            print(group.size)
            print('----')
        
        # df = pd.read_parquet(parquet_file, engine='fastparquet')
        # print(df.shape)

if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
    #     description="Retrieve exchange rate data from US Treasury.", 
    #     usage="""Example: python main.py will retrieve the most recent US Treasury rate data available, which is generated quarterly. 
    #     Alternatively the script and use the optional -s or the more verbose --script flag to specify a specific date, inclusive,
    #     to retrieve US Treasure exchange rate data. This would be python main.py -s='YYYY-MM-DD'."""
    # )
    # def len_gt_zero(input_str: str):
    #     if len(input_str) > 0 and :
    #         return input_str
    #     else:
    #         raise argparse.ArgumentTypeError("Outfile filename must have length greater than 0 in order to generate an identicon.")
    # parser.add_argument(
    #     "-s",
    #     "--since",
    #     type=str,
    #     required=False,
    #     help="Date string, inclusive, to retrieve the most recent US Treasury Exchange Rate data in the format of YYYY-MM-DD.",
    # )
  
    # args = parser.parse_args()

    api = ExchangeRateAPI('2023-12-01')
    api.generate_quarterly_files()
