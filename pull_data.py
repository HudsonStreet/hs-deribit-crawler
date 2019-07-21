#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# Created By  : Michael S
# Created Date: Wed June 26 20:52:00 EST 2019
# =============================================================================

import os, sys
import datetime
import configparser
import pandas as pd
from pprint import pprint
lib_path = os.path.abspath(os.path.join('.','python_deribit'))
sys.path.append(lib_path)
import openapi_client

def read_config(config_path, section):
    """
    :param config_path: the location of configuration file
    :param section: section's name
    :return return an object contains configuration info
    """
    # Locate the configuration path
    script_dir = os.path.abspath(os.path.join('.','config'))
    abs_config_path = script_dir + config_path
    # Read configuration 
    config = configparser.ConfigParser()
    config.read(abs_config_path) 
    print(abs_config_path)
    print(script_dir)
    info = {}
    if config.has_section(section):
        params = config.items(section)
        for par in params:
            info[par[0]] = par[1]
    else:
        raise Exception('section {0} not found in the {1} file'.format(section, abs_config_path))
        
    return info
#TODO(jinsiang): Update to use request sessions
def _deribit_api_client_instance(credentials):
    """
    Take creds and return api client
    """
    conf = openapi_client.Configuration()
    client = openapi_client.api_client.ApiClient(conf)
    publicApi = openapi_client.api.public_api.PublicApi(client)
    response = publicApi.public_auth_get('client_credentials', '', '', credentials['auth_key'], credentials['auth_secret'], '', '', '', scope='session:test wallet:read')
    access_token = response['result']['access_token']
    # Set up token information
    conf_authed = openapi_client.Configuration()
    conf_authed.access_token = access_token
    client_authed =  openapi_client.api_client.ApiClient(conf_authed)
    # Create an instance of the market api class
    market_api_instance = openapi_client.api.market_data_api.MarketDataApi(client_authed)

    return market_api_instance

def _data_get_summary_by_currency(api_client_instance, base_currency, derivative_kind):
    """
    Load data by using debrit API
    Need to put the openapi_client python in parent folder
    :param credentials: dic with key and secret information
    :return return the required data
    e.g.
    {
    'volume': 14.6,
    'underlying_price': 9548.31,
    'underlying_index': 'BTC-27DEC19',
    'quote_currency': 'USD',
    'open_interest': 83.4,
    'mid_price': 0.441,
    'mark_price': 0.44181073,
    'low': 0.3885,
    'last': 0.4295,
    'interest_rate': 0.0,
    'instrument_name': 'BTC-27DEC19-12000-P',
    'high': 0.4565,
    'estimated_delivery_price': 9484.63,
    'creation_timestamp': 1563340045235,
    'bid_price': 0.434,
    'base_currency': 'BTC',
    'ask_price': 0.448
    }
    """
    # Create an instance of the market api class
    currency = base_currency #'BTC'
    kind = derivative_kind #'option'
    api_response = api_client_instance.public_get_book_summary_by_currency_get(currency, kind=kind)
    #print(api_response['result'])
    
    return api_response['result']


#TODO(jinsiang): Update the writing pattern, this might causing memLeak
def _transform_data(api_client_instance, data_summary):
    row_data= [] 
    #  Generate a table as below format:
    # 'RowID', 'Date', 'volume', 'bid', 'last', 'ask', 'volume', '
    #  %change', 'change','strike'
    for ins in data_summary:
        instrument_summary = {}
        current_instrument_name = ins['instrument_name']
        instrument_summary['instrument_name'] = current_instrument_name
        name_collection = ins['instrument_name'].split('-')  
        if len(name_collection) > 3:
            #new_obj['creation_time'] = datetime.datetime.fromtimestamp(ins['creation_timestamp']/1000.0).strftime('%m-%d-%Y')
            expiryDate = datetime.datetime.strptime(name_collection[1], '%d%b%y').strftime('%m-%d-%Y')
            instrument_summary['expiry_date'] = expiryDate
            
            strikePrice = name_collection[2]
            instrument_summary['strike_price'] = strikePrice
            
            optionType = name_collection[3]
            instrument_summary['option_type'] = optionType

            instrument_summary['RowID'] = optionType + '-' + expiryDate + '-' + strikePrice
        
        instrument_summary['bid_price'] = ins['bid_price']
        instrument_summary['ask_price'] = ins['ask_price']
        instrument_summary['volume'] = ins['volume']
        instrument_summary['last'] = ins['last']
        bid_size_by_instrument, ask_size_by_instrument = _data_get_size_by_instrument(api_client_instance, current_instrument_name)
        instrument_summary['bid_size'] = bid_size_by_instrument
        instrument_summary['ask_size'] = ask_size_by_instrument
        #new_obj['underlying_price'] = ins['underlying_price'] #Do we need this? 
        row_data.append(instrument_summary)

    return row_data

def _data_get_size_by_instrument(api_client_instance, instrument):
    bid_ask_api_response = api_client_instance.public_get_order_book_get(instrument)
    #This is a list of [bid_price, bid_size]
    bid_price_with_size = bid_ask_api_response['result']['bids']
    if bid_price_with_size:
        bid_size_by_instrument = bid_ask_api_response['result']['bids'][0][1]
    else:
        bid_size_by_instrument = '-'
    
    ask_price_with_size = bid_ask_api_response['result']['asks']
    if ask_price_with_size:
        ask_size_by_instrument = bid_ask_api_response['result']['asks'][0][1]
    else:
        ask_size_by_instrument = '-'

    return bid_size_by_instrument, ask_size_by_instrument

# def _data_get_instruments(credentials, base_currency, derivative_kind):
#     market_api_instance = _deribit_api_client(credentials)
#     currency = base_currency #'BTC'
#     kind = derivative_kind #'option'
#     api_response = market_api_instance.public_get_instruments_get(currency, kind=kind)

#     print(api_response)


def _data_factory(row_data):
    """
    Pair Calls and Puts with expireDate/strike
    Format:
    'RowID': CurrencyType-ExpriryDate-StrickPrice
    """
    pass

def save_csv(data):
    df = pd.DataFrame(data)
    df.to_csv(r'final_data.csv',index=False)

if __name__ == "__main__":
    credentials = read_config('/config_prod.ini', 'deribit_credential')
    deribit_api_instance = _deribit_api_client_instance(credentials)
    data_summary = _data_get_summary_by_currency(deribit_api_instance, 'BTC', 'option')
    row_form = _transform_data(deribit_api_instance, data_summary)
    print(row_form)
    
    # _data_get_instruments(credentials, 'BTC', 'option')
    #data = _data_get_summary_by_currency(credentials, 'BTC', 'option')
    #_data_get_szie_by_instrument(credentials, data)
    # final_data = _transform_data(data)
    # save_csv(final_data)


# TODO: 
# 1. Add python-mongo connector to push data into mongo
# 2. Add python scheduler to forard data in a regular basis 