"""
Imports
"""
## Data Handling
import xarray as xr
import pandas as pd
import numpy as np

## Downloads 
import requests
from bs4 import BeautifulSoup as bs

## Misc
import os
import pytz
import time


"""
Pandas Client
"""
class EntsoePandasClient():    
    def __init__(self, api_key):
        self.api_key = api_key  
        
    def resolution_to_timedelta(self, res_text):
        resolutions = {
            'PT60M': '60min',
            'P1Y': '12M',
            'PT15M': '15min',
            'PT30M': '30min',
            'P7D': '7D',
            'P1M': '1M',
        }

        delta = resolutions.get(res_text)

        if delta is None:
            raise NotImplementedError("Sorry, I don't know what to do with the "
                                      "resolution '{}', because there was no "
                                      "documentation to be found of this format. "
                                      "Everything is hard coded. Please open an "
                                      "issue.".format(res_text))
        return delta

    def parse_datetimeindex(self, soup, tz = None):
        start = pd.Timestamp(soup.find('start').text)
        end = pd.Timestamp(soup.find('end').text)

        if tz is not None:
            start = start.tz_convert(tz)
            end = end.tz_convert(tz)

        delta = self.resolution_to_timedelta(res_text=soup.find('resolution').text)
        index = pd.date_range(start=start, end=end, freq=delta, closed='left')

        if tz is not None:
            dst_jump = len(set(index.map(lambda d:d.dst()))) > 1

            if dst_jump and delta == "7D":
                # For a weekly granularity, if we jump over the DST date in October,
                # date_range erronously returns an additional index element
                # because that week contains 169 hours instead of 168.
                index = index[:-1]

            index = index.tz_convert("UTC")

        return index

    def extract_timeseries(self, xml_text):
        if not xml_text:
            return

        soup = bs(xml_text, 'html.parser')

        for timeseries in soup.find_all('timeseries'):
            yield timeseries

    def parse_values(self, soup):
        index = []
        data = []

        for point in soup.find_all('point'):
            index.append(int(point.find('position').text))
            data.append(float(point.find('quantity').text))

        s = pd.Series(index=index, data=data)
        s = s.sort_index()
        s.index =self.parse_datetimeindex(soup)

        return s

    def parse_response(self, xml_text):
        s = pd.Series()

        for soup in self.extract_timeseries(xml_text):
            s = s.append(self.parse_values(soup))

        s = s.sort_index()

        return s
        
    def datetime_to_str(self, dt):
        if isinstance(dt, str):
            dt = pd.to_datetime(dt)
        
        if dt.tzinfo is not None and dt.tzinfo != pytz.UTC:
            dt = dt.tz_convert('UTC')
        
        dt_str = dt.strftime('%Y%m%d%H%M')
        
        return dt_str

        
    def query_entsoe(self, **params):
        url = 'https://transparency.entsoe.eu/api'
        
        params.update({'securityToken':self.api_key})

        r = requests.get(url, params=params)
        
        return r
    
    def query_country_load(self, entsoe_zone, start, end, process_type='A16'):
        params = {
            'documentType' : 'A65',
            'OutBiddingZone_Domain' : entsoe_zone,
            'periodStart' : self.datetime_to_str(start),
            'periodEnd' : self.datetime_to_str(end),
            'ProcessType' : process_type,
        }
        
        r = self.query_entsoe(**params)
        s = self.parse_response(r.text)
        
        return s
    

"""
ENTSOE Country Metadata
"""
country_meta = {
    'france' : {
        'country_code' : 'FR',
        'timezone' : 'Europe/Paris',
        'entsoe_zone' : '10YFR-RTE------C',
        'resolution' : 'H',
    },
    'germany' : {
        'country_code' : 'DE',
        'timezone' : 'Europe/Berlin',
        'entsoe_zone' : '10Y1001A1001A83F',
        'resolution' : '15T',
    },
    'italy' : {
        'country_code' : 'IT',
        'timezone' : 'Europe/Rome',
        'entsoe_zone' : '10YIT-GRTN-----B',
        'resolution' : 'H',
    },
    'norway' : {
        'country_code' : 'NO',
        'timezone' : 'Europe/Oslo',
        'entsoe_zone' : '10YNO-0--------C',
        'resolution' : 'H',
    },
    'spain' : {
        'country_code' : 'ES',
        'timezone' : 'Europe/Madrid',
        'entsoe_zone' : '10YES-REE------0',
        'resolution' : 'H',
    },
    'UK' : {
        'country_code' : 'GB',
        'timezone' : 'Europe/London',
        'entsoe_zone' : '10YGB----------A',
        'resolution' : '30T',
    },
}