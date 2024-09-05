import requests
import json
import pandas as pd
from pathlib import Path
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'safety_stocks')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

api_url = 'https://api.eia.gov/v2/petroleum/stoc/wstk/data'
params = {'api_key': 'QyPbWQo92CjndZz8conFD9wb08rBkP4jnDV02TAd'}
header = {
    "frequency": "weekly",
    "data": [
        "value"
    ],
    "facets": {
        "duoarea": [
            "R1X"
        ],
        "process": [
            "SAE",
            "SAXP",
            "SKB"
        ],
        "product": [
            "EPD0"
        ]
    },
    "start": "1982-08-20",
    "end": "2025-08-23",
    "sort": [
        {
            "column": "period",
            "direction": "desc"
        }
    ],
    "offset": 0,
    "length": 5000
}
# Initialize an empty list to store all the data points
all_eia_stock_values = []
while True:
    # Make the API request
    eia_stock = requests.get(
        api_url, params=params, headers={'X-Params': json.dumps(header)}
    )
    # Check if the request was successful
    if eia_stock.status_code == 200:
        eia_stock_data = eia_stock.json()
        eia_stock_series = eia_stock_data['response']['data']
        # Check if there's no more data
        if not eia_stock_series:
            break  # Exit the loop if no more data
        # Append data to the list
        for data_point in eia_stock_series:
            date = data_point['period']
            area = data_point['duoarea']
            product = data_point['product']
            stock = data_point['value']
            process = data_point['process']
            all_eia_stock_values.append(
                {
                    'area': area,
                    'date': date,
                    'stock': stock,
                    'product type': product,
                    'process': process
                }
            )
        header['offset'] += 5000
    else:
        print(f'Failed to retrieve data. Status code: {eia_stock.status_code}')
        break
eia_stock_raw_df = pd.DataFrame(all_eia_stock_values)

eia_stock_df = eia_stock_raw_df.copy()
eia_stock_df['date'] = pd.to_datetime(eia_stock_df['date'])

product_type_mapping = {
    'EPD0': 'distillate',
}
eia_stock_df['product type'] = (eia_stock_df['product type']
                                        .replace(product_type_mapping))
area_mapping = {
    'R1X': 'padd1a',
}
eia_stock_df['area'] = (eia_stock_df['area']
                               .replace(area_mapping))
process_mapping = {
    'SAE': 'weekly ending stock'
}
eia_stock_df['process'] = (eia_stock_df['process']
                                     .replace(process_mapping))
eia_stock_df = eia_stock_df.drop_duplicates(subset=['date', 'area', 'product type'])
eia_stock_df['area-product type'] = (eia_stock_df['area'] 
                                              + '-' 
                                              + eia_stock_df['product type']
                                              + '-'
                                              + eia_stock_df['process'])

eia_stock_wide_df = eia_stock_df.pivot(index='date', 
                                                       columns=['area-product type'], 
                                                       values='stock')
eia_stock_wide_df.reset_index(inplace=True)
eia_stock_wide_df.to_csv(f'{data}/eia_weekly_stock.csv', index=False)