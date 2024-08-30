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

api_url = 'https://api.eia.gov/v2/petroleum/pri/spt/data'
params = {'api_key': 'QyPbWQo92CjndZz8conFD9wb08rBkP4jnDV02TAd'}
header = {
    'frequency': 'daily',
    'data': [
        'value'
    ],
    'facets': {
        'duoarea': [
            'RGC', 'Y35NY'
        ],
        'product': [
            'EPD2DXL0', 'EPD2F', 'EPMRU'
        ]
    },
    'start': '1986-01-03',
    'end': '2025-01-01',
    'sort': [
        {
            'column': 'period',
            'direction': 'desc'
        }
    ],
    'offset': 0,
    'length': 5000
}
# Initialize an empty list to store all the data points
all_eia_spot_prices_values = []
while True:
    # Make the API request
    eia_spot_prices = requests.get(
        api_url, params=params, headers={'X-Params': json.dumps(header)}
    )
    # Check if the request was successful
    if eia_spot_prices.status_code == 200:
        eia_spot_prices_data = eia_spot_prices.json()
        eia_spot_prices_series = eia_spot_prices_data['response']['data']
        # Check if there's no more data
        if not eia_spot_prices_series:
            break  # Exit the loop if no more data
        # Append data to the list
        for data_point in eia_spot_prices_series:
            date = data_point['period']
            area = data_point['area-name']
            product = data_point['product']
            spot_price_nom = data_point['value']
            all_eia_spot_prices_values.append(
                {
                    'area': area,
                    'date': date,
                    'gas spot price (nominal)': spot_price_nom,
                    'product type': product
                }
            )
        header['offset'] += 5000
    else:
        print(f'Failed to retrieve data. Status code: {eia_spot_prices.status_code}')
        break
eia_spot_prices_raw_df = pd.DataFrame(all_eia_spot_prices_values)

eia_spot_prices_df = eia_spot_prices_raw_df.copy()
eia_spot_prices_df['date'] = pd.to_datetime(eia_spot_prices_df['date'])

product_type_mapping = {
    'EPD2F': 'no. 2 fuel oil / heating oil',
    'EPD2DXL0': 'no. 2 diesel low sulfur',
    'EPMRU': 'conventional regular gasoline',
    'EPM0R': 'reformulated motor gasoline',
    'EPM0U': 'conventional gasoline'
}
eia_spot_prices_df['product type'] = (eia_spot_prices_df['product type']
                                        .replace(product_type_mapping))
area_mapping = {
    'NA': 'usgc',
    'NEW YORK CITY': 'ny harbor',
}
eia_spot_prices_df['area'] = (eia_spot_prices_df['area']
                               .replace(area_mapping))

eia_spot_prices_df = eia_spot_prices_df.drop_duplicates(subset=['date', 'area', 'product type'])
eia_spot_prices_df['area-product type'] = (eia_spot_prices_df['area'] 
                                              + '-' 
                                              + eia_spot_prices_df['product type']
                                              + ' spot price (nominal)')

eia_spot_prices_wide_df = eia_spot_prices_df.pivot(index='date', 
                                                       columns=['area-product type'], 
                                                       values='gas spot price (nominal)')
eia_spot_prices_wide_df.reset_index(inplace=True)
eia_spot_prices_wide_df.to_csv(f'{data}/eia_spot_prices.csv', index=False)