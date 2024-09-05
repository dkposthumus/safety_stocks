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

api_url = 'https://api.eia.gov/v2/petroleum/pri/refmg/data'
params = {'api_key': 'QyPbWQo92CjndZz8conFD9wb08rBkP4jnDV02TAd'}
header = {
    'frequency': 'monthly',
    'data': [
        'value'
    ],
    'facets': {
        'duoarea': [
            'R1X', 'SCT', 'SMA',
            'SME', 'SNH', 'SRI', 'SVT'
        ],
        'product': [
            'EPM0', 'EPMR'
        ]
    },
    'start': '1983-01',
    'end': '2025-03',
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
all_eia_refiner_gasoline_prices_values = []
while True:
    # Make the API request
    eia_refiner_gasoline_prices = requests.get(
        api_url, params=params, headers={'X-Params': json.dumps(header)}
    )
    # Check if the request was successful
    if eia_refiner_gasoline_prices.status_code == 200:
        eia_refiner_gasoline_prices_data = eia_refiner_gasoline_prices.json()
        eia_refiner_gasoline_prices_series = eia_refiner_gasoline_prices_data['response']['data']
        # Check if there's no more data
        if not eia_refiner_gasoline_prices_series:
            break  # Exit the loop if no more data
        # Append data to the list
        for data_point in eia_refiner_gasoline_prices_series:
            date = data_point['period']
            area = data_point['duoarea']
            product = data_point['product']
            refiner_gasoline_price_nom = data_point['value']
            process = data_point['process']
            all_eia_refiner_gasoline_prices_values.append(
                {
                    'area': area,
                    'date': date,
                    'gas refiner_gasoline price (nominal)': refiner_gasoline_price_nom,
                    'product type': product,
                    'process': process
                }
            )
        header['offset'] += 5000
    else:
        print(f'Failed to retrieve data. Status code: {eia_refiner_gasoline_prices.status_code}')
        break
eia_refiner_gasoline_prices_raw_df = pd.DataFrame(all_eia_refiner_gasoline_prices_values)

eia_refiner_gasoline_prices_df = eia_refiner_gasoline_prices_raw_df.copy()
eia_refiner_gasoline_prices_df['date'] = pd.to_datetime(eia_refiner_gasoline_prices_df['date'])

product_type_mapping = {
    'EPM0': 'total gasoline',
    'EPMR': 'regular gasoline',
}
eia_refiner_gasoline_prices_df['product type'] = (eia_refiner_gasoline_prices_df['product type']
                                        .replace(product_type_mapping))
area_mapping = {
    'SME': 'me.',
    'SMA': 'ma.',
    'SCT': 'ct.',
    'R1X': 'padd1a',
    'SRI': 'ri.',
    'SNH': 'nh.',
    'SVT': 'vt.'
}
eia_refiner_gasoline_prices_df['area'] = (eia_refiner_gasoline_prices_df['area']
                               .replace(area_mapping))
process_mapping = {
    'POR': 'other users',
    'PWG': 'wholesale/resale',
    'PTR': 'company outlets'
}
eia_refiner_gasoline_prices_df['process'] = (eia_refiner_gasoline_prices_df['process']
                                     .replace(process_mapping))
eia_refiner_gasoline_prices_df = eia_refiner_gasoline_prices_df.drop_duplicates(subset=['date', 'area', 'product type'])
eia_refiner_gasoline_prices_df['area-product type'] = (eia_refiner_gasoline_prices_df['area'] 
                                              + '-' 
                                              + eia_refiner_gasoline_prices_df['product type']
                                              + '-'
                                              + eia_refiner_gasoline_prices_df['process']
                                              + ' refiner_gasoline price (nominal)')

eia_refiner_gasoline_prices_wide_df = eia_refiner_gasoline_prices_df.pivot(index='date', 
                                                       columns=['area-product type'], 
                                                       values='gas refiner_gasoline price (nominal)')
eia_refiner_gasoline_prices_wide_df.reset_index(inplace=True)
eia_refiner_gasoline_prices_wide_df.to_csv(f'{data}/eia_refiner_gasoline_prices.csv', index=False)