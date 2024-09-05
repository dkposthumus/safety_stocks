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

api_url = 'https://api.eia.gov/v2/petroleum/cons/prim/data'
params = {'api_key': 'QyPbWQo92CjndZz8conFD9wb08rBkP4jnDV02TAd'}
header = {
    "frequency": "monthly",
    "data": [
        "value"
    ],
    "facets": {
        "duoarea": [
            "R1X",
            "SCT",
            "SMA",
            "SME",
            "SNH",
            "SRI",
            "SVT"
        ],
        "product": [
            "EPD2",
            "EPD2D",
            "EPD2DXL0",
            "EPD2F"
        ]
    },
    "start": "1983-01",
    "end": "2022-03",
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
all_eia_prime_supplier_sales_values = []
while True:
    # Make the API request
    eia_prime_supplier_sales = requests.get(
        api_url, params=params, headers={'X-Params': json.dumps(header)}
    )
    # Check if the request was successful
    if eia_prime_supplier_sales.status_code == 200:
        eia_prime_supplier_sales_data = eia_prime_supplier_sales.json()
        eia_prime_supplier_sales_series = eia_prime_supplier_sales_data['response']['data']
        # Check if there's no more data
        if not eia_prime_supplier_sales_series:
            break  # Exit the loop if no more data
        # Append data to the list
        for data_point in eia_prime_supplier_sales_series:
            date = data_point['period']
            area = data_point['duoarea']
            product = data_point['product']
            prime_supplier_sales = data_point['value']
            all_eia_prime_supplier_sales_values.append(
                {
                    'area': area,
                    'date': date,
                    'prime supplier sales': prime_supplier_sales,
                    'product type': product,
                }
            )
        header['offset'] += 5000
    else:
        print(f'Failed to retrieve data. Status code: {eia_prime_supplier_sales.status_code}')
        break
eia_prime_supplier_sales_raw_df = pd.DataFrame(all_eia_prime_supplier_sales_values)

eia_prime_supplier_sales_df = eia_prime_supplier_sales_raw_df.copy()
eia_prime_supplier_sales_df['date'] = pd.to_datetime(eia_prime_supplier_sales_df['date'])

product_type_mapping = {
    'EPD2': 'no. 2 distillate',
    'EPD2D': 'no. 2 diesel',
    'EPD2DXL0': 'no. 2 diesel low sulfur',
    'EPD2F': 'no. 2 fuel/heating oil',
}
eia_prime_supplier_sales_df['product type'] = (eia_prime_supplier_sales_df['product type']
                                        .replace(product_type_mapping))
area_mapping = {
    'SME': 'me.',
    'SMA': 'ma.',
    'SCT': 'ct.',
    'R1X': 'padd1a',
    'SRI': 'ri.',
    'SNH': 'nh.',
    'SVT': 'vt.',
}
eia_prime_supplier_sales_df['area'] = (eia_prime_supplier_sales_df['area']
                               .replace(area_mapping))
eia_prime_supplier_sales_df = eia_prime_supplier_sales_df.drop_duplicates(subset=['date', 'area', 'product type'])
eia_prime_supplier_sales_df['area-product type'] = (eia_prime_supplier_sales_df['area'] 
                                              + '-' 
                                              + eia_prime_supplier_sales_df['product type']
                                              + '-'
                                              + 'prime supplier sales')

eia_prime_supplier_sales_wide_df = eia_prime_supplier_sales_df.pivot(index='date', 
                                                       columns=['area-product type'], 
                                                       values='prime supplier sales')
eia_prime_supplier_sales_wide_df.reset_index(inplace=True)
eia_prime_supplier_sales_wide_df.to_csv(f'{data}/eia_supplier_sales.csv', index=False)