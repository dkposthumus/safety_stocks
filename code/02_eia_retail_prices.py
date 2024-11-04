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

api_url = 'https://api.eia.gov/v2/petroleum/pri/gnd/data'
params = {'api_key': 'QyPbWQo92CjndZz8conFD9wb08rBkP4jnDV02TAd'}

header = {
    "frequency": "weekly",
    "data": ["value"],
    "facets": {
        "duoarea": ["R1X"],
        "product": ["EPD2D", "EPD2DXL0", "EPM0", "EPM0R", "EPM0U"]
    },
    "start": "1990-08-20",
    "end": "2025-01-01",
    "sort": [{"column": "period", "direction": "desc"}],
    "offset": 0,  # Starting offset
    "length": 5000  # Max rows per request
}
# Initialize an empty list to store all the data points
all_eia_retail_prices_values = []
while True:
    # Make the API request
    eia_retail_prices = requests.get(
        api_url, params=params, headers={'X-Params': json.dumps(header)}
    )
    # Check if the request was successful
    if eia_retail_prices.status_code == 200:
        eia_retail_prices_data = eia_retail_prices.json()
        #with open('test.json', 'w') as json_file:
            #json.dump(eia_retail_prices_data, json_file, indent=4)
        eia_retail_prices_series = eia_retail_prices_data['response']['data']
        # Check if there's no more data
        if not eia_retail_prices_series:
            break  # Exit the loop if no more data
        # Append data to the list
        for data_point in eia_retail_prices_series:
            date = data_point['period']
            state = data_point['area-name']
            product = data_point['product']
            retail_price_nom = data_point['value']
            all_eia_retail_prices_values.append(
                {
                    'state': state,
                    'date': date,
                    'gas retail price (nominal)': retail_price_nom,
                    'product type': product
                }
            )
        header['offset'] += 5000
    else:
        print(f'Failed to retrieve data. Status code: {eia_retail_prices.status_code}')
        break
eia_retail_prices_raw_df = pd.DataFrame(all_eia_retail_prices_values)

eia_retail_prices_df = eia_retail_prices_raw_df.copy()
eia_retail_prices_df['date'] = pd.to_datetime(eia_retail_prices_df['date'])

# now i want to do a mass renaming of product types. 
product_type_mapping = {
    'EPD2D': 'no. 2 diesel',
    'EPD2DXL0': 'no. 2 diesel low sulfur',
    'EPM0': 'total gasoline',
    'EPM0R': 'reformulated motor gasoline',
    'EPM0U': 'conventional gasoline'
}
eia_retail_prices_df['product type'] = (eia_retail_prices_df['product type']
                                        .replace(product_type_mapping))
state_mapping = { 
    'PADD 1A': 'padd1a',
}
eia_retail_prices_df['state'] = (eia_retail_prices_df['state']
                                 .replace(state_mapping))
eia_retail_prices_df = eia_retail_prices_df.drop_duplicates(subset=['date', 'state', 'product type'])
eia_retail_prices_df['state-product type'] = (eia_retail_prices_df['state'] 
                                              + '-' 
                                              + eia_retail_prices_df['product type']
                                              + ' retail price (nominal)')
eia_retail_prices_wide_df = eia_retail_prices_df.pivot(index='date', 
                                                       columns=['state-product type'], 
                                                       values='gas retail price (nominal)')
eia_retail_prices_wide_df.reset_index(inplace=True)
eia_retail_prices_wide_df.to_csv(f'{data}/eia_retail_prices.csv', index=False)