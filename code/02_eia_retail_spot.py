import pandas as pd
from pathlib import Path
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'safety_stocks')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

eia_retail_prices_df = pd.read_csv(f'{raw_data}/retail_diesel_all.csv', header=6)
eia_retail_prices_df.rename(
    columns = {
        'New England (PADD 1A) All Grades All Formulations Retail Gasoline Prices $/gal': 'padd1a conventional retail (nominal)',
        'New England (PADD 1A) No 2 Diesel Retail Prices $/gal': 'padd1a diesel retail (nominal)',
        'Week of': 'date',
    },
    inplace=True
)
eia_retail_prices_df['date'] = pd.to_datetime(eia_retail_prices_df['date'])

eia_spot_prices_df = pd.read_csv(f'{raw_data}/spot_price_all.csv', header=6)
eia_spot_prices_df.rename(
    columns = {
        'Day': 'date',
        'U.S. Gulf Coast Ultra-Low Sulfur No 2 Diesel Spot Price $/gal': 'usgc low sulfur no. 2 diesel spot (nominal)',
        'New York Harbor Ultra-Low Sulfur No 2 Diesel Spot Price $/gal': 'ny low sulfur no. 2 diesel spot (nominal)',
        'New York Harbor Conventional Gasoline Regular Spot Price FOB $/gal': 'ny conventional spot (nominal)',
        'U.S. Gulf Coast Conventional Gasoline Regular Spot Price FOB $/gal': 'usgc conventional spot (nominal)',
    },
    inplace=True
)
eia_spot_prices_df['date'] = pd.to_datetime(eia_spot_prices_df['date'])

spot_retail_price_df = pd.merge(eia_retail_prices_df, eia_spot_prices_df, on='date', how='outer')

spot_retail_price_df.to_csv(f'{data}/eia_retail_spot_prices.csv', index=False)