import pandas as pd
from pathlib import Path
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'gas_crisis')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

diesel_prices_df = pd.read_csv(f'{data}/diesel_prices.csv')
distillate_inventory_df = pd.read_csv(f'{data}/inventory_wholesale_resale.csv')
cpi_df = pd.read_csv(f'{data}/cpi.csv')
cpi_df['date'] = pd.to_datetime(cpi_df['date'])
rack_price_df = pd.read_csv(f'{data}/padd1a_rack_retail.csv')
colonial_tariff_df = pd.read_csv(f'{data}/colonial_tariffs.csv')

master_df = pd.merge(diesel_prices_df, distillate_inventory_df, on='date', how='outer')
#master_df = pd.merge(master_df, cpi_df, on='date', how='outer')
master_df = pd.merge(master_df, rack_price_df, on='date', how='outer')
master_df = pd.merge(master_df, colonial_tariff_df, on='date', how='outer')

master_df['date'] = pd.to_datetime(master_df['date'])
master_df['day'] = master_df['date'].dt.day
master_df['month'] = master_df['date'].dt.month
master_df['year'] = master_df['date'].dt.year

# now let's collapse on month:
vars_to_average = ['padd1a conventional retail (nominal)', 'padd1a diesel retail (nominal)', 
    'usgc low sulfur no. 2 diesel spot (nominal)', 'ny low sulfur no. 2 diesel spot (nominal)', 
    'ny conventional spot (nominal)',
    'usgc conventional spot (nominal)', 'me. total wholesale/resale price',
    'padd1a total wholesale/resale price', 'ri. total wholesale/resale price', 
    'vt. total wholesale/resale price', 'ct. total wholesale/resale price', 
    'ma. total wholesale/resale price', 'nh. total wholesale/resale price',
    'ri. diesel fuel wholesale/resale price', 'ct. diesel fuel wholesale/resale price',
    'nh. diesel fuel wholesale/resale price', 'ma. diesel fuel wholesale/resale price',
    'vt. diesel fuel wholesale/resale price', 'me. diesel fuel wholesale/resale price',
    'padd1a diesel fuel wholesale/resale price', 'padd1a weekly distillate total stock',
    'padd1 distillate stock at refineries', 'padd1 distillate stock at bulk terminals',
    'padd1 distillate stock at pipelines', 'new haven, heating oil dyed', 
    'bridgeport, heating oil dyed', 'burlington, heating oil dyed', 'boston, heating oil dyed',
    'hartford, heating oil dyed', 'portland, heating oil dyed', 'bangor, heating oil dyed',
    'providence, heating oil dyed', 'padd1a, no. 2 heating oil residential price',
    'ct., no. 2 heating oil residential price', 'ri., no. 2 heating oil residential price',
    'ma., no. 2 heating oil residential price', 'vt., no. 2 heating oil residential price',
    'me., no. 2 heating oil residential price', 'nh., no. 2 heating oil residential price',
    'houston - linden-buckeye tariff (nominal)'
]
for var in vars_to_average:
    master_df[var] = master_df.groupby(['year', 'month'])[var].transform('mean')

master_df = master_df[master_df['day'] == 1]
master_df = pd.merge(master_df, cpi_df, on=['date'], how='left')
cpi_anchor = pd.to_datetime('2023-03-01')
fixed_cpi = master_df.loc[master_df['date'] == cpi_anchor, 'all-urban cpi'].values
master_df['price deflator'] = master_df['all-urban cpi'] / fixed_cpi
cols_ffill = ['houston - linden-buckeye tariff (nominal)']
for var in cols_ffill:
    master_df[var] = master_df[var].ffill()

master_df.to_csv(f'{data}/safety_stocks_master_detailed.csv', index=False)

# now we need to create a few variables: 
master_df['usgc/ny average low sulfur no. 2 spot (nominal)'] = (
    master_df['ny low sulfur no. 2 diesel spot (nominal)'] 
    + master_df['usgc low sulfur no. 2 diesel spot (nominal)'])/2
master_df['padd1a diesel retail spread (nominal)'] = (
    master_df['padd1a diesel retail (nominal)'] 
    - master_df['usgc/ny average low sulfur no. 2 spot (nominal)'])

# now let's filter this data to only include certain variables
cols_keep = [
    'date', 'padd1a diesel retail spread (nominal)', 
    'padd1a diesel retail (nominal)', 'usgc low sulfur no. 2 diesel spot (nominal)',
    'padd1a weekly distillate total stock', 'houston - linden-buckeye tariff (nominal)',
    'price deflator'
]
master_filtered_df = master_df[cols_keep]

start_date = pd.to_datetime('01-01-2000')
master_filtered_df = master_filtered_df[master_filtered_df['date']>=start_date]

master_filtered_df.to_csv(f'{data}/safety_stocks_master.csv', index=False)