import pandas as pd
from pathlib import Path
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'safety_stocks')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

bbg_rack_retail_df = pd.read_csv(f'{data}/bbg_rack_retail.csv')
colonial_tariff_df = pd.read_csv(f'{data}/colonial_pipeline_tariffs_wide.csv')
cpi_df = pd.read_csv(f'{data}/cpi.csv')
eia_refiner_diesel_df = pd.read_csv(f'{data}/eia_refiner_diesel_prices.csv')
eia_refiner_gasoline_df = pd.read_csv(f'{data}/eia_refiner_gasoline_prices.csv')
eia_retail_df = pd.read_csv(f'{data}/eia_retail_prices.csv')
eia_spot_df = pd.read_csv(f'{data}/eia_spot_prices.csv')

for df in [bbg_rack_retail_df, colonial_tariff_df, cpi_df, eia_refiner_diesel_df,
           eia_refiner_gasoline_df, eia_retail_df, eia_spot_df]:
    df['date'] = pd.to_datetime(df['date'])

master_df = pd.merge(bbg_rack_retail_df, colonial_tariff_df, on='date', how='outer')
master_df = pd.merge(master_df, eia_refiner_diesel_df, on='date', how='outer')
master_df = pd.merge(master_df, eia_refiner_gasoline_df, on='date', how='outer')
master_df = pd.merge(master_df, eia_retail_df, on='date', how='outer')
master_df = pd.merge(master_df, eia_spot_df, on='date', how='outer')

master_df['date'] = pd.to_datetime(master_df['date'])
master_df['day'] = master_df['date'].dt.day
master_df['month'] = master_df['date'].dt.month
master_df['year'] = master_df['date'].dt.year

# now let's collapse on month:
for var in master_df.columns.drop(['date', 'day', 'month', 'year']).tolist():
    master_df[var] = master_df.groupby(['year', 'month'])[var].transform(lambda x: x.mean(skipna=True))
master_df = master_df[master_df['day'] == 1]
master_df.to_csv(f'{data}/test.csv', index=False)
master_df = pd.merge(master_df, cpi_df, on='date', how='outer')
master_df.to_csv(f'{data}/test2.csv', index=False)
cpi_anchor = pd.to_datetime('2023-03-01')
fixed_cpi = master_df.loc[master_df['date'] == cpi_anchor, 'all-urban cpi'].values
master_df['price deflator'] = master_df['all-urban cpi'] / fixed_cpi
cols_ffill = [col for col in master_df.columns if 'tariff rate' in col]
for var in cols_ffill:
    master_df[var] = master_df[var].ffill()

master_df.to_csv(f'{data}/safety_stocks_master_detailed.csv', index=False)

# now we need to create a few variables: 
master_df['usgc/ny average low sulfur no. 2 spot (nominal)'] = (
    master_df['ny harbor-no. 2 diesel low sulfur spot price (nominal)'] 
    + master_df['usgc-no. 2 diesel low sulfur spot price (nominal)'])/2
master_df['padd1a diesel retail spread (nominal)'] = (
    master_df['padd1a-no. 2 diesel low sulfur retail price (nominal)'] 
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