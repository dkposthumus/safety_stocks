import pandas as pd
from pathlib import Path
# import matplotlib.pyplot as plt
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'safety_stocks')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

# first, let's do distillate inventory
distillate_general_weekly_df = pd.read_csv(f'{raw_data}/distillate_weekly_stock_general.csv', header=6)
# rename each column
distillate_general_weekly_df.rename(
    columns = {
        'New England (PADD 1A) Ending Stocks of Distillate Fuel Oil Mbbl': 'padd1a weekly distillate total stock',        
        'Week of': 'date',
    },
    inplace=True
)
distillate_general_weekly_df = distillate_general_weekly_df[['padd1a weekly distillate total stock', 'date']]
distillate_general_weekly_df['date'] = pd.to_datetime(distillate_general_weekly_df['date'])

distillate_stock_general_df = pd.read_csv(f'{raw_data}/distillate_stock_state.csv', header=6)
for state, new_state in zip(
    ['Maine', 'Massachusetts', 'New Hampshire', 'Rhode Island', 'Vermont', 'Connecticut'],
    ['me.', 'ma.', 'nh.', 'ri.', 'vt.', 'ct.']   
):
    distillate_stock_general_df.rename(
        columns = {
            f'{state} Distillate Fuel Oil Stocks at Refineries Bulk Terminals and Natural Gas Plants Mbbl': f'{new_state} distillate total stock'
        }
    )
distillate_stock_general_df.rename(columns={'Month': 'date'}, inplace=True)
distillate_stock_general_df['date'] = pd.to_datetime(distillate_stock_general_df['date'])

distillate_stock_by_type_df = pd.read_excel(f'{raw_data}/distillate_stock_by_type.xls', sheet_name='Data 1', header=2)
distillate_stock_by_type_df.drop('East Coast (PADD 1) Ending Stocks of Distillate Fuel Oil (Thousand Barrels)', axis=1, inplace=True)
for var, new_var in zip(
    ['Refineries', 'Bulk Terminals', 'Pipelines'], 
    ['refineries', 'bulk terminals','pipelines']
):
    distillate_stock_by_type_df.rename(
        columns = { 
            f'East Coast (PADD 1) Distillate Fuel Oil Stocks at {var} (Thousand Barrels)': f'padd1 distillate stock at {new_var}',
            f'East Coast (PADD 1) Distillate Fuel Oil Stocks in {var} (Thousand Barrels)': f'padd1 distillate stock at {new_var}',
        },
        inplace=True
    )    
distillate_stock_by_type_df.rename(columns={'Date': 'date'}, inplace=True)
distillate_stock_by_type_df['date'] = pd.to_datetime(distillate_stock_by_type_df['date'])

resale_all_df = pd.read_csv(f'{raw_data}/resale_all_gas_price.csv', header=6)
resale_diesel_df = pd.read_csv(f'{raw_data}/resale_diesel_price.csv', header=6)
for df, var, new_var in zip(
    [resale_all_df, resale_diesel_df], 
    ['Total Gasoline', 'No 2 Distillate'],
    ['total', 'diesel fuel']
):
    for area, new_area in zip(
        ['Maine', 'New England (PADD 1A)', 'Rhode Island', 'Vermont', 'Connecticut',
        'Massachusetts', 'East Coast (PADD 1)', 'New Hampshire',],
        ['me.', 'padd1a', 'ri.', 'vt.', 'ct.',
        'ma.', 'padd1', 'nh.']
    ):
        df.rename(
            columns = {
                f'{area} {var} Wholesale/Resale Price by Refiners $/gal': f'{new_area} {new_var} wholesale/resale price'
            },
            inplace=True
        )
    df.rename(columns = {'Month': 'date',}, inplace=True)
    df['date'] = pd.to_datetime(resale_all_df['date'])

inventory_wholesale_resale_df = pd.merge(resale_all_df, resale_diesel_df, 
    on='date', how='outer')
inventory_wholesale_resale_df = pd.merge(inventory_wholesale_resale_df, distillate_general_weekly_df, 
    on='date', how='outer')
inventory_wholesale_resale_df = pd.merge(inventory_wholesale_resale_df, distillate_stock_by_type_df, 
    on='date', how='outer')

inventory_wholesale_resale_df.to_csv(f'{data}/inventory_wholesale_resale.csv', index=False)