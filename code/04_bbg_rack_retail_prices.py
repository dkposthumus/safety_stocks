import pandas as pd
from pathlib import Path
# import matplotlib.pyplot as plt
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'gas_crisis')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

bbg_padd1a_rack_df = pd.read_excel(f'{raw_data}/bloomberg_rack_prices.xlsx', 
                               sheet_name='padd1a_rack_import')
# rename vars 
bbg_padd1a_rack_df.rename(
    columns = {
        'Date': 'date',
        'RACKY0G PQN R Index': 'new haven, heating oil dyed',
        'RACKW0G PQN R Index': 'bridgeport, heating oil dyed',
        'RACKF8G PKR R Index': 'burlington, heating oil dyed',
        'RACKD3G PQN R Index': 'boston, heating oil dyed',
        'RACKX0G PQN R Index': 'hartford, heating oil dyed',
        'RACKI3G PQN R Index': 'portland, heating oil dyed',
        'RACKH3G PQN R Index': 'bangor, heating oil dyed',
        'RACKW6G PQN R Index': 'providence, heating oil dyed',
    },
    inplace=True
)  
bbg_padd1a_rack_df['date'] = pd.to_datetime(bbg_padd1a_rack_df['date'])

bbg_padd1a_retail_df = pd.read_excel(f'{raw_data}/state_heating_home_price.xlsx',
                                 sheet_name='residential_heating_oil_import')
bbg_padd1a_retail_df.rename(
    columns = {
        'RSHOAAAC Index': 'padd1a, no. 2 heating oil residential price',
        'RSHOAAAD Index': 'ct., no. 2 heating oil residential price',
        'RSHOAAAH Index': 'ri., no. 2 heating oil residential price',
        'RSHOAAAF Index': 'ma., no. 2 heating oil residential price',
        'RSHOAAAI Index': 'vt., no. 2 heating oil residential price',
        'RSHOAAAE Index': 'me., no. 2 heating oil residential price',
        'RSHOAAAG Index': 'nh., no. 2 heating oil residential price',
    },
    inplace=True
)  
for state in ['padd1a', 'ct.', 'ri.', 'ma.', 'vt.', 'me.', 'nh.']:
    bbg_padd1a_retail_df[f'{state}, no. 2 heating oil residential price'] = (
        bbg_padd1a_retail_df[f'{state}, no. 2 heating oil residential price']*0.01
    )
bbg_padd1a_retail_df['date'] = pd.to_datetime(bbg_padd1a_retail_df['date'])

padd1a_rack_retail_df = pd.merge(bbg_padd1a_rack_df, bbg_padd1a_retail_df,
                                 on='date', how='outer')

padd1a_rack_retail_df.to_csv(f'{data}/bbg_rack_retail.csv', index=False)