import pandas as pd
from pathlib import Path
# import matplotlib.pyplot as plt
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'safety_stocks')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

bbg_padd1a_rack_df = pd.read_excel(f'{raw_data}/bloomberg_rack_prices.xlsx', 
                               sheet_name='padd1a_rack_import')
# rename vars 
bbg_padd1a_rack_df.rename(
    columns = {
        'Date': 'date',
        'RACKY0G PQN R Index': 'new haven, heating oil dyed (nominal)',
        'RACKW0G PQN R Index': 'bridgeport, heating oil dyed (nominal)',
        'RACKF8G PKR R Index': 'burlington, heating oil dyed (nominal)',
        'RACKD3G PQN R Index': 'boston, heating oil dyed (nominal)',
        'RACKX0G PQN R Index': 'hartford, heating oil dyed (nominal)',
        'RACKI3G PQN R Index': 'portland, heating oil dyed (nominal)',
        'RACKH3G PQN R Index': 'bangor, heating oil dyed (nominal)',
        'RACKW6G PQN R Index': 'providence, heating oil dyed (nominal)',
    },
    inplace=True
)  
bbg_padd1a_rack_df['date'] = pd.to_datetime(bbg_padd1a_rack_df['date'])

bbg_padd1a_retail_df = pd.read_excel(f'{raw_data}/state_heating_home_price.xlsx',
                                 sheet_name='residential_heating_oil_import')
bbg_padd1a_retail_df.rename(
    columns = {
        'RSHOAAAC Index': 'padd1a, no. 2 heating oil residential price (nominal)',
        'RSHOAAAD Index': 'ct., no. 2 heating oil residential price (nominal)',
        'RSHOAAAH Index': 'ri., no. 2 heating oil residential price (nominal)',
        'RSHOAAAF Index': 'ma., no. 2 heating oil residential price (nominal)',
        'RSHOAAAI Index': 'vt., no. 2 heating oil residential price (nominal)',
        'RSHOAAAE Index': 'me., no. 2 heating oil residential price (nominal)',
        'RSHOAAAG Index': 'nh., no. 2 heating oil residential price (nominal)',
    },
    inplace=True
)  
for state in ['padd1a', 'ct.', 'ri.', 'ma.', 'vt.', 'me.', 'nh.']:
    bbg_padd1a_retail_df[f'{state}, no. 2 heating oil residential price (nominal)'] = (
        bbg_padd1a_retail_df[f'{state}, no. 2 heating oil residential price (nominal)']*0.01
    )
bbg_padd1a_retail_df['date'] = pd.to_datetime(bbg_padd1a_retail_df['date'])

padd1a_rack_retail_df = pd.merge(bbg_padd1a_rack_df, bbg_padd1a_retail_df,
                                 on='date', how='outer')

padd1a_rack_retail_df.to_csv(f'{data}/bbg_rack_retail.csv', index=False)