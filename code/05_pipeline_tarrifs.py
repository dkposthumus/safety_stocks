import pandas as pd
from pathlib import Path
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'safety_stocks')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 

colonial_pipeline_tarifs = {
    'origin': [
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        'houston', 'houston',
        ],
    'destination': [
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        'linden', 'linden-buckeye',
        ],
    'date': [
        '06-30-2024', '06-30-2024', 
        '07-01-2023', '07-01-2023',
        '07-10-2019', '07-10-2019',
        '07-01-2018', '07-01-2018',
        '07-23-2017', '07-23-2017',
        '07-01-2016', '07-01-2016',
        '07-01-2015', '07-01-2015',
        '07-01-2014', '07-01-2014',
        '07-01-2013', '07-01-2013',
        '07-01-2012', '07-01-2012',
        ],
    'tariff rate': [
        303.99, 303.99, 
        303.99, 303.99,
        246.27, 246.27,
        230.59, 230.59,
        220.85, 220.85,
        212.56, 212.56,
        208.82, 208.82,
        197.93, 197.93,
        189.41, 189.41,
        189.41, 189.41,
        ],
}
colonial_pipeline_tarifs_df = pd.DataFrame(colonial_pipeline_tarifs)

# now convert the tariff rate variable into $/gal by dividing by 4200:
# 1 barrel = 42 gallons
# also, tariffs are in CENTS hence dividing by 100 to get $/gal 
colonial_pipeline_tarifs_df['tariff rate'] = (colonial_pipeline_tarifs_df['tariff rate'] / 42) / 100

colonial_pipeline_tarifs_df['date'] = pd.to_datetime(colonial_pipeline_tarifs_df['date'])
colonial_pipeline_tarifs_df['origin_destination'] = (colonial_pipeline_tarifs_df['origin'] 
                                                       + '-' 
                                                       + colonial_pipeline_tarifs_df['destination'])

colonial_pipeline_tarifs_df.to_csv(f'{data}/colonial_pipeline_tariffs_long.csv', index=False)

colonial_pipeline_tarifs_pivoted_df = colonial_pipeline_tarifs_df.pivot(index='date', 
                                         columns='origin_destination', 
                                         values='tariff rate').reset_index()

colonial_pipeline_tarifs_pivoted_df.to_csv(f'{data}/colonial_pipeline_tariffs_wide.csv', index=False)