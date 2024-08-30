import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'gas_crisis')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 
output = (work_dir / 'output')

master_df = pd.read_csv(f'{data}/safety_stocks_master_detailed.csv')

def plot_time_series(df, vars, title, ylabel, save_path):
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date')
    plt.figure(figsize=(10, 6))
    for var in vars:
        plt.plot(df['date'], df[var], linestyle='-')
    plt.legend(vars, loc='upper left')
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'{output}/{save_path}.png')
    plt.show()

# first plot the real and nominal tariff rates for houston -- linden-buckeye
# calculate the real tariff rate first
master_df['houston - linden-buckeye tariff (real)'] = (master_df['houston - linden-buckeye tariff (nominal)']
                                                      /master_df['price deflator'])
# now plot
tariff_rates = ['houston - linden-buckeye tariff (nominal)', 'houston - linden-buckeye tariff (real)']
plot_time_series(master_df, tariff_rates, 'Houston, TX - Linden, NJ Tariff Rates', '$/Gal',
                 'colonial_linden_rates.png')

