import pandas as pd
from pathlib import Path
import xlsxwriter
import matplotlib.pyplot as plt
# let's create a set of locals referring to our directory and working directory 
home_dir = Path.home()
work_dir = (home_dir / 'safety_stocks')
data = (work_dir / 'data')
raw_data = (data / 'raw')
code = Path.cwd() 
output = (work_dir / 'output')

bbg_rack_retail_df = pd.read_csv(f'{data}/bbg_rack_retail.csv')
colonial_tariff_df = pd.read_csv(f'{data}/colonial_pipeline_tariffs_wide.csv')
cpi_df = pd.read_csv(f'{data}/cpi.csv')
cpi_df['date'] = pd.to_datetime(cpi_df['date'])
eia_refiner_diesel_df = pd.read_csv(f'{data}/eia_refiner_diesel_prices.csv')
eia_refiner_gasoline_df = pd.read_csv(f'{data}/eia_refiner_gasoline_prices.csv')
eia_retail_df = pd.read_csv(f'{data}/eia_retail_prices.csv')
eia_spot_df = pd.read_csv(f'{data}/eia_spot_prices.csv')
eia_weekly_stock_df = pd.read_csv(f'{data}/eia_weekly_stock.csv')
eia_monthly_stock_df = pd.read_csv(f'{data}/eia_monthly_stock.csv')
eia_supplier_sales_df = pd.read_csv(f'{data}/eia_supplier_sales.csv')

# define a cpi merge function 
def cpi_merge(df):
    df = pd.merge(df, cpi_df, on='date', how='outer')
    cpi_anchor = pd.to_datetime('2023-03-01')
    fixed_cpi = df.loc[df['date'] == cpi_anchor, 'all-urban cpi'].values
    df['price deflator'] = df['all-urban cpi'] / fixed_cpi
    return df 

df_names = [bbg_rack_retail_df, colonial_tariff_df, eia_refiner_diesel_df,
           eia_refiner_gasoline_df, eia_retail_df, eia_spot_df, eia_weekly_stock_df,
           eia_monthly_stock_df, eia_supplier_sales_df]

new_df_names = [
    'bbg_rack_retail_detailed_df', 'colonial_tariff_detailed_df', 
    'eia_refiner_diesel_detailed_df', 'eia_refiner_gasoline_detailed_df', 
    'eia_retail_detailed_df', 'eia_spot_detailed_df', 'eia_weekly_stock_detailed_df',
    'eia_monthly_stock_detailed_df', 'eia_supplier_sales_detailed_df'
]

for i, (original_df, detailed_df_name) in enumerate(zip(df_names, new_df_names)):
    original_df['date'] = pd.to_datetime(original_df['date'])
    original_df['day'] = original_df['date'].dt.day
    original_df['month'] = original_df['date'].dt.month
    original_df['year'] = original_df['date'].dt.year
    
    for var in original_df.columns.drop(['date', 'day', 'month', 'year']).tolist():
        original_df[var] = (original_df.groupby(['year', 'month'])[var]
                            .transform(lambda x: x.mean(skipna=True)))
    original_df = original_df[original_df['day'] == 1]
    original_df = original_df.drop(['day', 'month', 'year'], axis=1)
    original_df = original_df.sort_values(by='date')
    # Assign the modified DataFrame back to the original DataFrame variable
    # Alternatively, assign it directly to the new variable name using globals()
    globals()[detailed_df_name] = original_df.copy()
    globals()[detailed_df_name] = cpi_merge(globals()[detailed_df_name])
    cols_ffill = [col for col in globals()[detailed_df_name].columns if 'tariff' in col]
    for var in cols_ffill:
        globals()[detailed_df_name][var] = globals()[detailed_df_name][var].ffill()
    original_df = globals()[detailed_df_name].copy()
    original_df.drop(['price deflator', 'all-urban cpi'], axis=1, inplace=True)
    df_names[i] = original_df.copy()

# Now, the DataFrames have been properly updated, and you can proceed with merging
master_df = pd.merge(df_names[0], df_names[1], on='date', how='outer')
for df in df_names[2:]:
    master_df = pd.merge(master_df, df, on='date', how='outer')

master_df = cpi_merge(master_df) 

# master_df.to_csv(f'{data}/safety_stocks_master_detailed.csv', index=False)

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
    'padd1a-no. 2 diesel low sulfur retail price (nominal)', 
    'usgc-no. 2 diesel low sulfur spot price (nominal)',
    'padd1a-distillate-weekly ending stock', 'houston-linden-buckeye tariff rate (nominal)',
    'price deflator'
]
cols_ffill = [col for col in master_df.columns if col != 'date']
for var in cols_ffill:
    master_df[var] = master_df[var].ffill()
master_filtered_df = master_df[cols_keep]

start_date = pd.to_datetime('01-01-2000')
master_filtered_df = master_filtered_df[master_filtered_df['date']>=start_date]

master_filtered_df.to_csv(f'{data}/safety_stocks_master.csv', index=False)

# now let's make a 'true' detailed spreadsheet, 
# with different sheets corresponding to the different sets of variables
columns_to_exclude = ['all-urban cpi', 'price deflator']
eia_monthly_stock_detailed_df = eia_monthly_stock_detailed_df.drop(columns=columns_to_exclude)
eia_stock_df = pd.merge(eia_weekly_stock_detailed_df, 
                        eia_monthly_stock_detailed_df, on='date', how='outer')

df_names = [master_filtered_df, bbg_rack_retail_detailed_df, colonial_tariff_detailed_df, 
            eia_refiner_diesel_detailed_df, eia_refiner_gasoline_detailed_df, 
            eia_retail_detailed_df, eia_spot_detailed_df,
            eia_stock_df, eia_supplier_sales_detailed_df]
sheet_names = ['master filtered', 'rack_retail prices, bbg', 'colonial pipeline rates',
               'refiner diesel prices, eia', 'refiner gasoline prices, eia',
               'retail prices, eia', 'spot prices, eia',
               'stock, eia', 'supplier sales, eia']

file_name = f'{data}/safety_stocks_master_detailed.xlsx'
if len(df_names) != len(sheet_names):
    print("The number of DataFrames and sheet names do not match.")
else:
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        for df, sheet_name in zip(df_names, sheet_names):
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"File saved successfully as {file_name}")

# define a plotting function
def plot_time_series(df, vars, title, ylabel, save_path):
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date')
    plt.figure(figsize=(10, 6))
    for var in vars:
        # Plot the raw data
        plt.plot(df['date'], df[var], linestyle='-', 
                 linewidth=1, label=var)
        # Plot the 4-month moving average
        df[f'{var}_4m_MA'] = df[var].rolling(window=4, min_periods=1).mean()
        '''plt.plot(df['date'], df[f'{var}_4m_MA'], linestyle='-', 
                 linewidth=1.5, label=f'{var} (4-Month MA)')'''
    plt.legend(loc='upper left')
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'{output}/{save_path}.png')
    plt.show()
# now plot!
df_names = [master_filtered_df, bbg_rack_retail_detailed_df, colonial_tariff_detailed_df, 
            eia_refiner_diesel_detailed_df, eia_refiner_gasoline_detailed_df, 
            eia_retail_detailed_df, eia_spot_detailed_df,
            eia_stock_df, eia_supplier_sales_detailed_df]
sheet_names = ['master filtered', 'rack_retail prices, bbg', 'colonial pipeline rates',
               'refiner diesel prices, eia', 'refiner gasoline prices, eia',
               'retail prices, eia', 'spot prices, eia',
               'stock, eia', 'supplier sales, eia']

for df_plot, df_title in zip(df_names, sheet_names):
    if id(df_plot) in [id(colonial_tariff_detailed_df), id(eia_spot_detailed_df), 
                       id(bbg_rack_retail_detailed_df), id(eia_refiner_diesel_detailed_df), 
                       id(eia_refiner_gasoline_detailed_df), id(eia_retail_detailed_df)]:
        vars_plot = [col.replace(' (nominal)', '') for col in df_plot.columns if 'nominal' in col]
        for var in vars_plot:
            nominal_var = f'{var} (nominal)'
            # Ensure nominal_var exists and handle missing values carefully
            if nominal_var in df_plot.columns:
                df_plot[f'{var} (real)'] = (df_plot[nominal_var].dropna() 
                                          / df_plot['price deflator'].dropna())
        vars_plot_real = [var for var in df_plot.columns if '(real)' in var]
        #print(vars_plot_real)
        if vars_plot_real:
            if id(df_plot) in [id(colonial_tariff_detailed_df), id(eia_spot_detailed_df)]:
                plot_time_series(df_plot, vars_plot_real, f'{df_title.title()} Prices', 
                             '$/Gal', f'{df_title}_real_prices')
            if id(df_plot) == id(bbg_rack_retail_detailed_df):
                dyed_columns = [col for col in vars_plot_real 
                                if 'heating oil dyed' in col]
                if dyed_columns:
                    plot_time_series(df_plot, dyed_columns, f'{df_title.title()} Rack Prices',
                                 '$/Gal', f'{df_title}_real_prices')
                residential_columns = [col for col in vars_plot_real
                                       if 'heating oil residential price' in col]
                if residential_columns:
                    plot_time_series(df_plot, residential_columns, 
                                 f'{df_title.title()} Residential Home Heating Prices',
                                    '$/Gal', f'{df_title}_real_prices')
            if id(df_plot) == id(eia_refiner_diesel_detailed_df):
                retail_columns = [col for col in vars_plot_real 
                                if 'retail' in col]
                if retail_columns:
                    plot_time_series(df_plot, retail_columns, 'Retail Sales Refiner Prices, Distillate',
                                 '$/Gal', f'{df_title}_real_prices')
                wholesale_resale_columns = [col for col in vars_plot_real
                                       if 'wholesale/resale' in col]
                if wholesale_resale_columns:
                    plot_time_series(df_plot, wholesale_resale_columns, 
                                 'Wholesale/Resale Refiner Prices, Distillate',
                                    '$/Gal', f'{df_title}_real_prices')
            if id(df_plot) == id(eia_refiner_gasoline_detailed_df):
                company_outlets_columns = [col for col in vars_plot_real 
                                if 'company outlets' in col and 'total' in col]
                if company_outlets_columns:
                    plot_time_series(df_plot, company_outlets_columns, 
                                     'Company Outlets Refiner Prices, Total Gasoline',
                                 '$/Gal', f'{df_title}_real_prices')
                other_users = [col for col in vars_plot_real
                                       if 'other users' in col and 'total' in col]
                if other_users:
                    plot_time_series(df_plot, other_users, 
                                 'Other Users Refiner Prices, Total Gasoline',
                                    '$/Gal', f'{df_title}_real_prices')
                wholesale_resale_columns = [col for col in vars_plot_real
                                       if 'wholesale/resale' in col and 'total' in col]
                if wholesale_resale_columns:
                    plot_time_series(df_plot, wholesale_resale_columns, 
                                 'Wholesale/Resale Refiner Prices, Total Gasoline',
                                    '$/Gal', f'{df_title}_real_prices')
    if id(df_plot) == id(eia_stock_df):
        vars_plot = [col for col in df_plot.columns if 'stock' in col]
        distillate_column = [col for col in vars_plot if 'distillate' in col]
        if distillate_column:
            plot_time_series(df_plot, distillate_column, 'Monthly Ending Distillate Stocks', 
                         'Barrels', f'{df_title}_stocks')
        conventional_mg_column = [col for col in vars_plot if 'conventional mg' in col]
        if conventional_mg_column:
            plot_time_series(df_plot, conventional_mg_column, 
                         'Monthly Ending Conventional Motor Gasoline Stocks', 'Barrels', f'{df_title}_stocks')
        reformulated_mg_column = [col for col in vars_plot if 'rfmg' in col]
        if reformulated_mg_column:
            plot_time_series(df_plot, reformulated_mg_column, 
                         'Monthly Ending Reformulated Motor Gasoline Stocks', 'Barrels', 
                         f'{df_title}_stocks')
    if id(df_plot) == id(eia_supplier_sales_detailed_df):
        vars_plot = [col for col in df_plot.columns if 'sales' in col]
        distillate_column = [col for col in vars_plot if 'distillate' in col]
        if distillate_column:
            plot_time_series(df_plot, distillate_column, 'Supplier Sales Distillate', 
                         'Barrels', f'{df_title}_sales')
        fuel_heating_column = [col for col in vars_plot if 'fuel/heating oil' in col]
        if fuel_heating_column:
            plot_time_series(df_plot, fuel_heating_column, 
                         'Supplier Sales Fuel/Heating Oil', 'Barrels', f'{df_title}_sales')
        diesel_column = [col for col in vars_plot if ('diesel' in col) and ('low sulfur' not in col)]
        if diesel_column:
            plot_time_series(df_plot, diesel_column, 
                         'Supplier Sales Diesel', 'Barrels', 
                         f'{df_title}_sales')
        diesel_low_sulfur_column = [col for col in vars_plot if 'diesel' in col and 'low sulfur' in col]
        if diesel_low_sulfur_column:
            plot_time_series(df_plot, diesel_low_sulfur_column, 
                         'Supplier Sales Diesel Low Sulfur', 'Barrels', 
                         f'{df_title}_sales')
