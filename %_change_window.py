import pandas as pd
import os

# Path to the directory containing CSV filesD:\stock_cash_tbt_data\processed_file
# file_path = r'D:\stock_cash_tbt_data\processed_file\04_oct_2024_BHARTIARTL_.csv'
file_path = r'F:\mansukh\project-bhartiairtel_tbt_data_cash\processed_file\04_oct_2024_BHARTIARTL_.csv'
file_name = file_path.split('\\')[-1].split('.')[0]
df = pd.read_csv(file_path, low_memory = False)
df.columns = df.columns.str.lower()
# Ensure the timestamp column is in datetime format
df['time'] = pd.to_datetime(df['timestamp']).dt.time
df['date'] = pd.to_datetime(df['date'],format="%Y-%m-%d")
# Parameters
threshold = 0.005  # 0.5% price change

# Initialize variables
windows = []
start_idx = 0
current_high = df.loc[0, 'ltp']
current_low = df.loc[0, 'ltp']

# Iterate through the DataFrame
for i in range(1, len(df)):
    current_price = df.loc[i, 'ltp']
    
    # Update the high and low
    current_high = max(current_high, current_price)
    current_low = min(current_low, current_price)
    
    # Calculate the range
    range_percentage = (current_high - current_low) / current_low * 100
    
    # Check if range exceeds 0.5%
    if range_percentage >= 0.5: #or (current_price - df.loc[start_idx, 'ltp']) / df.loc[start_idx, 'ltp'] * 100 >= 0.5:
        # Close the current window
        window_data = df.iloc[start_idx:i+1]
        windows.append({
            'date': window_data['date'].iloc[0],
            's_time': window_data['time'].iloc[0],
            'e_time': window_data['time'].iloc[-1],
            's_vtt': window_data['vtt'].iloc[0],
            'e_vtt': window_data['vtt'].iloc[-1],
            'open': window_data['ltp'].iloc[0],
            'high': window_data['ltp'].max(),
            'low': window_data['ltp'].min(),
            'close': window_data['ltp'].iloc[-1],
            'o_cum_sum': window_data['cumm_sum'].iloc[0],
            'h_cum_sum': window_data['cumm_sum'].max(),
            'l_cum_sum': window_data['cumm_sum'].min(),
            'c_cum_sum': window_data['cumm_sum'].iloc[-1],
            'o_sig_cum_sum': window_data['sig_cumm_sum'].iloc[0],
            'h_sig_cum_sum': window_data['sig_cumm_sum'].max(),
            'l_sig_cum_sum': window_data['sig_cumm_sum'].min(),
            'c_sig_cum_sum': window_data['sig_cumm_sum'].iloc[-1],
            's_cumm_sum_w.r.t_hl': window_data['cumm_sum_w.r.t_hl'].iloc[0],
            'e_cumm_sum_w.r.t_hl': window_data['cumm_sum_w.r.t_hl'].iloc[-1],
            's_sig_cumm_sum_w.r.t_hl': window_data['sig_cumm_sum_w.r.t_hl'].iloc[0],
            'e_sig_cumm_sum_w.r.t_hl': window_data['sig_cumm_sum_w.r.t_hl'].iloc[-1],
            'total_volume': window_data['ltq'].sum(),
            'positive_signals': window_data.loc[window_data['signal'] > 0, 'signal'].count(),
            'negative_signals': window_data.loc[window_data['signal'] < 0, 'signal'].count()
        })
        
        # Reset for the next window
        start_idx = i
        current_high = df.loc[i, 'ltp']
        current_low = df.loc[i, 'ltp']

# Convert results to a DataFrame
ohlc = pd.DataFrame(windows)
ohlc['%_change_window'] = ((ohlc['high'] - ohlc['low'])/ohlc['low'])*100
ohlc['%_change'] = ((ohlc['close'] - ohlc['open'])/ohlc['open'])*100
ohlc['e_time'] = pd.to_datetime(ohlc['e_time'],format="%H:%M:%S.%f")
ohlc['s_time'] = pd.to_datetime(ohlc['s_time'],format="%H:%M:%S.%f")
ohlc['net_time'] = ohlc['e_time'] - ohlc['s_time']
# Convert net_time to total seconds
ohlc['net_time_seconds'] = ohlc['net_time'].dt.total_seconds()
ohlc['e_time'] = pd.to_datetime(ohlc['e_time'],format="%H:%M:%S.%f").dt.time
ohlc['s_time'] = pd.to_datetime(ohlc['s_time'],format="%H:%M:%S.%f").dt.time
ohlc['net_vtt'] = ohlc['e_vtt'] - ohlc['s_vtt']
ohlc['net_cum_sum'] = ohlc['c_cum_sum'] - ohlc['o_cum_sum']
ohlc['net_sig_cum_sum'] = ohlc['c_sig_cum_sum'] - ohlc['o_sig_cum_sum']

# Reset index for readability
ohlc.reset_index(inplace=True)

columns = ['date','%_change_window','s_vtt', 'e_vtt','net_vtt', 's_time', 'e_time','net_time_seconds', 'open', 'high',
       'low', 'close','%_change','positive_signals','negative_signals', 'o_cum_sum', 'h_cum_sum', 'l_cum_sum', 'c_cum_sum','net_cum_sum',
       'o_sig_cum_sum', 'h_sig_cum_sum', 'l_sig_cum_sum', 'c_sig_cum_sum','net_sig_cum_sum',
       's_cumm_sum_w.r.t_hl', 'e_cumm_sum_w.r.t_hl', 's_sig_cumm_sum_w.r.t_hl',
       'e_sig_cumm_sum_w.r.t_hl', 'total_volume',
    ]
ohlc = ohlc[columns]

print(ohlc)

# file_path1 = r'D:\stock_cash_tbt_data'
file_path1 = r'F:\mansukh\project-bhartiairtel_tbt_data_cash'
save_path = os.path.join(file_path1)
# Create the save directory if it doesn't exist
if not os.path.exists(save_path):
    os.makedirs(save_path)
file_name = os.path.join(save_path, f"{file_name}_.csv")
ohlc.to_csv(file_name, index=False)
    