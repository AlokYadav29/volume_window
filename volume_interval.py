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

volume_interval = 20000

loop_count = round(((df['ltq'].sum()/volume_interval)/1)*1)
# print(loop_count)
for i in range(1,loop_count+2)[:]:

    df['ltq_cumsum'] = df['ltq'].cumsum()
    # Find the first index where the condition is True
    condition = df['ltq_cumsum'] > volume_interval*i
    if condition.any():
        idx = condition.idxmax()  # Index of the first True value
        # print(f"Index of the first match: {idx}")
        
        if df.loc[idx-1,'ltq'] != volume_interval*i:
            # Get the matched row
            matched_row = df.loc[idx]
            
            # Insert the duplicate row at idx + 1
            df = pd.concat(
                [df.iloc[:idx + 1], pd.DataFrame([matched_row]), df.iloc[idx + 1:]]
            ).reset_index(drop=True)
            
            df.loc[idx,'ltq'] = matched_row['ltq'] - (matched_row['ltq_cumsum'] - volume_interval*i)
            df.loc[idx+1,'ltq'] = matched_row['ltq_cumsum'] - volume_interval*i
            df.loc[idx,'vtt'] = df.loc[idx,'ltq'] + df.loc[idx-1,'vtt']
            df.loc[idx+1,'vtt'] = df.loc[idx+1,'ltq'] + df.loc[idx,'vtt']
    else:
        print(f"No row found where 'ltq_cumsum' > {volume_interval*i}.")

# Create a volume interval identifier
df['volume_id'] = ((df['ltq_cumsum'] - 1 )// volume_interval).astype(int) + 1

# Group by the volume interval and calculate OHLC
ohlc = df.groupby('volume_id').agg({
    'vtt' : ['first','last'],
    'time': ['first','last'],  # Start time of the interval
    'ltp': ['first', 'max', 'min', 'last'],  # OHLC for price
    'cumm_sum': ['first','max','min','last'],
    'sig_cumm_sum': ['first','max','min','last'],
    'cumm_sum_w.r.t_hl': ['first','last'],
    'sig_cumm_sum_w.r.t_hl': ['first','last'],
    'ltq': 'sum'  # Total volume in the interval
})

# # Flatten MultiIndex columns
# ohlc.columns = ['svtt','evtt','stime', 'etime','open', 'high', 'low', 'close','scum_sum','ecum_sum','ssig_cumm_sum','esig_cumm_sum', 'total_volume']
# Flatten the MultiIndex columns
ohlc.columns = ['_'.join(col).strip() for col in ohlc.columns]

ohlc.rename(columns={
                'vtt_first' : 's_vtt',
                'vtt_last' : 'e_vtt',
                'time_first' : 's_time',
                'time_last' : 'e_time',
                'ltp_first' : 'open',
                'ltp_max' : 'high',
                'ltp_min' : 'low',
                'ltp_last' : 'close',
                'cumm_sum_first' : 'o_cum_sum',
                'cumm_sum_max' : 'h_cum_sum',
                'cumm_sum_min' : 'l_cum_sum',
                'cumm_sum_last' : 'c_cum_sum',
                'sig_cumm_sum_first' : 'o_sig_cum_sum',
                'sig_cumm_sum_max' : 'h_sig_cum_sum',
                'sig_cumm_sum_min' : 'l_sig_cum_sum',
                'sig_cumm_sum_last' : 'c_sig_cum_sum',
                'cumm_sum_w.r.t_hl_first' : 's_cumm_sum_w.r.t_hl',
                'cumm_sum_w.r.t_hl_last' : 'e_cumm_sum_w.r.t_hl',
                'sig_cumm_sum_w.r.t_hl_first' : 's_sig_cumm_sum_w.r.t_hl',   
                'sig_cumm_sum_w.r.t_hl_last' : 'e_sig_cumm_sum_w.r.t_hl',               
                'ltq_sum' : 'total_volume'
            }, inplace=True)

ohlc['e_time'] = pd.to_datetime(ohlc['e_time'],format="%H:%M:%S.%f")
ohlc['s_time'] = pd.to_datetime(ohlc['s_time'],format="%H:%M:%S.%f")

ohlc['net_vtt'] = ohlc['e_vtt'] - ohlc['s_vtt']
ohlc['net_time'] = ohlc['e_time'] - ohlc['s_time']
ohlc['net_cum_sum'] = ohlc['c_cum_sum'] - ohlc['o_cum_sum']
ohlc['net_sig_cum_sum'] = ohlc['c_sig_cum_sum'] - ohlc['o_sig_cum_sum']

# Convert net_time to total seconds
ohlc['net_time_seconds'] = ohlc['net_time'].dt.total_seconds()
ohlc['e_time'] = pd.to_datetime(ohlc['e_time'],format="%H:%M:%S.%f").dt.time
ohlc['s_time'] = pd.to_datetime(ohlc['s_time'],format="%H:%M:%S.%f").dt.time
ohlc['%_change'] = ((ohlc['close'] - ohlc['open'])/ohlc['open'])*100

# Reset index for readability
ohlc.reset_index(inplace=True)

columns = ['total_volume','s_vtt', 'e_vtt','net_vtt', 's_time', 'e_time','net_time_seconds', 'open', 'high',
       'low', 'close','%_change', 'o_cum_sum', 'h_cum_sum', 'l_cum_sum', 'c_cum_sum','net_cum_sum',
       'o_sig_cum_sum', 'h_sig_cum_sum', 'l_sig_cum_sum', 'c_sig_cum_sum','net_sig_cum_sum',
       's_cumm_sum_w.r.t_hl', 'e_cumm_sum_w.r.t_hl', 's_sig_cumm_sum_w.r.t_hl',
       'e_sig_cumm_sum_w.r.t_hl'
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

print("finished_work")