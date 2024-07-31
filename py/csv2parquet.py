import pandas as pd
import sys

csv_path = sys.argv[1]
pq_path = sys.argv[2]
df = pd.read_csv(csv_path,             
                 on_bad_lines='skip',
                 encoding_errors='ignore',
                 names=['dt', 'cmd', 'cnt'])

df['dt'] = df['dt'].str.replace('^', '')
df['cmd'] = df['cmd'].str.replace('^', '')
df['cnt'] = df['cnt'].str.replace('^', '')

# 'coerce' : 변환할 수 없는 데이터를 만나면 값을 강제로 NaN으로 치환
df['cnt'] = pd.to_numeric(df['cnt'], errors='coerce')
# NaN 값을 원하는 방식으로 처리 (예: 0으로 채우기)
df['cnt'] = df['cnt'].fillna(0).astype(int)
df['cnt'] = df['cnt'].astype(int)

df.to_parquet(f'{pq_path}',partition_cols=['dt'])
