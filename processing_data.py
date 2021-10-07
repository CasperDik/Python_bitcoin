import pandas as pd

import pandas as pd
df = pd.read_csv('../input/transactions.csv')
df['date_time'] = pd.to_datetime(df.timestamp * 1000000)