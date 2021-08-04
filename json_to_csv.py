import pandas as pd
import json

with open('/Users/bhupi/PycharmProjects/splash_engine/output_files/div_history_hardingloevner_2021_06_23-04:04:33_AM.json', 'r') as f:
    data = json.load(f)
df = pd.json_normalize(data)
print(df.columns)