import pandas as pd
import numpy as np

df = pd.read_csv("listings_raw_part_1.csv")

for col in df.columns:
    if np.issubdtype(df[col].dtype, np.number):
        max_val = df[col].max()
        min_val = df[col].min()
        if (max_val > 2147483647) or (min_val < -2147483648):
            print(f"⚠️ Column '{col}' exceeds integer range: {min_val} to {max_val}")
