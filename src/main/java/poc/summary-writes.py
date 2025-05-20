import sys
import math
import numpy as np
import pandas as pd

data   = pd.read_csv('/home/opc/Documents/cursor_projects/tsbechaudit/log/write_thread.csv', sep = ',', header=None)
data.columns = ["ts", "query", "rowcount", "ela"]

def count(x):
    return x.count()

def avg(x):
    return x.mean().round()

def max(x):
    return x.max()

def p90(x):
    return x.quantile(0.90)

def p95(x):
    return x.quantile(0.95)

def p97(x):
    return x.quantile(0.97)

def p98(x):
    return x.quantile(0.98)

def p99(x):
    return x.quantile(0.99)

df_ela_summary = data.groupby('query').agg({'ela': [count, avg, p90, p95, p97, p98, p99, max]})
df_cnt_summary = data.groupby('ts')['ts'].count().mean().round()
max_writes_per_sec = data.groupby('ts')['ts'].count().max()

print("----------------------------------------------")
print(df_ela_summary)
print("----------------------------------------------")
print("Avg writes/sec: " + str(df_cnt_summary))
print("Max writes/sec: " + str(max_writes_per_sec))
