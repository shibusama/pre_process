import pandas as pd

df1 = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie']
})

df2 = pd.DataFrame({
    'id': [1, 2, 4],
    'score': [90, 85, 88]
})

merged = pd.merge(df1, df2, on='id', how='inner')  # 默认 inner join
print(merged)
