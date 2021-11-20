import pandas as pd


df = pd.read_csv('cleaned_data/collaborations.csv')
df = df.iloc[:,1:]
for index, row in df.iterrows():
    name1 = row['Name1']
    name2 = row['Name2']
    if name1 > name2:
        df.iloc[index, 0] = name2
        df.iloc[index, 1] = name1

df.to_csv('cleaned_data/collaborations.csv', index=False)

        