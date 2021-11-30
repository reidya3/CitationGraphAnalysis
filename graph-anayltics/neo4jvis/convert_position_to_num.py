import pandas as pd

df = pd.read_csv('graph-anayltics/cleaned_data/unique_authors.csv')
unique_positions = df['Job_Title'].unique()
unique_diff = dict(zip(list(unique_positions), range(0, len(unique_positions))))
df['Job_title_num'] = df['Job_Title'].replace(unique_diff)

df.to_csv("graph-anayltics/cleaned_data/neo4jvis/unique_authors.csv",index=False)


