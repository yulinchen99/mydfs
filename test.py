# %%
import pandas as pd  
df = pd.read_csv('test.csv')
    
# %%
string = df.to_csv(index=False)
# %%
string
# %%
