import pandas as pd

def construct_index(data):
    top_100 = data.sort_values('market_cap', ascending=False).head(100)
    index_value = top_100['price'].mean()
    return index_value
