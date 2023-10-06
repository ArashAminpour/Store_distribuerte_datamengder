
from haversine import haversine, Unit
from sqlalchemy import create_engine
import pandas as pd

def query_to_dataframe(connection_string, query):
    engine = create_engine(connection_string)
    
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        
    return df

def calculate_distance(position): 
    distance = 0 
    for i in range(1,len(position)): 
        tp = position[i-1]
        next_tp = position[i]
        distance += haversine((tp[0], tp[1]), (next_tp[0], next_tp[1]), unit=Unit.KILOMETERS)

    return distance  
