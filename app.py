import os
from datetime import datetime
import pandas as pd
from fn import attribution, prep, proc


'''
Bring data from Bigquery
'''
dataframe = prep.query_data(today = datetime.now().date())
test(data = dataframe)
'''
data = pd.read_csv("/home/arturplur/projects/scotiabank/out/app/df_values1.csv")

print(dataframe.head(2))


split = proc.group(dataframe = dataframe)
'''

'''
Drop column event_name only for prestamo_exito

dataframe_prestamo = prep.prestamo(data = dataframe)
'''

'''
First and last touch dataframes

first_and_last_touch = prep.steps(data = dataframe_prestamo)
'''

'''
Apply data driven attribution model on dataframe

attr = attribution.markov(data = dataframe_prestamo)

print(attr)


mma_total_conversions = attr[0]
mma_transition_matrix = attr[1]
mma_removal_effects = attr[2]
'''

'''
Add date and timestamp

mma_total_conversions = prep.add_date(data = mma_total_conversions)
mma_transition_matrix = prep.add_date(data = mma_transition_matrix)
mma_removal_effects = prep.add_date(data = mma_removal_effects)
mma_first_touch = prep.add_date(data = first_and_last_touch[0])
mma_last_touch = prep.add_date(data = first_and_last_touch[1])
'''

'''
Add path and platform

mma_total_conversions = prep.channel_name_split(data = mma_total_conversions)
#mma_transition_matrix = prep.channel_to_split(data = mma_transition_matrix)
mma_removal_effects = prep.channel_name_split(data = mma_removal_effects)
mma_first_touch = prep.channel_name_split(data = mma_first_touch)
mma_last_touch = prep.channel_name_split(data = mma_last_touch)
'''

'''
Add name to dataframes

mma_total_conversions.name = 'mma_total_conversions'
mma_transition_matrix.name = 'mma_transition_matrix'
mma_removal_effects.name = 'mma_removal_effects'
mma_first_touch.name = 'mma_first_touch'
mma_last_touch.name = 'mma_last_touch'
'''

'''
Write total_conversions to Bigquery table

dataframes = [
    mma_total_conversions,
    mma_transition_matrix,
    mma_removal_effects,
    mma_first_touch,
    mma_last_touch
]

upload = prep.upload_to_bigquery(dataframes = dataframes)
'''