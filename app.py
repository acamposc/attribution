import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from fn import attribution, prep, proc, test


'''
Bring data from Bigquery
'''
dataframe = prep.query_data(today = datetime.now().date())

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


'''
Variable init
'''
dataframe_list = []
success_events = []

def test(data):
    data_g = data.groupby(['event_name'])
    dataframe = pd.DataFrame(data_g)        
    '''
    Takes dataframe in and splits it into multiple dataframes
    
    data_groupedby_eventname = dataframe.groupby('event_name')
    dataframe = pd.DataFrame(data_groupedby_eventname)
    '''
    #list groups within dataframe
    data_names = [x for x in dataframe[0]]
    #print(data_names)
    
    #data_process_names = [x for x in dataframe[1]]
    #print(data_process_names)
    

    #Keeping rows that match the business rules.
    data_sets = []
    for name in data_names:
            data_sets.append(data_g.get_group(name))

    #print(data_sets)

    frames = []
    #print(data_sets[0])
    #create multiple dataframes 
    for i in range(len(data_sets)):
            frames.append(pd.DataFrame(data_sets[i]))

    #cantidad de dataframes con success events
    #print(frames)

    mdls = []
    #antes de enviar a modelo hay que eliminar columnas.
    for frame in frames:
            #for earch frame keep columns path, conversions
            mdls.append(frame[['path','conversions']])
            
    #print(len(mdls))

    
    #apply attribution model to dataframes.
    attris = []
    for mdl in mdls:
            attris.append(
                    attribution.markov(mdl)
            )

    #success events as column values.
    mdl_results = []
    for attri, name in zip(attris, data_names):
            #print(len(attri), name)
            for at in attri:
                    at['success_event'] = name
                    mdl_results.append(at)

    #print(mdl_results)

    #split channel and platform from channel_name
    for mdl in mdl_results:
            if mdl.columns.values[0] == 'channel_name':
                new_col = mdl['channel_name'].str.split(' - ', n = 1, expand = True)
                mdl['path'] = new_col[0]
                mdl['platform'] = new_col[1]
                mdl['upload_date'] = datetime.now().date().strftime("%Y-%m-%d")
                mdl['upload_timestamp'] = datetime.now().timestamp() 
            else:
                    if mdl.columns.values[0] == 'channel_from':
                        mdl['upload_date'] = datetime.now().date().strftime("%Y-%m-%d")
                        mdl['upload_timestamp'] = datetime.now().timestamp() 
                    
                    else: pass                       
    
    '''
    Upload data to bigquery
    '''
    
    #prep.upload_to_bigquery(dataframes = mdl_results)
    


    #print(mdl_results)
    print(len(mdl_results))

test(data = dataframe)