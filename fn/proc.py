'''
Processing dataframe
- Group by event_name.
- Split it into multiple dataframes.
- Drop unnecesary columns for the model.
- Apply attribution model to each dataframe.
- Hypothesis, each product might have it's own model.
- Then merge results into a single dataframe.
'''

import os
import pandas as pd
import numpy as np
from fn import prep, attribution

'''
Variable init
'''
dataframe_list = []
success_events = []

def group(dataframe):
    '''
    Takes dataframe in and splits it into multiple dataframes
    '''
    data_groupedby_eventname = dataframe.groupby('event_name')
    dataframe = pd.DataFrame(data_groupedby_eventname)

    #list groups within dataframe
    data_names = [x for x in dataframe[0]]

    #Keeping rows that match the business rules.
    sets = []
    for name in data_names:
            sets.append(data_groupedby_eventname.get_group(name))

    frames = []
    #print(sets[0])
    ''' create multiple dataframes '''
    for i in range(len(sets)):
            frames.append(pd.DataFrame(sets[i]))

    #cantidad de dataframes con success events
    #frames = pd.DataFrame(frames)
    print(type(frames))

    mdls = []
    #antes de enviar a modelo hay que eliminar columnas.
    for frame in frames:
            #for earch frame keep columns path, conversions
            mdls.append(frame[['path','conversions']])
    
    #enviar dataframes a modelo atribucion
    attris = []
    for mdl in mdls:
            attris.append(
                    attribution.markov(mdl)
            )

    '''
    Join lists into a dictionary
    Ouput: a dict with key: product name and value: attribution model results.
    Store the dict in mdls_d
    '''    
    def convert_lists_to_dict(name,attri):
        thing1 = iter(name)
        thing2 = iter(attri)
        res_dct = dict(zip(name, attri))
        return res_dct
    mdls_d = convert_lists_to_dict(data_names, attris)

    '''mdls_r = { key:value for key, value in mdls_d.items() }'''

    '''test_mdl = mdls_d.get('quiero_prestamo_exito')
    print(len(test_mdl))
    test_mdl[0]['event'] = 'quiero_prestamo_exito'
    print(test_mdl[0])'''

    models = []
    events = []
    def add_event_as_column(model_results):
        for key, value in model_results.items():
                models = model_results.get(key)
                events = model_results.keys()
    add_event_as_column(mdls_d)

    '''def merge_events_models(events, models):
        pd.DataFrame(list(zip(events,models)))
    merged = merge_events_models(events, models)

    print(len(merged))
    print(merged)'''

    print(mdls_d)


