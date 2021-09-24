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
#import attribution
from datetime import datetime, timedelta


'''
Variable init
'''
dataframe_list = []
success_events = []

def test(data):
    '''
    Lee el excel 
    data = pd.read_csv("/home/arturplur/projects/scotiabank/out/app/df_values1.csv")
    '''

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
                    pass
            
    print(mdl_results)
    print(len(mdl_results))
    
    '''
    paths = []
    conversions = []
    count = []
    last_path = []
    last_channel = []
    #first and last touch
    for mdl in mdls:
        paths.append(mdl.loc[:,'path'])
        conversions.append(mdl.loc[:,'conversions'])
    
    for path in paths:
            count.append(path.str.count('>'))
            last_path.append(path.str.split(' > '))
    
    print(last_path)
    
    for index, value in last_path:
        print(f"Index : {index}, Value : {value}")'''

    '''for last in last_path:
            print(len(last))'''




    '''count = []
                for path in paths:
                        count.append(path.count('>'))
                last_path = []
                for path in paths:
                        last_path.append(path.split(' > '))
                last_channel = [item[-1] for item in last_path]
                last_touch = pd.DataFrame(list(zip(last_channel, count, conversions)), 
                        columns=['channel_name','session_count', 'conversions'])
                
        )'''
    #print(count)
    #print(len(count))



    #send results to bigquery






    '''
    Join lists into a dictionary
    Ouput: a dict with key: product name and value: attribution model results.
    Store the dict in mdls_d
    '''    
    '''def convert_lists_to_dict(name,attri):
        thing1 = iter(name)
        thing2 = iter(attri)
        res_dct = dict(zip(name, attri))
        return res_dct
    mdls_d = convert_lists_to_dict(data_names, attris)
    mdls_l = { key:value for key, value in mdls_d.items() }
    mdls_v = pd.DataFrame.from_dict(mdls_r, orient = "index")

    print(mdls_l)'''
