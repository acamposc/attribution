import os
import pandas as pd
import pychattr
from pychattr.channel_attribution import MarkovModel

def markov(data):
    '''
    features for the model.
    '''
    path_feature="path"
    conversion_feature="conversions"
    null_feature=None
    revenue_feature=None
    cost_feature=None
    separator=">"
    k_order=1
    n_simulations=10000
    max_steps=None
    return_transition_probs=True
    random_state=26


    mm = MarkovModel(path_feature=path_feature,
                    conversion_feature=conversion_feature,
                    null_feature=null_feature,
                    revenue_feature=revenue_feature,
                    cost_feature=cost_feature,
                    separator=separator,
                    k_order=k_order,
                    n_simulations=n_simulations,
                    max_steps=max_steps,
                    return_transition_probs=return_transition_probs,
                    random_state=random_state)

    '''
    Fit model to data
    '''
    mm.fit(data)
    
    '''
    Apply model and save as csv 

    - mma_total_conversions has total conversions (direct and assisted).
    - mma_transition_matrix has the probability of assistance from one step to the next.
    - mma_removal_effects has the importance of a step.
    '''
    mma_total_conversions = mm.attribution_model_
    #mma_total_conversions.to_csv("mma_total_conversions.csv", index = False)

    mma_transition_matrix = mm.transition_matrix_
    #mma_transition_matrix.to_csv("mma_transition_matrix.csv", index = False)

    mma_removal_effects = mm.removal_effects_
    #mma_removal_effects.to_csv("mma_removal_effects", index = False)

    return [
            mma_total_conversions,
            mma_transition_matrix,
            mma_removal_effects
            ]


