def state_mean(question, state, entries):
    question_entries = entries[entries["Question"] == question]
    question_state_entries = question_entries[question_entries["LocationDesc"] == state]

    # extract all "Data_Value" column values
    state_values = question_state_entries["Data_Value"]

    result_dict = {
        state: sum(state_values) / float(len(state_values))
    }

    return result_dict

def get_question_states(question, entries):
    question_entries = entries[entries["Question"] == question]

    # return all unique states names from "LocationDesc" column
    return list(set(question_entries["LocationDesc"]))

def states_mean(question, entries):
    states = get_question_states(question, entries)
    results_dict = {}

    for state in states:
        state_result = state_mean(question, state, entries)
        results_dict.update(state_result)

    # return sorted results in ascending oreder by values
    return dict(sorted(results_dict.items(), key = lambda item : item[1]))

def best5(question, questions_best_is_min, questions_best_is_max, entries):
    mean_dict = states_mean(question, entries)

    if question in questions_best_is_min:
        # the results are already sorted ascendingly, return the first 5 entries
        return dict(list(mean_dict.items())[:5])

    if question in questions_best_is_max:
        last5_states = dict(list(mean_dict.items())[-5:])

        # sort the last 5 states descendingly by mean and return them
        return dict(sorted(last5_states.items(), key = lambda item : item[1], reverse=True))

    # the given question is not one of the available questions
    return {}

def worst5(question, questions_best_is_min, questions_best_is_max, entries):
    mean_dict = states_mean(question, entries)

    if question in questions_best_is_min:
        last5_states = dict(list(mean_dict.items())[-5:])

        # sort the last 5 states descendingly by mean and return them
        return dict(sorted(last5_states.items(), key = lambda item : item[1], reverse=True))

    if question in questions_best_is_max:
        # the results are already sorted ascendingly, return the first 5 entries
        return dict(list(mean_dict.items())[:5])

    # the given question is not one of the available questions
    return {}

def global_mean(question, entries):
    question_entries = entries[entries["Question"] == question]

    # extract all "Data_Value" column values
    question_values = list(question_entries["Data_Value"])

    result_dict = {
        "global_mean": sum(question_values) / float(len(question_values))
    }

    return result_dict

def state_diff_from_mean(question, state, entries):
    result_mean = global_mean(question, entries)["global_mean"]
    st_mean = state_mean(question, state, entries)[state]

    return {state: result_mean - st_mean}

def diff_from_mean(question, entries):
    mean_dict = states_mean(question, entries)

    result_dict = {}

    for state in mean_dict.keys():
        state_diff_result = state_diff_from_mean(question, state, entries)
        result_dict.update(state_diff_result)

    return result_dict

def get_categories_names(entries):
    if (not entries.empty) and ("StratificationCategory1" in entries.columns):
        return entries["StratificationCategory1"].dropna().unique().tolist()

    # there are no entries or is no "StratificationCategory1" column
    return []

def get_stratification_category_entries(category, entries):
    return entries[entries["StratificationCategory1"] == category]

def get_stratifications_names(entries):
    if (not entries.empty) and ("Stratification1" in entries.columns):
        return entries["Stratification1"].dropna().unique().tolist()

    # there are no entries or is no "Stratification1" column
    return []

def get_stratification_entries(stratification, entries):
    return entries[entries["Stratification1"] == stratification]

def get_stratification_mean(stratification, entries):
    str_entries = get_stratification_entries(stratification, entries)

    # extract all "Data_Value" column values
    data_values = list(str_entries["Data_Value"])

    # compute the mean of data values
    str_mean = sum(data_values) / float(len(data_values))

    return {stratification: str_mean}

def state_mean_by_category(question, state, entries):
    question_entries = entries[entries["Question"] == question]
    question_state_entries = question_entries[question_entries["LocationDesc"] == state]

    # extract all unique "StratificationCategory1" column values corresponding to the state
    categories = get_categories_names(question_state_entries)

    strat_cat_results = {}
    for strat_cat in categories:
        category_entries = get_stratification_category_entries(strat_cat,
                                                               question_state_entries)

        # extract all unique "Stratification1" column values of the current category
        stratifications = get_stratifications_names(category_entries)

        for strat in stratifications:
            strat_mean_result = get_stratification_mean(strat, category_entries)
            strat_cat_results.update(
                { "('" + strat_cat + "', '" + strat + "')": strat_mean_result[strat]
                }
            )

    # sort the results ascendingly by (category, stratification) key
    strat_cat_results = dict(sorted(strat_cat_results.items()))

    return {state: strat_cat_results}

def mean_by_category(question, entries):
    states = get_question_states(question, entries)
    results_dict = {}

    for state in states:
        results = state_mean_by_category(question, state, entries)

        # extract all the state results dictionary entries
        state_results = results[state]

        # extract the keys (category + stratification) of all the dictionary entries
        keys = state_results.keys()

        for key in keys:
            value = state_results[key]
            new_key = "('" + state + "', " + key[1:]
            results_dict.update({new_key: value})

    # return sorted results in ascending order by state name
    return dict(sorted(results_dict.items()))

class ThreadUtils():
    endpoint_func_map = {
        "state_mean": state_mean,
        "states_mean": states_mean,
        "best5": best5,
        "worst5": worst5,
        "global_mean": global_mean,
        "state_diff_from_mean": state_diff_from_mean,
        "diff_from_mean": diff_from_mean,
        "state_mean_by_category": state_mean_by_category,
        "mean_by_category": mean_by_category
    }
