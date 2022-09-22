import pandas as pd

calendar = pd.read_csv('data/clean/calendar.csv')
taxi_zone_lookup = pd.read_csv('data/clean/taxi_zone_lookup_enhanced.csv')


def gen_cube(data, feature, agg_method):
    t2 = data[['dateID', 'WID', 'CID', 'locationID', feature]].groupby(['dateID', 'WID', 'CID', 'locationID'])
    cube1 = getattr(t2, agg_method)()
    cube1 = cube1.sort_values(by=['dateID', 'locationID'])
    cube2 = cube1[feature].unstack(level=-1).reset_index()
    cube2 = cube2.merge(calendar[['dateID', 'year', 'month', 'day']], how='left', on='dateID')
    cube2 = cube2.set_index(['year', 'month', 'day'])
    t_cube = cube2.drop(columns=['dateID', 'WID', 'CID'])
    location_id_map = taxi_zone_lookup[['LocationID', 'Borough']].set_index('LocationID').to_dict()['Borough']
    mc1 = [x for x in t_cube.columns]
    mc2 = [location_id_map[x] if x in location_id_map else 'undefined' for x in mc1]
    t_cube.columns = [mc2, mc1]
    df = t_cube.reindex(sorted(t_cube.columns), axis=1)
    if feature == 'case_count' or feature == 'trip_distance':
        df = df[[df.columns[0]]]
        df.columns = [feature]
    return df