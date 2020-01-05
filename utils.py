"""
This file holds various functions that do various utlity tasks

Author: Matt Nicholson
1 Jan 2020
"""
import pandas as pd
import os

import constants


def read_dow_lut():
    lut_dir = constants.dirs['lut']
    lut_f = constants.lut_filenames['weekdays']
    path = os.path.join(lut_dir, lut_f)
    lut = pd.read_csv(path, sep=',', header=0)
    lut = lut.set_index('Code')
    dow_dict = lut.to_dict('dict')['Description']

    return dow_dict



def get_avail_files(dir, year):
    """
    Get the available data files for the given year

    Parameters
    ----------
    dir : str
        Directory to query
    year : int or str
    """
    if (isinstance(year, int)):
        year = str(year)

    files = [f for f in os.listdir(dir) if year in f]
    return files



def get_airport_ids(lut_file, airports, key, keywords=None):
    """
    Get a dataframe containing the IDs for specific airports

    Parameters
    -----------
    lut_file : str
        Absolute path of the airport ID LUT (ex: L_AIRPORT_ID.csv)
    airports: str, list of str
        Airport(s) to look up. For best results, use the city name. E.g., when
        looking up Hartsfield-Jackson Atlanta International Airport, pass 'Atlanta'
    key : str
        Either 'location' or 'name'; determines whether to search the airport
        locations or names
    keywords : str, list of str; optional
        Keywords to further-narrow the resulting airports. For example, when searching
        'Atlanta', Atlanta Regional Falcon Field and Hartsfield-Jackson International
        are returned. Using 'International' as a keyword will return only
        Hartsfield-Jackson International
    """
    col_keys = {'location': 'Location',
                'name': 'AirportName'}
    col_name = col_keys[key]

    if (isinstance(airports, str)):
        airports = [airports]

    df = pd.read_csv(lut_file, sep=',', header=0)

    df_result = df.loc[df[col_name].str.contains(airports[0])]

    # Perform the intial search to get all matching airports
    for airport in airports[1:]:
        df_sub = df.loc[df[col_name].str.contains(airport)]
        df_result = pd.concat([df_result, df_sub])

    # If 'keywords' is given, perform another search on the dataframe resulting
    # from the first search
    if (keywords):
        if (isinstance(keywords, str)):
            keywords = [keywords]

        df_rslt_kw = df_result.loc[df_result[col_name].str.contains(keywords[0])]

        for kw in keywords[1:]:
            df_sub = df_result.loc[df_result[col_name].str.contains(kw)]
            df_rslt_kw = pd.concat([df_rslt_kw, df_sub])

        return_df = df_rslt_kw
    else:
        return_df = df_result

    return return_df



def main():
    dirs = constants.dirs
    luts = constants.lut_filenames

    airline_lut = os.path.join(dirs['lut'], luts['airline_id'])

    #get_airline_ids(airline_lut, schedule_df)

    # get_airport_ids(airport_lut, airports_to_find, key='name', keywords=['International', 'Regional'])



if __name__ == '__main__':
    main()
