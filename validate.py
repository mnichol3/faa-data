"""
This file holds functions to cleanse & validate DOT airline on-time
performance data

Author: Matt Nicholson
1 Jan 2020
"""
import pandas as pd
import os


def format_air_lut(dir, fnames, type):
    """
    Format the airport and/or airline ID look up tables.

    The 'Description' column contains the location (city, state/country) and
    airport name. This function creates two new dataframe columns, one for the
    airport location, and another for the name of the airport.

    Parameters
    -----------
    dir : str
        Directory that contains the look up tables to format
    fnames : str, list of str
        Name(s) of the look up tables to process
    type : str
        Either 'airport' or 'airline'. Determines what to name the new LUT columns
    """
    if isinstance(fnames, str):
        fnames = [fnames]

    for lut_file in fnames:
        out_str = 'Formatting {}...'.format(lut_file)
        print(out_str)

        f_path = os.path.join(dir, lut_file)

        df = pd.read_csv(f_path, sep=',', header=0)

        if (list(df.columns) != ['Code', 'Description']):
            continue

        # First, drop the last row, which is NAN because the previous row ends
        # in a comma
        df.drop(df.tail(1).index,inplace=True)

        # Split the 'Description' column
        desc = df['Description'].str.split(':')

        new_col1 = desc.str.get(0).str.strip()
        new_col2 = desc.str.get(1).str.strip()

        # Remove the current 'Description' column from the dataframe
        df = df.drop(['Description'], axis=1)

        # Add the Series to the dataframe as new columns, using
        # the original dataframe's index
        if (type == 'airport'):
            df = df.assign(Location=new_col1.values)
            df = df.assign(AirportName=new_col2.values)
        elif (type == 'airline'):
            df = df.assign(Name=new_col1.values)
            df = df.assign(Abbrev=new_col2.values)

        # Write the new dataframe
        df.to_csv(f_path, sep=',', header=True, index=False)




def main():
    data_dir = '/media/mnichol3/tsb1/data/airline-data'

    #==========================================================================#
    #================ Format airport & aircraft look up tables ================#
    #==========================================================================#
    lut_dir = os.path.join(data_dir, 'LUT')
    airport_luts = ['L_AIRPORT_ID.csv', 'L_AIRPORT_ID.csv']
    airline_luts = ['L_AIRLINE_ID.csv']

    format_air_lut(lut_dir, airport_luts, 'airport')
    format_air_lut(lut_dir, airline_luts, 'airline')
    ############################################################################
    ############################################################################
    ############################################################################



if __name__ == '__main__':
    main()
