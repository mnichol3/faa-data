"""
Parse historical airline schedules for a given airport
"""
import pandas as pd
import os

import utils
import constants



def parse_schedule(airport, years):
    """
    For a given airport, parse its departure & arrival schedule for all months in
    a given year

    Parameters
    ----------
    dirs : dictionary of str
        Dictionary holding various directory paths
    airport : str
        IATA code of the airport to find flights for
    years : list of int or str
        Year(s) to process

    Return
    ------
    out_paths : list of str
        List of paths of the historical schedule files
    """
    out_paths = []
    day_of_wk = [1, 2, 3, 4, 5, 6, 7]

    dir_data = constants.dirs['data']
    dir_hist = constants.dirs['hist']
    dir_lut = constants.dirs['lut']

    airline_lut = pd.read_csv(os.path.join(dir_lut, constants.lut_filenames['airline_id']))

    dow_dict = utils.read_dow_lut()
    dow_dict.pop(9, None)

    if (not isinstance(years, list)):
        years = [str(years)]

    for year in years:
        year = str(year)

        data_files = utils.get_avail_files(dir_data, year)

        # Set up the output directories
        out_dir = os.path.join(dir_hist, airport, year)

        if (not os.path.isdir(out_dir)):
            print('Creating directory {}'.format(out_dir))
            os.makedirs(out_dir)

        for f in data_files:
            print('Processing {}...'.format(f))

            curr_month = f.split('-')[1].split('.')[0]
            f_path = os.path.join(dir_data, f)

            schedule_df = pd.read_csv(f_path, sep=',', header=0)

            # Get a subset of flights that depart or arrive at our airport
            schedule_df = schedule_df.loc[(schedule_df['ORIGIN'] == airport)|
                                          (schedule_df['DEST'] == airport)]

            # Re-arrange column order, drop some
            schedule_df = schedule_df[['DAY_OF_WEEK', 'FL_DATE', 'OP_CARRIER',
                                       'OP_CARRIER_FL_NUM', 'ORIGIN', 'DEST',
                                       'CRS_DEP_TIME', 'CRS_ARR_TIME']]

            # Get list of unique carrier abbreviations and match them to names
            op_list = schedule_df['OP_CARRIER'].unique().tolist()
            airline_lut = airline_lut.loc[airline_lut['Abbrev'].isin(op_list)]
            airline_lut2 = airline_lut.rename(columns={'Abbrev': 'OP_CARRIER'})

            # Merge the airline names into the schedule df, rename the column 'OP_CARRIER_NAME'
            schedule_df = schedule_df.merge(airline_lut2[['Name', 'OP_CARRIER']],
                                            on='OP_CARRIER', how='left')

            schedule_df = schedule_df.rename(columns={'Name': 'OP_CARRIER_NAME'})

            # Move the airline name column between the flight number and origin columns
            col_names = schedule_df.columns.tolist()
            new_cols = col_names[:4] + ['OP_CARRIER_NAME'] + col_names[4:-1]
            schedule_df = schedule_df[new_cols]

            # Create output directory
            curr_out_dir = os.path.join(out_dir, curr_month)
            if (not os.path.isdir(curr_out_dir)):
                print('Creating directory {}'.format(curr_out_dir))
                os.mkdir(curr_out_dir)

            # Get flights by & for each day of the week
            for dow_idx, dow in dow_dict.items():
                dow_df = schedule_df.loc[schedule_df['DAY_OF_WEEK'] == dow_idx]

                f_out = '{}.csv'.format(dow)
                abs_out = os.path.join(curr_out_dir, f_out)

                print('Writing {}'.format(f_out))
                dow_df.to_csv(abs_out, sep=',', header=True, index=False)
                out_paths.append(abs_out)
    return out_paths



def parse_hist_sched_op_name(airport_str, airline_lut, year=None):
    """
    Retroactively add a column containing airline names to historical schedule
    files in a given directory

    Wrapper for _get_airline_ids()

    Parameters
    -----------
    airport_str : str
        Airport of the historical schedule files
    airline_lut : str
        Absolute path of the airline id LUT
    year : int or str, optional
        Specific year of historical schedules to parse
    """
    # Read the airline id lut into a df to pass to _get_airline_ids()
    airline_lut = pd.read_csv(airline_lut, sep=',', header=0)

    root_dir = os.path.join(constants.dirs['hist'], airport_str)

    # If year is given, add it to the end of the base path
    if (year != None):
        root_dir = os.path.join(root_dir, str(year))

    # Walk through the subdirectories in 'base_dir' and process all the files
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            print('Processing {}...'.format(file), end='')
            sched_path = os.path.join(subdir, file)
            new_sched = _get_airline_ids(sched_path, lut_df=airline_lut)
            new_sched.to_csv(sched_path, sep=',', header=True, index=False)
            print('Done!')



def _get_airline_ids(schedule_path, lut_file=None, lut_df=None):
    """
    Retroactively add a column containing airline names to a historical schedule
    file

    Parameters
    -----------
    schedule_path : str
        Absolute path of a historical schedule for an airport
    lut_file : str, optional
        Absolute path of the airline id LUT. MUST be given if 'airline_df' is None.
        Default is None
    lut_df : Pandas DataFrame, optional
        DataFrame containing the airline id LUT.  MUST be given if 'lut_file' is None.
        Default is None

    Return
    -------
    schedule_df : Pandas DataFrame
        schedule_df with added column of airline names corresponding to each flight
    """
    ### Validate the airline ID LUT input and read the file into a dataframe if neccessary
    if (lut_df is None):
        if (lut_file is None):
            raise ValueError("'lut_file' and 'lut_df' cannot both be None")
        else:
            lut_df = pd.read_csv(lut_file, sep=',', header=0)

    ### Read the historical schedule file into a dataframe
    schedule_df = pd.read_csv(schedule_path, sep=',', header=0)

    # Check that the schedule dataframe has not already been modified by this function
    if (schedule_df.columns.shape[0] != 8):
        raise ValueError('Schedule has more than 8 columns and has possibly been modified')

    # Get a list of unique carrier ID's
    op_list = schedule_df['OP_CARRIER'].unique().tolist()

    # Subset the airline ID dataframe based on the airline ID's present in the
    # schedule dataframe
    lut_df = lut_df.loc[lut_df['Abbrev'].isin(op_list)]

    # Rename the 'Abbrev' column to match that of the schedule dataframe column
    # so we can marge on it
    lut_df = lut_df.rename(columns={'Abbrev': 'OP_CARRIER'})

    # Merge the airline dataframe 'Name' column into the schedule dataframe
    schedule_df = schedule_df.merge(lut_df[['Name', 'OP_CARRIER']], on='OP_CARRIER', how='left')

    # Rename the 'Name' column as 'OP_CARRIER_NAME'
    schedule_df = schedule_df.rename(columns={'Name': 'OP_CARRIER_NAME'})

    # Move the airline name column between the flight number and origin columns
    col_names = schedule_df.columns.tolist()
    new_cols = col_names[:4] + ['OP_CARRIER_NAME'] + col_names[4:-1]
    schedule_df = schedule_df[new_cols]

    return schedule_df



def main():

    #==========================================================================#
    #================= Recipe for parse_hist_sched_op_name() ==================#
    #==========================================================================#
    # dirs = constants.dirs
    # luts = constants.lut_filenames
    # apt = 'MRY'
    # airline_lut = os.path.join(dirs['lut'], luts['airline_id'])
    # parse_hist_sched_op_name(apt, airline_lut, year=None)
    ############################################################################
    ############################################################################
    ############################################################################


    #==========================================================================#
    #======================= Recipe for parse_schedule() ======================#
    #==========================================================================#
    apt = 'MRY'
    year = 2018

    parse_schedule(apt, year)
    ############################################################################
    ############################################################################
    ############################################################################




if __name__ == '__main__':
    main()
