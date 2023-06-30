# Importing all required libraries
import sys
import as4_cfg as cfg
import dask.dataframe as dd


# Function to get the csv data and process to get required output
def get_data(arg1):
    # Argument passed for year through terminal
    argument = int(arg1)
    # print(argument)

    batting_r = dd.read_csv(cfg.db5357['batting'], dtype={'CS': 'float64',
                                                          'GIDP': 'float64',
                                                          'RBI': 'float64',
                                                          'SB': 'float64',
                                                          'SO': 'float64',
                                                          'lgID': 'object'})
    batting_f = batting_r.drop(columns=['stint', 'G', 'R', 'RBI', 'SB', 'CS', 'SO', 'IBB', 'SH', 'GIDP', 'lgID'])
    batting_f = batting_f.fillna(0)
    batting_f = batting_f.loc[batting_f['yearID'] == argument]
    batting_f.compute()
    batting_f = batting_f.groupby(['playerID'])[['AB', 'H', '2B', '3B', 'HR', 'BB', 'HBP', 'SF']].sum()
    batting_f.compute()
    batting_f["TB"] = batting_f['H'] + batting_f['2B'] + batting_f['3B'] * 2 + batting_f['HR'] * 3
    batting_f["OBP"] = (batting_f['H'] + batting_f['BB'] + batting_f['HBP']) / (
            batting_f['AB'] + batting_f['BB'] + batting_f['HBP'] + batting_f['SF'])
    batting_f = batting_f.fillna(0)
    batting_f["RC"] = (batting_f['TB'] * batting_f['OBP'])
    batting_f.compute()
    # batting_f = batting_f.reset_index(drop=True)
    # print(batting_f.tail())

    appearances_r = dd.read_csv(cfg.db5357['appearances'], dtype={'GS': 'float64',
                                                                  'G_defense': 'float64',
                                                                  'G_dh': 'float64',
                                                                  'G_ph': 'float64',
                                                                  'G_pr': 'float64',
                                                                  'lgID': 'object'})
    # print(appearances_r.describe())
    appearances_f = appearances_r.drop(
        columns=['G_all', 'GS', 'G_batting', 'G_defense', 'G_ph', 'G_pr', 'G_of', 'lgID'])
    appearances_f = appearances_f.fillna(0)
    appearances_f = appearances_f.loc[appearances_f['yearID'] == argument]
    appearances_f.compute()
    appearances_f = appearances_f.groupby(['playerID'])[
        ['G_p', 'G_c', 'G_1b', 'G_2b', 'G_3b', 'G_ss', 'G_lf', 'G_cf', 'G_rf', 'G_dh']].sum()
    appearances_f.compute()
    appearances_f = appearances_f.reset_index()
    appearances_f = appearances_f.melt(id_vars=['playerID'], var_name='position', value_name='runs',
                                       value_vars=['G_p', 'G_c', 'G_1b', 'G_2b', 'G_3b', 'G_ss', 'G_lf', 'G_cf', 'G_rf',
                                                   'G_dh'])
    appearances_f.compute()
    appearances_f = appearances_f.join(batting_f, on='playerID', how='inner')
    appearances_f.compute()
    appearances_f = appearances_f.drop(
        columns=['AB', 'H', '2B', '3B', 'HR', 'BB', 'HBP', 'SF', 'TB', 'OBP'])
    appearances_f = appearances_f[appearances_f.runs != 0]
    appearances_f = appearances_f.reset_index(drop=True)

    max1 = appearances_f.groupby('playerID')['runs'].max()
    appearances_f['max_run'] = appearances_f['playerID'].map(max1)
    appearances_f.compute()
    appearances_f = appearances_f[appearances_f.runs == appearances_f.max_run]
    appearances_f = appearances_f.reset_index(drop=True)

    max2 = appearances_f.groupby('position')['RC'].max()
    appearances_f['RunsCreated'] = appearances_f['position'].map(max2)
    appearances_f.compute()

    appearances_f = appearances_f[appearances_f.RC == appearances_f.RunsCreated]
    appearances_f.compute()
    appearances_f = appearances_f.drop(columns=['runs', 'RC', 'max_run'])
    appearances_f = appearances_f.reset_index(drop=True)
    appearances_f.compute()

    print('---------------------------------------------------------------------------------')
    print('The superstars for ' + str(argument) + ' were:')
    print('---------------------------------------------------------------------------------')
    print(appearances_f.head(500))
    print('---------------------------------------------------------------------------------')


if __name__ == "__main__":
    for y in range(1, len(sys.argv)):
        args = sys.argv[y]
    get_data(args)
