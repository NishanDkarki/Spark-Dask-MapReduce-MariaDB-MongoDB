import sys
import pymysql
import csv
import as1_cfg as cfg


# function queries the database and returns two tuples for games played and runs created
def query_database(arg1):
    argument = arg1

    # Setting up connection to mariadb
    conn = pymysql.connect(
        host=cfg.db5357['host'],
        user=cfg.db5357['user'],
        password=cfg.db5357['password'],
        db=cfg.db5357['db'],
    )

    # cursor to connect to database
    cur = conn.cursor()

    # query to extract number of games played by each player in different position in the selected year
    query_games = "select playerID, yearID,\
                coalesce(sum(G_2b)) as 2B, coalesce(sum(G_ss)) as SS, coalesce(sum(G_p)) as P, coalesce(sum(G_lf)) as LF, coalesce(sum(G_3b)) as 3B,\
                coalesce(sum(G_dh)) as DH, coalesce(sum(G_1b)) as 1B, coalesce(sum(G_rf)) as RF, coalesce(sum(G_c)) as C, coalesce(sum(G_cf)) as CF\
                from appearances\
                    where (yearID = {})\
                    group by playerid".format(argument)

    # query to extract runs created by each player in the selected year
    query_batting = "select playerid, coalesce((((coalesce(sum(b_H),0)+coalesce(sum(b_BB),0)+coalesce(sum(b_HBP),0))/(coalesce(sum(b_AB),0)+coalesce(sum(b_BB),0)+coalesce(sum(b_HBP),0)+coalesce(sum(b_SF),0))) * ((coalesce(sum(b_H),0))+(coalesce(sum(b_2B),0))+(2*(coalesce(sum(b_3B),0)))+(3*(coalesce(sum(b_HR),0))))),0) as RC\
                    from batting\
                    where (yearId = {})\
                    group by playerid".format(argument)

    # executing the queries and assigning output to python tuples
    cur.execute(query_games)
    # stores players games records for each position
    games_records = cur.fetchall()

    cur.execute(query_batting)
    # stores players runs created (RC) records
    runs_records = cur.fetchall()

    # closing connection
    conn.close()

    return games_records, runs_records, argument


# function determines a player position and finds superstars in each position
def superstar_finder(games_records, runs_records, year1):
    # dictionary to store max games played by a player
    p_max_games = {}

    # dictionary to store positions of the player for the max games played by a player
    p_best_pos = {}

    # iterating through player records and find max games played by a player
    for i in range(len(games_records)):
        max_games = 0
        # iterate through records in player record to calculate max games played by the player
        for j in range(2, len(games_records[i])):
            if (games_records[i][j]) > max_games:
                max_games = (games_records[i][j])

        # only process player if the player has games played more than 0 in at least one position
        if max_games != 0:
            # tuple stores (player id and max games played)
            p_max_games[games_records[i][0]] = max_games

        # only process player if the player has games played more than 0 in at least one position
        if max_games != 0:
            # finding positions for which the player played maximum games
            for k in range(2, len(games_records[i])):
                # store the player position if the player played max games in the position
                if (games_records[i][k]) == max_games:
                    # store player position on a tuple with a list for position
                    # handle players that have equal games played in multiple positions
                    if k == 2:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('2B')
                        else:
                            p_best_pos[games_records[i][0]] = ['2B']
                    elif k == 3:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('SS')
                        else:
                            p_best_pos[games_records[i][0]] = ['SS']
                    elif k == 4:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('P')
                        else:
                            p_best_pos[games_records[i][0]] = ['P']
                    elif k == 5:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('LF')
                        else:
                            p_best_pos[games_records[i][0]] = ['LF']
                    elif k == 6:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('3B')
                        else:
                            p_best_pos[games_records[i][0]] = ['3B']
                    elif k == 7:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('DH')
                        else:
                            p_best_pos[games_records[i][0]] = ['DH']
                    elif k == 8:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('1B')
                        else:
                            p_best_pos[games_records[i][0]] = ['1B']
                    elif k == 9:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('RF')
                        else:
                            p_best_pos[games_records[i][0]] = ['RF']
                    elif k == 10:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('C')
                        else:
                            p_best_pos[games_records[i][0]] = ['C']
                    elif k == 11:
                        if games_records[i][0] in p_best_pos:
                            p_best_pos[games_records[i][0]].append('CF')
                        else:
                            p_best_pos[games_records[i][0]] = ['CF']

    # list of all positions to be considered
    positions = ['2B', 'SS', 'P', 'LF', '3B', 'DH', '1B', 'RF', 'C', 'CF']

    print('The superstars for ' + year1 + ' were:')
    # printing out superstar record for each position
    print('PlayerID' + '\t' + '\t' + 'Position' + '\t' + '\t' + '\t' + 'Runs Created')
    # csv_list = []
    # calculating superstars for each position
    for r in range(len(positions)):
        # dictionary to store each qualified player runs created for each position
        superstars = {}
        # iterate through the dictionary and make a new dictionary with qualified player for each position
        # qualified player id and runs created
        for k, v in p_best_pos.items():
            for p in range(len(v)):
                # handles player with multiple position
                if v[p] == positions[r]:
                    for q in range(len(runs_records)):
                        # finding runs record for the player
                        if runs_records[q][0] == k:
                            superstars[k] = runs_records[q][1]

        # only process if a position has at least 1 superstar
        if (len(superstars)) > 0:
            # finding superstar player with maximum runs for a position
            superstar = max(superstars, key=superstars.get)

            # store runs created by the superstar player
            runs = str()
            for x in range(len(runs_records)):
                if runs_records[x][0] == superstar:
                    runs = str(runs_records[x][1])

            # printing superstars
            print(superstar + '\t' + '\t' + positions[r] + '\t' + '\t' + '\t' + '\t' + runs)
            csv_list.append({'PlayerID': superstar, 'Position': positions[r], 'Runs Created': runs, 'Year': year1})
    print('--------------------------------------------------------------------------------')

    fields = ['PlayerID', 'Position', 'Runs Created', 'Year']
    filename = "karki.as1.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(csv_list)


if __name__ == "__main__":
    csv_list = []
    for y in range(1, len(sys.argv)):
        args = sys.argv[y]
        # query_database(args)
        superstar_finder(query_database(args)[0], query_database(args)[1], query_database(args)[2])
