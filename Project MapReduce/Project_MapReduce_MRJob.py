import sys
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRBaseball(MRJob):

    # Getting year argument from command line
    def configure_args(self):
        super(MRBaseball, self).configure_args()
        self.add_passthru_arg("--year")

    # Mapper reducer steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]

    # mapper to get all appearance value for each player in all different positions
    def mapper1(self, _, line):
        row = line.split(',')
        # Checking first element of each row and yielding as needed
        if row[0] == 'yearID':
            return
        elif row[0] == 'playerID':
            return
        elif row[0].isdigit():
            player_id = row[3]
            # Assigning position values
            if not row[8]:
                p_ggp = 0
            else:
                p_ggp = int(row[8])

            if not row[9]:
                p_ggc = 0
            else:
                p_ggc = int(row[9])

            if not row[10]:
                p_g1b = 0
            else:
                p_g1b = int(row[10])

            if not row[11]:
                p_g2b = 0
            else:
                p_g2b = int(row[11])

            if not row[12]:
                p_g3b = 0
            else:
                p_g3b = int(row[12])

            if not row[13]:
                p_gss = 0
            else:
                p_gss = int(row[13])

            if not row[14]:
                p_glf = 0
            else:
                p_glf = int(row[14])

            if not row[15]:
                p_gcf = 0
            else:
                p_gcf = int(row[15])

            if not row[16]:
                p_grf = 0
            else:
                p_grf = int(row[16])

            if not row[18]:
                p_gdh = 0
            else:
                p_gdh = int(row[18])
            p_year_id = int(row[0])
            # Get data for the specified year only
            if p_year_id == int(self.options.year):
                # yield all position,player id and games played number
                yield (player_id, 'pos_ggp'), p_ggp
                yield (player_id, 'pos_ggc'), p_ggc
                yield (player_id, 'pos_g1b'), p_g1b
                yield (player_id, 'pos_g2b'), p_g2b
                yield (player_id, 'pos_g3b'), p_g3b
                yield (player_id, 'pos_gss'), p_gss
                yield (player_id, 'pos_glf'), p_glf
                yield (player_id, 'pos_gcf'), p_gcf
                yield (player_id, 'pos_grf'), p_grf
                yield (player_id, 'pos_gdh'), p_gdh

        elif (row[0].isdigit()) is False:
            player_id = row[0]
            # Assigning position values
            if not row[6]:
                r_bab = 0
            else:
                r_bab = int(row[6])

            if not row[8]:
                r_bbh = 0
            else:
                r_bbh = int(row[8])

            if not row[9]:
                r_b2b = 0
            else:
                r_b2b = int(row[9])

            if not row[10]:
                r_b3b = 0
            else:
                r_b3b = int(row[10])

            if not row[11]:
                r_bhr = 0
            else:
                r_bhr = int(row[11])

            if not row[15]:
                r_bbb = 0
            else:
                r_bbb = int(row[15])

            if not row[18]:
                r_hbp = 0
            else:
                r_hbp = int(row[18])

            if not row[20]:
                r_bsf = 0
            else:
                r_bsf = int(row[20])

            r_year_id = int(row[1])
            # Get data for the specified year only
            if r_year_id == int(self.options.year):
                # yield all position,player id and runs created
                yield (player_id, 'run_bab'), r_bab
                yield (player_id, 'run_bbh'), r_bbh
                yield (player_id, 'run_b2b'), r_b2b
                yield (player_id, 'run_b3b'), r_b3b
                yield (player_id, 'run_bhr'), r_bhr
                yield (player_id, 'run_bbb'), r_bbb
                yield (player_id, 'run_hbp'), r_hbp
                yield (player_id, 'run_bsf'), r_bsf

    # Reduce to sum id with multiple teams for a year
    def reducer1(self, player_id_pos, games_played):
        player_id, position = player_id_pos
        yield None, (player_id, position, sum(games_played))

    def reducer2(self, _, player_info):
        player_list = []
        player_runs = []

        # Assigning runs and position values to different lists
        for v in player_info:
            if v[1].startswith('pos_') and v[2] > 0:
                player_list.append(v)
            elif v[1].startswith('run_'):
                player_runs.append(v)

        # Stores player and games played by them
        player_dict = {}
        for each in player_list:
            if each[0] not in player_dict:
                player_dict[each[0]] = [each[2]]
            else:
                player_dict[each[0]].append(each[2])

        # Getting max games played by a player
        player_max_played = {}
        for k, v in player_dict.items():
            if k not in player_max_played:
                player_max_played[k] = max(v)

        # Get every qualified position for each player
        max_games_position = {}
        for each in player_list:
            position = []
            max_games = player_max_played.get(each[0])

            for every in player_list:
                if (every[0] == each[0]) and (every[2] == max_games):
                    if every[1] not in position:
                        position.append(every[1])

            if each[0] not in max_games_position:
                max_games_position[each[0]] = [max_games, position]

        # Find runs created for each player
        def find_RC(id1, pr1):
            b_AB = 0
            b_H = 0
            b_2B = 0
            b_3B = 0
            b_HR = 0
            b_BB = 0
            b_HBP = 0
            b_SF = 0
            for e1 in pr1:
                if (e1[0] == id1) and (e1[1] == 'run_bab'):
                    b_AB = e1[2]
                elif (e1[0] == id1) and (e1[1] == 'run_bbh'):
                    b_H = e1[2]
                elif (e1[0] == id1) and (e1[1] == 'run_b2b'):
                    b_2B = e1[2]
                elif (e1[0] == id1) and (e1[1] == 'run_b3b'):
                    b_3B = e1[2]
                elif (e1[0] == id1) and (e1[1] == 'run_bhr'):
                    b_HR = e1[2]
                elif (e1[0] == id1) and (e1[1] == 'run_bbb'):
                    b_BB = e1[2]
                elif (e1[0] == id1) and (e1[1] == 'run_hbp'):
                    b_HBP = e1[2]
                elif (e1[0] == id1) and (e1[1] == 'run_bsf'):
                    b_SF = e1[2]

            if ((b_AB + b_BB + b_HBP + b_SF) * (b_H + b_2B + (2 * b_3B) + (3 * b_HR))) == 0:
                RC = 0
            else:
                RC = (b_H + b_BB + b_HBP) / (b_AB + b_BB + b_HBP + b_SF) * (b_H + b_2B + (2 * b_3B) + (3 * b_HR))
            return RC

        # Get runs created for each player
        player_run_dict = {}
        for each in player_runs:
            if each[0] not in player_run_dict:
                player_run_dict[each[0]] = find_RC(each[0], player_runs)

        # Get runs for each player
        # Player record has to exist in both appearances and batting csv files
        for key in list(max_games_position.keys()):
            if key not in player_run_dict:
                max_games_position.pop(key)

        for key in max_games_position.keys():
            runs = player_run_dict.get(key)
            max_games_position[key].append(runs)

        # Calculates max RC for each position
        max_2b = []
        max_ss = []
        max_p = []
        max_lf = []
        max_3b = []
        max_dh = []
        max_1b = []
        max_rf = []
        max_c = []
        max_cf = []
        for k_r, v_r in max_games_position.items():
            pos_2b = 'pos_g2b'
            if pos_2b in v_r[1]:
                max_2b.append(v_r[2])
            pos_ss = 'pos_gss'
            if pos_ss in v_r[1]:
                max_ss.append(v_r[2])
            pos_p = 'pos_ggp'
            if pos_p in v_r[1]:
                max_p.append(v_r[2])
            pos_lf = 'pos_glf'
            if pos_lf in v_r[1]:
                max_lf.append(v_r[2])
            pos_3b = 'pos_g3b'
            if pos_3b in v_r[1]:
                max_3b.append(v_r[2])
            pos_dh = 'pos_gdh'
            if pos_dh in v_r[1]:
                max_dh.append(v_r[2])
            pos_1b = 'pos_g1b'
            if pos_1b in v_r[1]:
                max_1b.append(v_r[2])
            pos_rf = 'pos_grf'
            if pos_rf in v_r[1]:
                max_rf.append(v_r[2])
            pos_c = 'pos_ggc'
            if pos_c in v_r[1]:
                max_c.append(v_r[2])
            pos_cf = 'pos_gcf'
            if pos_cf in v_r[1]:
                max_cf.append(v_r[2])

        # Trace back to find players with max RC in the position
        output_list = []
        for k1, v1 in max_games_position.items():
            p_2b = 'pos_g2b'
            if max_2b:
                if (v1[2] == max(max_2b)) and (p_2b in v1[1]):
                    output_list.append([k1, '2B', max(max_2b)])
                    yield k1, ('2B', max(max_2b))
            p_ss = 'pos_gss'
            if max_ss:
                if (v1[2] == max(max_ss)) and (p_ss in v1[1]):
                    output_list.append([k1, 'SS', max(max_ss)])
                    yield k1, ('SS', max(max_ss))
            p_p = 'pos_ggp'
            if max_p:
                if (v1[2] == max(max_p)) and (p_p in v1[1]):
                    output_list.append([k1, 'P', max(max_p)])
                    yield k1, ('P', max(max_p))
            p_lf = 'pos_glf'
            if max_lf:
                if (v1[2] == max(max_lf)) and (p_lf in v1[1]):
                    output_list.append([k1, 'LF', max(max_lf)])
                    yield k1, ('LF', max(max_lf))
            p_3b = 'pos_g3b'
            if max_3b:
                if (v1[2] == max(max_3b)) and (p_3b in v1[1]):
                    output_list.append([k1, '3B', max(max_3b)])
                    yield k1, ('3B', max(max_3b))
            p_dh = 'pos_gdh'
            if max_dh:
                if (v1[2] == max(max_dh)) and (p_dh in v1[1]):
                    output_list.append([k1, 'DH', max(max_dh)])
                    yield k1, ('DH', max(max_dh))
            p_1b = 'pos_g1b'
            if max_1b:
                if (v1[2] == max(max_1b)) and (p_1b in v1[1]):
                    output_list.append([k1, '1B', max(max_1b)])
                    yield k1, ('1B', max(max_1b))
            p_rf = 'pos_grf'
            if max_rf:
                if (v1[2] == max(max_rf)) and (p_rf in v1[1]):
                    output_list.append([k1, 'RF', max(max_rf)])
                    yield k1, ('RF', max(max_rf))
            p_c = 'pos_ggc'
            if max_c:
                if (v1[2] == max(max_c)) and (p_c in v1[1]):
                    output_list.append([k1, 'C', max(max_c)])
                    yield k1, ('C', max(max_c))
            p_cf = 'pos_gcf'
            if max_cf:
                if (v1[2] == max(max_cf)) and (p_cf in v1[1]):
                    output_list.append([k1, 'CF', max(max_cf)])
                    yield k1, ('CF', max(max_cf))

        sys.stdout.write(('------------------------------------------------------------------' + '\n').encode('utf-8'))
        sys.stdout.write(('The superstars for ' + (self.options.year) + ' were:' + '\n').encode('utf-8'))
        sys.stdout.write(('------------------------------------------------------------------' + '\n').encode('utf-8'))
        sys.stdout.write(('PlayerID' + '\t').encode('utf-8'))
        sys.stdout.write(('Position' + '\t').encode('utf-8'))
        sys.stdout.write(('Runs Created' + '\n').encode('utf-8'))

        for rec in output_list:
            sys.stdout.write((rec[0] + '\t').encode('utf-8'))
            sys.stdout.write((rec[1] + '\t' + '\t').encode('utf-8'))
            sys.stdout.write(str(rec[2]).encode('utf-8'))
            sys.stdout.write(('\n').encode('utf-8'))

        sys.stdout.write(('------------------------------------------------------------------' + '\n').encode('utf-8'))

        output_list.sort(key=lambda x: x[1])
        sys.stdout.write(('Sorted---------------------------------------------------------' + '\n').encode('utf-8'))
        sys.stdout.write(('The superstars for ' + (self.options.year) + ' were:' + '\n').encode('utf-8'))
        sys.stdout.write(('------------------------------------------------------------------' + '\n').encode('utf-8'))
        sys.stdout.write(('PlayerID' + '\t').encode('utf-8'))
        sys.stdout.write(('Position' + '\t').encode('utf-8'))
        sys.stdout.write(('Runs Created' + '\n').encode('utf-8'))

        for rec in output_list:
            sys.stdout.write((rec[0] + '\t').encode('utf-8'))
            sys.stdout.write((rec[1] + '\t' + '\t').encode('utf-8'))
            sys.stdout.write(str(rec[2]).encode('utf-8'))
            sys.stdout.write(('\n').encode('utf-8'))

        sys.stdout.write(('------------------------------------------------------------------' + '\n').encode('utf-8'))


if __name__ == '__main__':
    MRBaseball.run()
