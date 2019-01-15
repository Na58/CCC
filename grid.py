class Grid:
    ###store grid boundaries###
    ###given coordinate check if within grid###
    def __init__(self, id, min_long, max_long, min_lat, max_lat):
        self.id = id
        self.min_long = min_long
        self.max_long = max_long
        self.min_lat = min_lat
        self.max_lat = max_lat

    def __eq__(self, other):
        return (self.id == other.id)

    def __hash__(self):
        return hash((self.id, self.min_long, self.max_long,\
            self.min_lat, self.max_lat))

    def check_grid(self, long, lat):
        if self.min_long <= long <= self.max_long and self.min_lat <= lat\
            <= self.max_lat:
            return True
        else:
            return False


import json

def load_grid(log_file, file='melbGrid.json'):
    #retrieve grid information from file
    #return a dictionary with key as grid
    try:
        grid_data = json.load(open(file))
        grid_list = grid_data['features']

        grid_dict = {}
        for grid_item in grid_list:
            grid_info = grid_item['properties']
            grid = Grid(grid_info['id'], grid_info['xmin'], grid_info['xmax'],
                grid_info['ymin'], grid_info['ymax'])
            grid_dict[grid] = 0
        return grid_dict
    except ValueError:
        print "Invalid Grid"
        f = open(log_file, 'a')
        f.write("Invalid Grid File...Ending programme\n")
        f.flush()
        f.close()
        quit()



    

def count_insta(coor_list, grid_dict):
    grids = grid_dict.keys()
    for coor in coor_list:
        (long, lat) = coor
        for g in grids:
            if g.check_grid(long, lat):
                grid_dict[g] += 1
                break
    return grid_dict

def sumup_dict(dict, target = 'r'):
    sum_dict = {}
    key_list = []
    if target == 'r':
        for grid in dict.keys():
            key_list.append(grid.id[0])
    else:
        for grid in dict.keys():
            key_list.append(grid.id[1:])
    for key in set(key_list):
        sum_dict[key] = 0

    if target == 'r':
        for grid in dict.keys():
            sum_dict[grid.id[0]] += dict[grid]
    else:
        for grid in dict.keys():
            sum_dict[grid.id[1:]] += dict[grid]
    return sum_dict

def rank(dict, fhandle, target = 't'):
    rank_list = []
    for grid in dict.keys():
        rank_list.append((dict[grid], grid))
    rank_list.sort(reverse=True)

    if target == 't':
        fhandle.write("---Ranked Grid Box---\n")
        for (count, grid) in rank_list:
            s = grid.id + ': ' + str(count) + 'posts\n'
            fhandle.write(s)
            print s
    if target == 'r':
        fhandle.write("---Ranked Row---\n")
        for (count, row) in rank_list:
            s =  row + '-Row: ' + str(count) + 'posts\n'
            fhandle.write(s)
            print s
    if target == 'c':
        fhandle.write("---Ranked Column---\n")
        for (count, col) in rank_list:
            s = 'Column ' + col + ': ' + str(count) + 'posts\n'
            fhandle.write(s)
            print s


















#
