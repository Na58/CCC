from mpi4py import MPI
import grid as G
import json
import time

def generate_fpoint(fname, split_size):
    #consume the first line
    fhandle = open(fname)
    fhandle.readline()
    #read through the file
    #output [(line_start, line_end)]
    file_record = []
    while True:
        start_pos = fhandle.tell()
        data = fhandle.readline()
        if data:
            stop_pos = fhandle.tell()
            file_record.append((start_pos, stop_pos))
        else:
            break
    fhandle.close()

    #divide the line points based on split size
    l = len(file_record) / split_size
    point_list = []
    slice1 = 0
    slice2 = l
    for i in xrange(split_size):
        if i == split_size - 1:
            point_list.append(file_record[slice1:])
        else:
            point_list.append(file_record[slice1:slice2])
            slice1 = slice2
            slice2 = slice2 + l

    #retrieve the start and end point for each task
    task_list = []
    for lis in point_list:
        (start, y) = lis[0]
        (x, end) = lis[-1]
        task_list.append((start,end))

    return task_list

def insta_reader(task_list, fname, log_file):
    #read insta file as task_list bounded
    #retrieve the coordinates in every line
    #return a list of coordinates
    start_pos, end_pos = task_list
    fhandle = open(fname)
    fhandle.seek(start_pos, 1)
    insta_coor = []
    while fhandle.tell() != end_pos:
        pattern = ",\n\r"
        raw_data = fhandle.readline().rstrip(pattern)
        if raw_data:
            try:
                insta = json.loads(raw_data)
                if 'coordinates' in insta['doc'] and \
                    'coordinates'in insta['doc']['coordinates']:
                    coordinate = insta['doc']['coordinates']['coordinates']
                    (long,lat) = (coordinate[1], coordinate[0])
                    insta_coor.append((long,lat))
            except ValueError:
                #print raw_data
                s = "Invalid Json Line on " + str(fhandle.tell()) + '\n'
                f = open(log_file, 'a')
                f.write(s)
                f.flush()
                f.close()
                print s
                #print comm.rank
        else:
            break
    fhandle.close()
    return insta_coor


if __name__ == '__main__':

    startTime = time.time()
    
    #fname = 'mediumInstagram.json'
    #fname = 'tinyInstagram.json'
    fname = 'bigInstagram.json'
    log_file = 'result.txt'

    comm = MPI.COMM_WORLD
    grid = G.load_grid(log_file)

    print "Starting...", comm.size
    
    if comm.rank == 0:
        loghdl = open(log_file,'a')
        loghdl.write("**********" + str(startTime) + "**********\n")
        loghdl.flush()
        loghdl.close()
        #file_read_time = time.time()
        insta_list = generate_fpoint(fname, comm.size)
        result = []
        #print "through time", time.time() - file_read_time
    else:
        insta_list = []
    
    my_grid = grid
    my_insta_list = comm.scatter(insta_list, root = 0)
    
    #assign coordinates to individual grid
    my_ranked_grid = G.count_insta(insta_reader(my_insta_list, fname, log_file), my_grid)
    #result: a list of grid dict with insta count
    result = comm.gather(my_ranked_grid, root = 0)
    
    
    if comm.rank == 0:
        final_result = {}
        grid_list = grid.keys()
        for g in grid_list:
            final_result[g] = 0
        for sub_grid in result:
            for key in sub_grid.keys():
                final_result[key] += sub_grid[key]
        
        loghdl = open(log_file, 'a')

        G.rank(final_result, loghdl)
        G.rank(G.sumup_dict(final_result, target='r'), fhandle = loghdl, target = 'r')
        G.rank(G.sumup_dict(final_result, target='c'), fhandle = loghdl, target = 'c')

        time_used = "---Total Time---\n" + str(time.time() - startTime)
        print time_used
        loghdl.write(time_used + "\n\n\n")
        loghdl.flush()
        loghdl.close()

    #comm.Reduce(grid, my_grid, MPI.SUM, root=0)

