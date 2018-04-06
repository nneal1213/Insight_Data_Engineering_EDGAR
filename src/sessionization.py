import datetime
import time
import sys
from collections import OrderedDict


def open_files(input_filename, session_filename, inactivity_file):

    """
    Initializing file pointers for input file, output file, and inactivity file
    """
    input_f = open(input_filename, encoding='utf-8')
    # getting rid of header
    input_f.readline()
    # opening output file
    output_f = open(session_filename, 'a+')
    # reading from inactivity file
    with open(inactivity_file) as f3:
        inactivity_period = int(f3.read().strip())
    return input_f, output_f, inactivity_period


def feed_data(line):
    """
    Parses out the ip, date, time, cik, accession, extention from each line
    Converts the timestamp into unix time.
    """
    line = line.split(',')
    ip, date, time, cik, accession, extention = line[:3]+line[4:7]
    timestamp = datetime.datetime.strptime(date + ' ' + time, '%Y-%m-%d %X').timestamp()
    return ip, timestamp, (cik, accession, extention)


def write_to_file(output_f, key, value):
    """
    Writes user, first timestamp, last timestamp, duration, and counts to file
    """   
    user = key
    first_timestamp, last_timestamp, counts = value
    duration = str(int(last_timestamp - first_timestamp)+1)
    counts = str(int(counts))
    first_timestamp = datetime.datetime.fromtimestamp(first_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    last_timestamp = datetime.datetime.fromtimestamp(last_timestamp).strftime('%Y-%m-%d %H:%M:%S')    
    output_f.write(','.join([user, first_timestamp, last_timestamp, duration, counts]))
    output_f.write('\n')

    
def remove_inactive_sessions(active_session_dict, current_time):
    """
    Cleaning up inactive sessions
    """
    if active_session_dict:      
        keys_list = list(active_session_dict.keys())        
        for key in keys_list:
            if abs(current_time - active_session_dict[key][1]) >= inactivity_period:
                try:
                    write_to_file(output_f, key, active_session_dict[key])
                except IOError:
                    print('Unable to write line to file')
                    raise
                active_session_dict.pop(key)
    return active_session_dict


def saving_all_active_sessions(active_session_dict):
    """
    Saving all to file
    """
    for key in active_session_dict.keys():
                try:
                    write_to_file(output_f, key, active_session_dict[key])
                except IOError:
                    print('Unable to write line to file')
                    raise


if __name__ == '__main__':
    
    try:
        input_filename, inactivity_file, session_filename = sys.argv[1:]  
    except:
        print('file input must have (input file), (inactivity file), (output file)')
    
    begin = time.time()
    row_counter = 0

    try:
        input_f, output_f, inactivity_period = open_files(
            input_filename, session_filename, inactivity_file
        )
        line = input_f.readline()

    except IOError:
        print('One or more files not present')
        raise

    active_session_dict = OrderedDict()

    try:
        ip, timestamp, doc = feed_data(line)
        current_time = timestamp
    except:
        print('Data in incompatible format.  Continuing with next row')

    while line != '':

        row_counter += 1
        # First session
        if ip not in active_session_dict.keys():
            active_session_dict[ip] = [timestamp, timestamp, 1]
        # Already existing session
        else:
            active_session_dict[ip][1] = timestamp
            active_session_dict[ip][2] += 1    

        try:
            line = input_f.readline()
            ip, timestamp, doc = feed_data(line)

        except:
            print('End of file or incompatible format')
            break
        # changing over to current time
        if timestamp > current_time:
            active_session_dict = remove_inactive_sessions(active_session_dict, current_time)
            current_time = timestamp

    # Saving all to output file and closing file pointers
    saving_all_active_sessions(active_session_dict)
    input_f.close()
    output_f.close()

    print('Time Spent (s):{}'.format(time.time()-begin))
    print('Data points processed:{}'.format(row_counter))


