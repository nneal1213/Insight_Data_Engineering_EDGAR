# Description of the Challenge

As a data engineer, my task for this challenge is to create session logs from incoming data.  For practicality purposes, we have assumed an input file to simulate streamining data. Within the incoming data, ip address, time of the request, and document identifiers should be parsed to store the details of the request. 

Given a certain file containing the inactivity period determining the end of a session, it will be possible to determine the beginning and ending of individual sessions.


# Methodology

Upon opening the two input files, one designating the "streaming data" and the other defining the duration of inactivity to conclude a session, the input file was read line-by-line. The relevant portions were extracted, namely the ip, date, time, cik, accession, and extention. An Unix timestamp was calculated from the date and time. 

The ip, timestamp, and counts defining active sessions are stored within an OrderedDict data structure from the collections module within Python. The data structure offers a fast lookup and satisfies the criteria that sessions concluding at the same time would have to be logged in the chronological order of their begin times. 

After the conclusion of each second, an examination was performed upon the active session log to write inactivity sessions to the output file and to purge them from the data structure afterwards. 

When the input file reaches end-of-file (although this would not occur in the case of streaming data), all inactive sessions are written to the output file and the file pointers closed out. 


 
