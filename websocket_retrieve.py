###################################################
#
# Open websocket and pipe output to txt file
#
###################################################

import websocket
import time
from websocket import create_connection
from time import gmtime

f = open('wikipedia_edit_stream_data.txt','w')

ws = create_connection("ws://wikimon.hatnote.com:9000")

# Connect to websocket for 24 hours and write output to file f
tout = time.time() + 60*60*24
while time.time() < tout:
    
    result =  ws.recv()
    
    # Get current time 
    t = time.strftime("%Y-%m-%d %H:%M:%S", gmtime())

    # Add timestamp to the websocket output, retaining format
    f.write('{"date": "%s", %s\n' % (t, result[1:]))    

ws.close()
