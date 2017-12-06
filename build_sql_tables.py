################################################
#
# Build SQL tables and read in websocket output
#
################################################

import json
import sqlite3

# Arrays to hold websocket output in correct json format
edits_array  = []
location_array = []

# counters
it0=0
it1=0
it2=0

# Loop over websocket output data
for line in open('wikipedia_edit_stream_data.txt','r'):        

    # Select only Wikipedia page edits (reject all entries for "warning", "create" etc.)
    if '"action": "edit"' in line:

        it0 += 1
        
        # Fill entries array with all non-"geo_ip":{} location information
        edits_array.append(json.loads(line))
        
        # Grab the `"date: "",` entry from the beginning of the line  
        start       = line.find('"date"')
        end         = line.find(', "action"')
        time_string = line[start:end]
        
        # Search for location information in the line
        if '"geo_ip": {"city":'in line:
            x     = '"geo_ip": {'
            start = line.find(x) + len(x)
            end   = line.find('},')
            
            # Fetch the location information entries
            location_string = line[start:end]

            # Combine with the `"date: "",` information
            db_entry        = ("{%s, %s}" % (time_string, location_string))
            it1 += 1

        else:
            # If no location information exists, just fetch the `"date: "",` information
            db_entry        = ("{%s}" % time_string)
            it2 += 1

        # write db_entry to the location_array_info array
        # "date" column added as a sanity check for when later merging with edits_array[] table
        location_array.append(json.loads(db_entry))

# Print the number of (non-)geo-tagged lines and the total as sanity check
print("geo-tagged entries = %i" % it1)
print("non-tagged entries = %i" % it2)
print("     total entries = %i" % it0)

# Open and connect to .sqlite output file
db   = sqlite3.connect('wiki_edits_full.sqlite')
curs = db.cursor()

# Create an SQL table for the edits_array[] information
curs.execute('''CREATE TABLE IF NOT EXISTS edits_info(\
entry_id INTEGER PRIMARY KEY AUTOINCREMENT,\
date TEXT,\
action TEXT,\
change_size INTEGER,\
flags TEXT,\
is_anon INTEGER,\
is_bot INTEGER,\
is_minor INTEGER,\
is_new INTEGER,\
is_unpatrolled INTEGER,\
page_title TEXT,\
parent_rev_id TEXT,\
rev_id TEXT,\
summary TEXT,\
user TEXT\
)''')

# Create an SQL table for the location_array[] information
curs.execute('''CREATE TABLE IF NOT EXISTS location_info(\
entry_id INTEGER PRIMARY KEY AUTOINCREMENT,\
date TEXT,\
city TEXT,\
country_name TEXT,\
latitude REAL,\
longitude REAL,\
region_name TEXT\
)''')

# Build SQL command for inserting edits_array information into table
SQL_EDITS = 'INSERT INTO edits_info \
(date, action, change_size, flags, \
is_anon, is_bot, is_minor, is_new, is_unpatrolled, \
page_title, parent_rev_id, rev_id, \
summary, user) \
VALUES (:date, :action, :change_size, :flags, \
:is_anon, :is_bot, :is_minor, :is_new, \
:is_unpatrolled, :page_title, \
:parent_rev_id, :rev_id, :summary, :user)'

# Default values in case any are missing in edits_array[]
defaults_edit = {
    "date": None, "action": None, "change_size": None, 
    "flags": None, "is_anon": None, "is_bot": None, 
    "is_minor": None, "is_new": None, "is_unpatrolled": None,
    "page_title": None, "parent_rev_id": None, "rev_id": None, 
    "summary": None, "user": None
    }

# Fill SQL table with edit information inserting default values for any missing entries
curs.executemany(SQL_EDITS, ({k: d.get(k, defaults_edit[k]) for k in defaults_edit} for d in edits_array))

#curs.executemany('INSERT INTO edits_info \
#(date, action, change_size, flags, \
#is_anon, is_bot, is_minor, is_new, is_unpatrolled, \
#page_title, parent_rev_id, rev_id, \
#summary, user) \
#VALUES (:date, :action, :change_size, :flags, \
#:is_anon, :is_bot, :is_minor, :is_new, \
#:is_unpatrolled, :page_title, \
#:parent_rev_id, :rev_id, :summary, :user)', edits_array)

# Build SQL command for location information
SQL_LOCATION = "INSERT INTO location_info (date, city, country_name, latitude, longitude, region_name)\
VALUES (:date, :city, :country_name, :latitude, :longitude, :region_name)"

# Default location info values
defaults_location = {
    "date": None, "city": None, "country_name": None, "latitude": -1e9, 
    "longitude": -1e9, "region_name": None
    }

# Fill location_array table
curs.executemany(SQL_LOCATION, ({k: d.get(k, defaults_location[k]) for k in defaults_location} for d in location_array))

# Commit changes to sqlite file and close
db.commit()
db.close()


