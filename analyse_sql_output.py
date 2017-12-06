import numpy as np
import pandas as pd
import pandas.io.sql as pdsql
import matplotlib.pyplot as plt
import sqlite3

# Open sql database 
db = sqlite3.connect('wiki_edits_full.sqlite')
c = db.cursor()

##############################################################
#
# Make a plot of the cumulative total edits over a ~13 hour 
# period of the 10 most-edited Wikipedia pages
#
##############################################################

# SQL query selecting the 10 most often edited pages 
# not including User pages and internal Wikipedia organisation pages
sql3 = ("SELECT date, page_title, user, is_bot FROM edits_info WHERE page_title IN (SELECT page_title FROM edits_info WHERE page_title NOT LIKE 'User%' AND page_title NOT LIKE 'Wikipedia%' GROUP BY page_title ORDER BY COUNT(*) DESC LIMIT 10) ORDER BY page_title")

# Import SQL output as pandas dataframe
df_page = pdsql.read_sql(sql3, db)

# Set 'date' column as pandas datetime format and as dataframe index
df_page['date'] = df_page['date'].astype('datetime64[ns]') 
df_page.set_index('date', drop=False, inplace=True)

# Save a copy of the dataframe
df_page.to_csv('cumulative_study.csv',encoding='utf-8')

# Create a "blank" dataframe with 1 row for each desired time interval
d_range = pd.date_range(start='2017-10-31 16:00:00', end='2017-11-01 05:15:00', freq='5Min')
df_all_p = pd.DataFrame({'date': d_range})
df_all_p = df_all_p.set_index('date')

# Array for storing a dataframe for each page_title
p_list = []

# List of page titles ordered by number of edits
counts = df_page.page_title.value_counts().index.tolist()

# Loop over page titles and create new dataframe for each listing edits per 5 minute window  
for iter in range(0, len(counts)):
    
    p_list.append(df_page[(df_page.page_title == counts[iter])].groupby(pd.TimeGrouper(freq='5Min'))[['page_title']].count())

    # Rename dataframe column after page_title
    p_list[iter].columns = [counts[iter]]
    
    # Add each dataframe into the blank one
    df_all_p = pd.merge(df_all_p, p_list[iter], left_index=True, right_index=True, how='outer')

# Fill all missing values as 0
df_all_p = df_all_p.fillna(0)


# Plot the cumulative total of edits for each page title on one canvas
t_range=pd.date_range(start='2017-10-31 16:00:00', end='2017-11-01 05:15:00', freq='H')
df_all_p.cumsum().plot(drawstyle='steps', linewidth=3, figsize=(14,10), xticks=t_range, 
                       fontsize=15, grid=True)

# Plot formatting
plt.title('Cumulative Totals of Wikipedia Page Edits', fontsize=15)
plt.ylabel('Counts', fontsize=15)
plt.xlabel('Time and Date', fontsize=15)
plt.legend(loc='best', fontsize=15)

# Save plot to file
plt.savefig('Cumulative_totals_10_pages.pdf', bbox_inches='tight')




##############################################################
#
# Make a plot of total wikipedia edits per 15 minute time 
# window for 4 separate countries (USA, UK, India, Australia)
#
##############################################################

# SQL query 
sql_loc = ("SELECT e.date, e.page_title, e.user, \
l.city, l.country_name, l.latitude, l.longitude \
FROM edits_info e \
LEFT JOIN location_info l ON l.entry_id = e.entry_id \
WHERE e.is_bot = 0 AND l.latitude > -1e6 \
ORDER BY e.date")

# Read in sql query output as a pandas dataframe
df_loc = pdsql.read_sql(sql_loc, db)

# Set 'date' column as pandas datetime format
df_loc['date'] = df_loc['date'].astype('datetime64[ns]') 

# Set date column as dataframe index
df_loc.set_index('date', drop=False, inplace=True)

# Save a copy of the dataframe to file
df_loc.to_csv('location_study.csv',encoding='utf-8')

# Dict for storing a dataframe for each country
c_list = {} 

# Loop over country names and create new dataframe for each listing edits per 15 minute window
for country in ['United States', 'United Kingdom', 'India', 'Australia']:
    c_list[country] = df_loc[(df_loc.country_name == country)].groupby(pd.TimeGrouper(freq='15Min'))[['country_name']].count()

    # Rename dataframe column after each country
    c_list[country].columns = [country]

# Merge the separate dataframes
df_all = c_list["United States"].join(c_list["United Kingdom"]).join(c_list["India"]).join(c_list["Australia"])



# Remove first and last rows from the dataframe as they cover a time period < 15 minutes
df_all.drop(df_all.head(1).index, inplace=True)
df_all.drop(df_all.tail(1).index, inplace=True)

t_range=pd.date_range(start='2017-10-31 16:00:00', end='2017-11-01 05:15:00', freq='H')

# Make a line-graph plot of the dataframe with a separate subplot for each column/country
axes = df_all.plot(kind='line', subplots=True, figsize=(8, 9), xticks=t_range, grid=True,
            title="Wikipedia Page Edits per 15 Minutes"); 

# Additional plot formatting
fig=axes[0].figure
fig.text(0.05,0.5, "Counts", ha="center", va="center", rotation=90)

plt.subplots_adjust(hspace=.0) 
plt.legend(loc='best')
plt.xlabel('Time and Date')


# Save plot to file
plt.savefig('Wikipedia_edits_per_15_minutes_USA_UK_IND_AUS.pdf',bbox_inches='tight')


