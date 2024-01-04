
from infinitydb.access import *

# port 37411 is sometimes blocked by firewalls, so we use 443
infinitydb_url = 'https://infinitydb.com/infinitydb/data'
# True or None for a server with verifiable TLS certificate, 
# which means it has a certificate from a trusted certificate authority
# with a matching domain name.
verify = True
#infinitydb_url = 'https://localhost:37411/infinitydb/data'
#verify = False 

""" Database names (URI components) """

# Databases have one slash separating two names each like
# [A-Za-z][A-Za-z0-9._-]*
# An infinitydb server by default has these available for the testUser
database_uri = 'demo/writeable' 
#database_uri = 'demo/readonly'

""" The User Name and Password """

# A public guest user for browsing and experimentation, in the
# 'guest' role. Contact us for your own experimentation login and db.
user = 'testUser'
password = 'db'

""" The connection to the database """

infdb = InfinityDBAccessor(infinitydb_url, db=database_uri, user=user, password=password, verify=verify)

infdb.head() # check that the connection is working
    
""" Get JSON given a prefix Item from the REST connection """

# This shows direct access to the db, but we prefer query-based access below 
# To see the documentation graphically in the demo/readonly database, go to:
# https://infinitydb.com:37411/infinitydb/data/demo/readonly/Documentation?action=edit
# or without the action=edit to see it in JSON form.
# Here we read that JSON into content, with success being a boolean.
# The success only indicates that some data was read, not that there was an error.
# Real errors raise InfinityDBError
# The JSON is represented by nested dicts and lists.
# We use a path prefix of EntityClass('Documentation') which is an 
# Item with a single initial class component:
success, content, content_type = infdb.get_json([EntityClass('Documentation')])
print(content)

# Launch a query on the server. There is no request header or response header
# This just copies and restructures a small amount of data on the db 
def copy_aircraft():
    success, content, response_content_type = infdb.execute_query(
        ['examples','Aircraft to AircraftModel'])

# Some statistics calculated over the samples table
def get_summarize_samples():
    success, content, response_content_type = infdb.execute_query(
        ['examples','summarize samples2'])
    return success, content, response_content_type

# for blobs, we do a direct 'get blob' type of query. Very fast        
def get_image(pic):
    data = { Attribute('name'): pic }
    success, content, response_content_type = infdb.execute_get_blob_query(
        ['examples','Display Image'], data=data)
    return success, content, response_content_type

def get_people_country():
    success, content, response_content_type = infdb.execute_query(
        ['examples.person','get residence state'])
    return success, content, response_content_type
    

try:
    print('copy aircraft')
    copy_aircraft()
    print('summarize samples')
    success, content, type = get_summarize_samples()
    if success:
        print(' ',content)
    success, content, type = get_image('pic0')
    if success:
        print('retrieved image size=', len(content))
    success, content, type = get_people_country()
    print(content)
    
except InfinityDBError as e:
    print('Could not access infdb ', e)
except Exception as e:
    print('Could not access infdb ', e)
    
