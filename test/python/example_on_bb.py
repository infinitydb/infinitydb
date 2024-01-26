
'''
This is a simple example of using the InfinityDB REST API from Python.
MIT License https://opensource.org/licenses/MIT
'''

from infinitydb.access import *

# Optional: prevent the warning about the certificate in case
# there is not a verifiable TLS certificate on the server.
# This is not for production use, but for testing.
# The data is still encrypted, but the server is not verified.
# Performace is better with verification, because
# keep-alive works better so use it if possible.
verify = True
def disable_verification():
    import requests
    from urllib3.exceptions import InsecureRequestWarning
    from urllib3 import disable_warnings
    # Disable only the InsecureRequestWarning
    disable_warnings(InsecureRequestWarning)
    global verify
    verify = False

# Our alternate port is 37411 but it is sometimes blocked by firewalls, so we default to 443

case = 1
if case == 1: 
    # Use the demo server
    infinitydb_url = 'https://infinitydb.com/infinitydb/data'
elif case == 2:
    # Use the local server
    infinitydb_url = 'https://localhost/infinitydb/data'
    disable_verification()
elif case == 3:
    # Use a temporary AWS server
    infinitydb_url = 'https://ec2-3-137-151-100.us-east-2.compute.amazonaws.com/infinitydb/data'
    disable_verification()
        
    
""" Database names (URI components) """

# Databases have one slash separating two names each like
# [A-Za-z][A-Za-z0-9._-]*
# An infinitydb server by default has these available for the testUser
database_uri = 'demo/writeable' 
#database_uri = 'demo/readonly'

""" The User Name and Password """

# A public guest user for browsing and experimentation, in the
# 'guest' role. By default, the testUser user has no password, so
# the admin user may set one.
user = 'testUser'
password = 'db'

""" The connection to the database """

infdb = InfinityDBAccessor(infinitydb_url, db=database_uri, user=user, password=password, verify=verify)

infdb.head() # check that the connection is working
    
# better than json.dumps because it handles tuples and others
def pprint(o, indent=0):
    def to_str(o):
        if isinstance(o, tuple):
            t = (to_str(i) for i in o)
            return '(' + ', '.join(t) + ')'
        if isinstance(o, str):
            return '"' + o + '"'
        else:
            return str(o)
    if isinstance(o, dict):
        print('{')
        for k, v in o.items():
            print(' ' * (indent + 4) + to_str(k) + ' : ', end='')
            pprint(v, indent + 4)
        print(' ' * indent + '}')
    elif isinstance(o, list):
        print('[')
        for i in o:
            print(' ' * (indent + 4), end='')
            pprint(to_str(i), indent + 4)
        print(' ' * indent + ']')
    else:
        print(to_str(o))
                    
""" Get JSON given a prefix Item from the REST connection """
def get_documentation():
    # This shows direct access to the db, but we prefer query-based access below 
    # To see the documentation graphically in the demo/readonly database, go to:
    # https://infinitydb.com/infinitydb/data/demo/readonly/Documentation?action=edit
    # or without the action=edit to see it in JSON form.
    # Here we read that JSON into content, with success being a boolean.
    # The success only indicates that some data was read, not that there was an error.
    # Real errors raise InfinityDBError
    # The JSON is represented by nested dicts and lists.
    success, content, content_type = infdb.get_json(
        [EntityClass('Documentation'),'Backend Web Access', Attribute('description'),
            Index(1), EntityClass('Browser')])
    return success, content, content_type

# Launch a query on the server. There is no request header or response header
# This just copies and restructures a small amount of data on the db 
def copy_aircraft():
    success, content, response_content_type = infdb.execute_query(
        ['examples','AircraftCopy'])

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
    
def get_fish_farm_profit():
    success, content, response_content_type = infdb.execute_query(
        ['examples','fish farm profit'])
    return success, content, response_content_type

try:
    print('get documentation')
    success, content, type = get_documentation()
    if success:
        pprint(content)
    
    print('copy aircraft')
    copy_aircraft()
    
    print('summarize samples')
    success, content, type = get_summarize_samples()
    if success:
        pprint(compact_tree_tips(unflatten_from_tuples(content)))
        
    success, content, type = get_image('pic0')
    if success:
        print('retrieved image size=', len(content))

    # This one has no data in the v 6.0.34 demo database
    print('get people country')
    success, content, type = get_people_country()
    if success:
        # this is a dict with tuple keys and None values like
        # {PersonState: {('ca', 'joe'): None, ('ca', 'sally'): None}} 
        pprint(content)
        # this is a dict of state keys, containing a dict with person keys like
        # {PersonState: {'ca': {'joe': None, 'sally': None}}}
        pprint(unflatten_from_tuples(content))
    
    print('get fish farm profit')
    success, content, type = get_fish_farm_profit()
    if success:
        print('fish farm profit with tuple keys')
        pprint(content)
        print('fish farm profit with nested dicts')
        pprint(unflatten_from_tuples(content))
        print('fish farm profit with nested dicts compacted')
        pprint(compact_tree_tips(unflatten_from_tuples(content)))

except InfinityDBError as e:
    print('Could not access infdb ', e)
except Exception as e:
    print('Could not access infdb ', e)
    
