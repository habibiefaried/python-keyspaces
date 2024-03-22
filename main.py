from cassandra.cluster import Cluster, ConsistencyLevel
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider
from uuid import uuid4
from cassandra.query import SimpleStatement

ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations('sf-class2-root.crt')
ssl_context.verify_mode = CERT_REQUIRED

# use this if you want to use Boto to set the session parameters.
region = "ap-northeast-1"
boto_session = boto3.Session(region_name=region)
auth_provider = SigV4AuthProvider(boto_session)

clustersrc = Cluster(['194.233.68.255'], port=19042, auth_provider={})
clusterdest = Cluster(['cassandra.{}.amazonaws.com'.format(region)], 
                  ssl_context=ssl_context,
                  auth_provider=auth_provider,
                  port=9142)
sessionsrc = clustersrc.connect()
sessiondest = clusterdest.connect()

def list_all_tables(sess):
    system_schemas = {}
    rows = sess.execute("SELECT keyspace_name FROM system_schema.keyspaces;")

    # Print the list of keyspaces
    for row in rows:
        if not row.keyspace_name.startswith('system'): # exclude system keyspaces
            # Get tables in the keyspace
            system_schemas[row.keyspace_name] = {}
            tables = sess.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{row.keyspace_name}'")
            for table in tables:
                print(f"{row.keyspace_name}.{table.table_name}")
                system_schemas[row.keyspace_name][table.table_name] = []
                columns = sess.execute(f"""
                    SELECT column_name, type
                    FROM system_schema.columns
                    WHERE keyspace_name = '{row.keyspace_name}' AND table_name = '{table.table_name}'
                """)

                for column in columns:
                    system_schemas[row.keyspace_name][table.table_name].append({
                        column.column_name : column.type
                    })
                    
    return system_schemas

print(list_all_tables(sessionsrc))

# try:
#     # Create a new keyspace
#     sessiondest.execute(f"""
#     CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
#     WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '3' }}
#     """)
#     print(f"Keyspace {keyspace_name} created")

#     # Create a new keyspace
#     sessiondest.execute(f"""
#     CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
#         id uuid PRIMARY KEY,
#         name text,
#         age int
#     )
#     """)
#     print("Table created successfully.")

#     data_to_insert = [
#         (uuid4(), 'Alice', 30),
#         (uuid4(), 'Bob', 25),
#         (uuid4(), 'Charlie', 35)
#     ]

#     insert_query = SimpleStatement(f'''
#         INSERT INTO {keyspace_name}.{table_name} (id, name, age)
#         VALUES (%s, %s, %s)
#         ''',
#         consistency_level=ConsistencyLevel.LOCAL_QUORUM)

#     for data in data_to_insert:        
#         sessiondest.execute(insert_query, data)

#     print("Data inserted successfully.")
# except Exception as e:
#     print(e)
# finally:
#     sessiondest.shutdown()
#     clusterdest.shutdown()