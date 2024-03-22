import boto3
import subprocess
import time

from cassandra.cluster import Cluster, ConsistencyLevel
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from cassandra_sigv4.auth import SigV4AuthProvider
from uuid import uuid4
from cassandra.query import SimpleStatement

connectsource = {
    "ip": "194.233.68.255",
    "port": 19042,
}
clustersrc = Cluster(
    [connectsource["ip"]], port=connectsource["port"], auth_provider={}
)
sessionsrc = clustersrc.connect()

ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations("sf-class2-root.crt")
ssl_context.verify_mode = CERT_REQUIRED

# use this if you want to use Boto to set the session parameters.
region = "ap-northeast-1"
boto_session = boto3.Session(region_name=region)
auth_provider = SigV4AuthProvider(boto_session)
clusterdest = Cluster(
    ["cassandra.{}.amazonaws.com".format(region)],
    ssl_context=ssl_context,
    auth_provider=auth_provider,
    port=9142,
)
sessiondest = clusterdest.connect()


def contains_only_newlines(s):
    stripped_string = s.strip()
    return stripped_string == ""


try:
    system_schemas = {}
    rows = sessionsrc.execute("SELECT keyspace_name FROM system_schema.keyspaces;")

    # Print the list of keyspaces
    for row in rows:
        if not row.keyspace_name.startswith("system"):  # exclude system keyspaces
            command = f'cqlsh {connectsource["ip"]} {connectsource["port"]} -e "DESCRIBE KEYSPACE {row.keyspace_name}"'
            process = subprocess.run(
                command, shell=True, stdout=subprocess.PIPE, text=True
            )
            output = process.stdout
            print(f"Creating {row.keyspace_name} with all its tables and properties")
            keyspace_exists = False
            querysplit = output.split(";")

            for s in querysplit:
                if not contains_only_newlines(s):
                    sessiondest.execute(s)  # keyspace must come at very first
                    while not keyspace_exists:
                        try:
                            sessiondest.set_keyspace(row.keyspace_name)
                            keyspace_exists = True
                        except Exception as e:
                            print("Keyspace not yet available, waiting...")
                            time.sleep(5)  # Adjust the sleep time as necessary

except Exception as e:
    print(e)
finally:
    sessionsrc.shutdown()
    clustersrc.shutdown()
    sessiondest.shutdown()
    clusterdest.shutdown()
