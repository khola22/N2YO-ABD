import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def setup():
    # Connect using the service name from docker-compose
    auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
    
    while True:
        try:
            print("Checking Cassandra connection...")
            cluster = Cluster(['cassandra'], auth_provider=auth)
            session = cluster.connect()
            break 
        except Exception as e:
            print(f"Cassandra not ready yet... waiting 5s. ({e})")
            time.sleep(5)

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS satellite
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS satellite.positions (
            satid int,
            satname text,
            satlatitude double,
            satlongitude double,
            sataltitude double,
            timestamp int,
            eclipsed boolean,
            PRIMARY KEY (satid, timestamp)
        )
    """)
    print("Keyspace and Table created successfully!")
    cluster.shutdown()

if __name__ == "__main__":
    setup()