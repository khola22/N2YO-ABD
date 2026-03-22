# Imports
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def ensure_column(session, keyspace, table, column_name, column_type):
    row = session.execute(
        """
        SELECT column_name
        FROM system_schema.columns
        WHERE keyspace_name = %s AND table_name = %s AND column_name = %s
        """,
        (keyspace, table, column_name),
    ).one()

    if row is None:
        session.execute(
            f"ALTER TABLE {keyspace}.{table} ADD {column_name} {column_type}"
        )

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

    # TABLE 1 — Positions brutes (Speed + Batch Layer)
    session.execute("""
        CREATE TABLE IF NOT EXISTS satellite.positions (
            satid int,
            timestamp int,
            satname text,
            datetime timestamp,
            satlatitude double,
            satlongitude double,
            sataltitude double,
            eclipsed boolean,
            speed_km_s   double,  
            PRIMARY KEY (satid, timestamp)
        )
    """)

    # Make the schema resilient for databases created from older definitions.
    ensure_column(session, "satellite", "positions", "timestamp", "int")
    ensure_column(session, "satellite", "positions", "datetime", "timestamp")
    ensure_column(session, "satellite", "positions", "satname", "text")
    ensure_column(session, "satellite", "positions", "satlatitude", "double")
    ensure_column(session, "satellite", "positions", "satlongitude", "double")
    ensure_column(session, "satellite", "positions", "sataltitude", "double")
    ensure_column(session, "satellite", "positions", "eclipsed", "boolean")
    ensure_column(session, "satellite", "positions", "speed_km_s", "double")
    
    # TABLE 2 — Statiqtiques réalisées quotidiennement (Alimentées par le traitement Batch)
    # On stocke les vues Batch
    session.execute("""
        CREATE TABLE IF NOT EXISTS satellite.daily_stats (
            satid int,
            satname text,
            day date,
            avg_speed double,
            max_altitude double,
            min_altitude double,
            total_records bigint, 
            PRIMARY KEY (satid, day)
        )
    """) 
    
    print("Keyspace and Table created successfully!")
    cluster.shutdown()

if __name__ == "__main__":
    setup()