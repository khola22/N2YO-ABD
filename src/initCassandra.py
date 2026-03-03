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

    # TABLE 1 — Positions brutes (Speed + Batch Layer)
    session.execute("""
        CREATE TABLE IF NOT EXISTS satellite.positions (
            satid int,
            satname text,
            satlatitude double,
            satlongitude double,
            sataltitude double,
            timestamp int,
            eclipsed boolean,
            speed_km_s   double,  
            PRIMARY KEY (satid, timestamp)
        )
    """)

    # # TABLE 2 — Statistiques d'éclipse (Speed Layer)
    # session.execute("""
    #     CREATE TABLE IF NOT EXISTS satellite.eclipse_stats (
    #         satid            int,
    #         satname          text,
    #         time_in_eclipse  bigint,
    #         time_in_sunlight bigint,
    #         total_positions  bigint,
    #         PRIMARY KEY (satid)
    #     )
    # """)

    # # TABLE 3 — Vitesse orbitale (Speed + Batch Layer)
    # session.execute("""
    #     CREATE TABLE IF NOT EXISTS satellite.orbital_speed (
    #         satid        int,
    #         timestamp    int,
    #         satname      text,
    #         speed_km_s   double,
    #         satlatitude  double,
    #         satlongitude double,
    #         sataltitude  double,
    #         PRIMARY KEY (satid, timestamp)
    #     )
    # """)
    
    print("Keyspace and Table created successfully!")
    cluster.shutdown()

if __name__ == "__main__":
    setup()