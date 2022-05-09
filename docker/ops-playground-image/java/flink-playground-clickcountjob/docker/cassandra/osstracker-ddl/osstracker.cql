CREATE KEYSPACE IF NOT EXISTS missing
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS missing.delivery (
                                                ride_id bigint,
                                                is_start boolean,
                                                PRIMARY KEY(ride_id));