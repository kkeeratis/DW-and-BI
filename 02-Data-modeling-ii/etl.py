import glob
import json
import os
from typing import List
from cassandra.cluster import Cluster

table_drop = "DROP TABLE IF EXISTS events"

table_create = """
    CREATE TABLE IF NOT EXISTS events
    (
        id text,
        type text,
        public boolean,
        PRIMARY KEY (
            id,
            type
        )
    )
"""

create_table_queries = [table_create,]
drop_table_queries = [table_drop,]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

def get_files(filepath: str) -> List[str]:
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files

def process(session, filepath):
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables here
                query = f"""
                INSERT INTO events (id, type, public) VALUES ('{each["id"]}', '{each["type"]}', true)
                """
                session.execute(query)

def insert_sample_data(session):
    query = f"""
    INSERT INTO events (id, type, public) VALUES ('23487929637', 'IssueCommentEvent', true)
    """
    session.execute(query)

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)

    process(session, filepath="../data")
    # insert_sample_data(session)

    print("\nData from Cassandra Table:")
    print("{:<15} {:<20} {:<10}".format("ID", "Type", "Public"))
    print("-" * 45)

    query_select_all = "SELECT * from events"
    try:
        rows = session.execute(query_select_all)
    except Exception as e:
        print(e)

    for row in rows:
        print("{:<15} {:<20} {:<10}".format(row.id, row.type, row.public))

    session.shutdown()

if __name__ == "__main__":
    main()
