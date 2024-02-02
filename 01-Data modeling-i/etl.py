import glob
import json
import os
from typing import List

import psycopg2

def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files

def insert_actor(cur, each):
    actor_insert_statement = """
        INSERT INTO actors (id, login)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING
    """
    cur.execute(actor_insert_statement, (each["actor"]["id"], each["actor"]["login"]))

def insert_event(cur, each):
    event_insert_statement = """
        INSERT INTO events (id, type, actor_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """
    cur.execute(event_insert_statement, (str(each["id"]), each["type"], each["actor"]["id"]))

def insert_comment(cur, each):
    if each["type"] == "IssueCommentEvent":
        comment_insert_statement = """
            INSERT INTO comments (id, event_id, user_id, comment_text, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """
        cur.execute(comment_insert_statement, (
            each["id"],
            each["id"],
            each["actor"]["id"],
            each["payload"]["comment"]["body"],
            each["created_at"]
        ))

def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                if each["type"] == "IssueCommentEvent":
                    print(
                        each["id"],
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                        each["payload"]["issue"]["url"],
                    )
                else:
                    print(
                        each["id"],
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                    )

                # Insert data into tables
                insert_actor(cur, each)
                insert_event(cur, each)
                insert_comment(cur, each)

                conn.commit()

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    table_drop_comments = "DROP TABLE IF EXISTS comments CASCADE"
    table_drop_reactions = "DROP TABLE IF EXISTS reactions CASCADE"
    table_drop_state = "DROP TABLE IF EXISTS state CASCADE"
    table_drop_events = "DROP TABLE IF EXISTS events CASCADE"
    table_drop_actors = "DROP TABLE IF EXISTS actors CASCADE"

    drop_table_queries = [
        table_drop_comments,
        table_drop_reactions,
        table_drop_state,
        table_drop_events,
        table_drop_actors,
    ]

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    table_create_actors = """
        CREATE TABLE IF NOT EXISTS actors (
            id bigint PRIMARY KEY,
            login text
        )
    """

    table_create_events = """
        CREATE TABLE IF NOT EXISTS events (
            id bigint PRIMARY KEY,
            type text,
            actor_id bigint,
            FOREIGN KEY (actor_id) REFERENCES actors(id)
        )
    """

    table_create_comments = """
        CREATE TABLE IF NOT EXISTS comments (
            id bigint PRIMARY KEY,
            event_id bigint,
            user_id bigint,
            comment_text text,
            created_at timestamp,
            FOREIGN KEY (event_id) REFERENCES events(id),
            FOREIGN KEY (user_id) REFERENCES actors(id)
        )
    """

    create_table_queries = [
        table_create_actors,
        table_create_events,
        table_create_comments,
    ]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
        )
        with conn:
            cur = conn.cursor()
            drop_tables(cur, conn)
            create_tables(cur, conn)
            process(cur, conn, filepath="../data")
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
