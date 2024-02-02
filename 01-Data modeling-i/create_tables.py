import logging
from typing import NewType

import psycopg2
import traceback

PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

# Queries to drop tables if they exist
table_drop_events = "DROP TABLE IF EXISTS events CASCADE"
table_drop_actors = "DROP TABLE IF EXISTS actors CASCADE"
table_drop_reactions = "DROP TABLE IF EXISTS reactions CASCADE"
table_drop_state = "DROP TABLE IF EXISTS state CASCADE"
table_drop_comments = "DROP TABLE IF EXISTS comments CASCADE"  # เพิ่มตาราง comments ในการลบ

# Queries to create tables
table_create_actors = """
    CREATE TABLE IF NOT EXISTS actors (
        id SERIAL PRIMARY KEY,
        login text
    )
"""

table_create_events = """
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        type text,
        actor_id bigint,  -- เปลี่ยนชนิดข้อมูลเป็น bigint
        FOREIGN KEY (actor_id) REFERENCES actors(id)
    )
"""

table_create_reactions = """
    CREATE TABLE IF NOT EXISTS reactions (
        id SERIAL PRIMARY KEY,
        event_id bigint, 
        reaction_type text,
        user_id int,
        created_at timestamp,
        FOREIGN KEY (event_id) REFERENCES events(id),
        FOREIGN KEY (user_id) REFERENCES actors(id)
    )
"""

table_create_state = """
    CREATE TABLE IF NOT EXISTS state (
        id SERIAL PRIMARY KEY,
        event_id bigint,  -- เปลี่ยนชนิดข้อมูลเป็น bigint
        state_type text,
        user_id int,
        created_at timestamp,
        FOREIGN KEY (event_id) REFERENCES events(id),
        FOREIGN KEY (user_id) REFERENCES actors(id)
    )
"""

# New table for comments
table_create_comments = """
    CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        event_id bigint,  -- เปลี่ยนชนิดข้อมูลเป็น bigint
        user_id int,
        comment_text text,
        created_at timestamp,
        FOREIGN KEY (event_id) REFERENCES events(id),
        FOREIGN KEY (user_id) REFERENCES actors(id)
    )
"""

# Lists of queries for dropping and creating tables
create_table_queries = [
    table_create_actors,
    table_create_events,
    table_create_reactions,
    table_create_state,
    table_create_comments,
]

drop_table_queries = [
    table_drop_reactions,
    table_drop_state,
    table_drop_comments,  # เพิ่มตาราง comments ในการลบ
    table_drop_events,
    table_drop_actors,
]

def drop_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
      cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
        )
        with conn:
            cur = conn.cursor()
            drop_tables(cur, conn)
            create_tables(cur, conn)
    except psycopg2.Error as e:
        logging.error(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
