from typing import NewType
import psycopg2

PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

table_drop_events = "DROP TABLE IF EXISTS events"
table_drop_actors = "DROP TABLE IF EXISTS actors"

table_create_actors = """
    CREATE TABLE IF NOT EXISTS actors (
        id SERIAL PRIMARY KEY,
        login TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

table_create_events = """
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        type TEXT,
        actor_id INT,
        event_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        location TEXT,
        details TEXT,
        FOREIGN KEY(actor_id) REFERENCES actors(id)
    )
"""

create_table_queries = [
    table_create_actors,
    table_create_events,
]
drop_table_queries = [
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

    # Insert sample data for testing
    cur.execute("""
        INSERT INTO actors (login, first_name, last_name, email)
        VALUES ('john_doe', 'John', 'Doe', 'john.doe@example.com')
    """)
    cur.execute("""
        INSERT INTO events (type, actor_id, location, details)
        VALUES ('login', 1, 'Office', 'Successful login')
    """)
    conn.commit()

def main():
    """
    - Drops (if exists) and Creates the database.
    - Establishes connection with the database and gets cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
