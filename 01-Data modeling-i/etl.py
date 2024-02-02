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


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                print(
                    each["id"],
                    each["type"],
                    each["actor"]["id"],
                    each["actor"]["login"],
                    each["repo"]["id"],
                    each["repo"]["name"],
                    each["created_at"],
                    each["payload"]["issue"]["url"] if each["type"] == "IssueCommentEvent" else None,
                )

                # Insert data into tables here
                actor_insert_statement = f"""
                    INSERT INTO actors (
                        id,
                        login
                    ) VALUES ({each["actor"]["id"]}, '{each["actor"]["login"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                cur.execute(actor_insert_statement)

                # Format labels and milestone as JSON strings
                labels_str = json.dumps(each.get("payload", {}).get("issue", {}).get("labels", []))
                milestone_str = json.dumps(each.get("payload", {}).get("issue", {}).get("milestone", ""))

                event_insert_statement = f"""
                    INSERT INTO events (
                        id,
                        type,
                        actor_id,
                        labels,
                        milestone
                    ) VALUES ('{each["id"]}', '{each["type"]}', {each["actor"]["id"]},
                               '{labels_str}', '{milestone_str}')
                    ON CONFLICT (id) DO NOTHING
                """
                cur.execute(event_insert_statement)

                conn.commit()


def etl_process(cur, conn, filepath):
    # Perform ETL for all files in the specified directory
    process(cur, conn, filepath)


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    etl_process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()
