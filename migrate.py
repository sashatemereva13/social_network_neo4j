import sqlite3
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# SQLite connection
sqlite_conn = sqlite3.connect("social_network.db")
sqlite_cursor = sqlite_conn.cursor()

# Neo4j connection
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
)


# migrate users
def migrate_users():
    print("we are migrating the users now")
    rows = sqlite_cursor.execute("SELECT id, username, name FROM users").fetchall()

    with driver.session() as session:
        for row in rows:
            session.run(
                """
                        CREATE (u:User {old_id: $id, username: $username, name: $name})
                        """,
                id=row[0],
                username=row[1],
                name=row[2],
            )


#  migrate posts
def migrate_posts():
    print("now we are migtating the posts")
    rows = sqlite_cursor.execute("SELECT id, user_id, content FROM posts").fetchall()

    with driver.session() as session:
        for row in rows:
            session.run(
                """
                        MATCH (u:User {old_id: $user_id})
                        CREATE (p:Posts {old_id: $id, content: $content, timestamp: datetime()})
                        CREATE (u)-[:POSTED]->(p)
                        """,
                id=row[0],
                user_id=row[1],
                content=row[2],
            )


#  migrate follows
def migrate_follows():
    print("and now we are finally migrating the follows")
    rows = sqlite_cursor.execute(
        "SELECT follower_id, followee_id FROM followers"
    ).fetchall()

    with driver.session() as session:
        for row in rows:
            session.run(
                """
                MATCH (a:User {old_id: $follower_id}),
                       b:User {old_id: $followee_id}
                MERGE (a)-[:FOLLOWS]->(b)
                        """,
                follower_id=row[0],
                followee_id=row[1],
            )


#  M is for migrate
if __name__ == "__main__":
    migrate_users()
    migrate_posts()
    migrate_follows()
    driver.close()
    sqlite_conn.close()

    print("and we are done migrating :)")
