import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DBNAME"),  # Replace with your database name
            user=os.getenv("POSTGRES_USERNAME"),  # Replace with your username
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def run_explain_query(conn, query):
    try:
        cursor = conn.cursor()
        # Execute an EXPLAIN (FORMAT JSON) query
        cursor.execute(f"EXPLAIN (BUFFERS ON, ANALYZE ON, COSTS ON, VERBOSE ON, SUMMARY ON, TIMING ON, FORMAT JSON) {query}")
        # Fetch the result
        result = cursor.fetchone()
        # Convert the result to JSON
        explain_json = json.loads(json.dumps(result[0]))
        return explain_json

    except Exception as e:
        print(f"Error executing the query: {e}")
        return None
    finally:
        cursor.close()

def print_plan(plan, level=0):
    indent = "    " * level
    node_type = plan.get("Node Type", "Unknown")
    relation = plan.get("Relation Name", "N/A")
    alias = plan.get("Alias", "")
    relation_info = f" on {relation}" if relation != "N/A" else ""
    alias_info = f" (alias: {alias})" if alias else ""
    print(f"{indent}- {node_type}{relation_info}{alias_info}")
    if "Plans" in plan:
        for subplan in plan["Plans"]:
            print_plan(subplan, level + 1)

def main():
    conn = connect_to_db()
    if conn:
        # Sample query from TPC-H (modify as needed)
        # sample_query = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) FROM lineitem GROUP BY l_returnflag, l_linestatus"
        sample_query = """SELECT
    o_orderkey,
    o_orderdate,
    o_totalprice,
    c_name AS customer_name
FROM
    orders o
JOIN
    customer c ON o.o_custkey = c.c_custkey
WHERE
    o.o_orderstatus = 'O'
ORDER BY
    o.o_totalprice DESC
LIMIT 10;"""

        explain_json = run_explain_query(conn, sample_query)
        if explain_json:
            print("EXPLAIN Plan Tree:")
            for plan in explain_json:
                print_plan(plan["Plan"])
        # Close the connection
        conn.close()

if __name__ == "__main__":
    main()
