from flask import Flask, request
import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta
import re
import pdb
import logging
import signal
import time
# Storing the DB Connections and credentials in memory
CONNECTIONS = {}

app = Flask(__name__)

SECRET_KEY = b'DBSP_PROJECT_2'

def remove_linebreaks_and_extra_spaces(query):
    '''
    Helper function used to remove all the linebreaks and whitespaces from the query
    '''
    # Remove line breaks
    query_without_linebreaks = query.replace('\n', ' ').replace('\r', '')
    # Replace multiple spaces with a single space
    query_without_extra_spaces = re.sub(r'\s+', ' ', query_without_linebreaks)
    # Strip leading and trailing spaces
    return query_without_extra_spaces.strip()

def execute_sql_query(conn, query):
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Execute the query required
        cur.execute(query)
        # Fetch the result
        results = cur.fetchall()
        # Convert the result to JSON Format
        explain_json = json.loads(json.dumps(results, default=str))
        conn.commit()
        return explain_json
    except Exception as e:
        print(f"Failed to execute query: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()

def run_explain_query(conn, query, explain="BUFFERS ON, ANALYZE ON, COSTS ON, VERBOSE ON, SUMMARY ON, TIMING ON, FORMAT JSON"):
    try:
        cur = conn.cursor()
        # Execute an EXPLAIN (FORMAT JSON) query
        cur.execute(f"EXPLAIN ({explain}) {query}")
        # Fetch the result
        result = cur.fetchone()
        # Convert the result to JSON
        explain_json = json.loads(json.dumps(result[0]))
        conn.commit()
        
        return explain_json

    except Exception as e:
        print(f"Error executing the query: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
   
def extract_scan_info(table, block=-1):
    with open('queryplan.json', 'r') as file:
        data = json.load(file)
        plan = data[0]['Plan']
    scan_info = []
    def traverse_plans(node):
        if "Node Type" in node and "Scan" in node["Node Type"]:
            relation_name = node.get("Relation Name", "N/A")
            alias_name = node.get("Alias", "N/A")
            if relation_name == table:
              filter_condition = node.get("Filter", "N/A")
              scan_info.append((relation_name, alias_name, filter_condition))

        if "Plans" in node:
            for subplan in node["Plans"]:
                traverse_plans(subplan)

    traverse_plans(plan)
    relation, alias_name, filter = scan_info[-1][0], scan_info[-1][1], scan_info[-1][2]
    if relation not in filter:
        filter = filter.replace(f"{alias_name}.", f"{relation}.")
    if block == -1:
        if filter != "N/A":
            query = f"WITH all_blocks AS ( SELECT DISTINCT (ctid::text::point)[0] AS block_id FROM {relation} ) SELECT ab.block_id, COALESCE(COUNT(*), 0) AS tuple_count FROM all_blocks ab LEFT JOIN {relation} ON ab.block_id = ({relation}.ctid::text::point)[0] AND {filter} GROUP BY ab.block_id ORDER BY ab.block_id;"
        else:
            query = f"WITH all_blocks AS ( SELECT DISTINCT (ctid::text::point)[0] AS block_id FROM {relation} ) SELECT ab.block_id, COALESCE(COUNT(*), 0) AS tuple_count FROM all_blocks ab LEFT JOIN {relation} ON ab.block_id = ({relation}.ctid::text::point)[0] GROUP BY ab.block_id ORDER BY ab.block_id;"
    else:
        if filter != "N/A":
            query = f"WITH all_blocks AS ( SELECT (ctid::text::point)[1] AS tuple_id, * FROM {relation} WHERE (ctid::text::point)[0] = {block}) SELECT ab.*, CASE WHEN bc.tuple_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS result_column FROM all_blocks ab LEFT JOIN (SELECT (ctid::text::point)[1] as tuple_id, * FROM {relation} WHERE (ctid::text::point)[0] = {block} AND {filter}) as bc ON ab.tuple_id = bc.tuple_id;"
        else:
            query = f"WITH all_blocks AS ( SELECT (ctid::text::point)[1] AS tuple_id, * FROM {relation} WHERE (ctid::text::point)[0] = {block}) SELECT ab.*, CASE WHEN bc.tuple_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS result_column FROM all_blocks ab LEFT JOIN (SELECT (ctid::text::point)[1] as tuple_id, * FROM {relation} WHERE (ctid::text::point)[0] = {block}) as bc ON ab.tuple_id = bc.tuple_id;"

    return query


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


@app.route('/connection', methods=['POST'])
def connect_to_db():
    try:
        # Parse the body of the request
        data = request.get_json()
        dbname = data.get('dbname')
        username = data.get('username')
        password = data.get('password')
        host = data.get('host')
        port = data.get('port')
        conn = psycopg2.connect(
            dbname=dbname,
            user=username,
            password=password,
            host=host,
            port=port,
        )

        # If the connection is successful, create a token
        token = jwt.encode({
            'user': username,
            'exp': datetime.utcnow() + timedelta(hours=1)  # token expiry set to 1 hour
        }, SECRET_KEY, algorithm='HS256')
        
        CONNECTIONS[token] = conn

        return {"token": token}, 200
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return {"error": "Database connection failed. Try again!"}, 400


@app.route('/blocks', methods=['POST'])
def tuples_in_blocks():
    token = request.headers.get('Authorization').split(" ")[0]
    try:
        data = request.get_json()
        table = data.get('table')
        block = data.get('block')
        column_names = execute_sql_query(CONNECTIONS[token], f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}';")
        accessed_tuples = execute_sql_query(CONNECTIONS[token], extract_scan_info(table, block))
        column_names = [name[0] for name in column_names]

        return {"column_names": column_names, "accessed": accessed_tuples}, 200
    except Exception as e:
        logging.error(f"Error while trying to access block level tuples: {e}")
        return {"error": str(e)}, 400

@app.route('/visuals', methods=['POST'])
def get_blocks_accessed():
    '''
    This function is an endpoint which helps visualise the blocks. It gets a table and returns the tuples accessed from that particular block
    '''
    token = request.headers.get('Authorization').split(" ")[0]
    try:
        data = request.get_json()
        table = data.get('table')
        results = execute_sql_query(CONNECTIONS[token], extract_scan_info(table))
        results = {int(key): value for key,value in results}
        
        return results, 200
    except Exception as e:
        return {"error": str(e)}, 400


@app.route('/query', methods=['POST'])
def handle_query():
    token = request.headers.get('Authorization').split(" ")[0]
    try:
        # Parse the data in the request body
        data = request.get_json()
        query = data.get('query')
        
        # If the token is not in connections, that means a connection to the database was never established
        if token not in CONNECTIONS:
            raise Exception("Invalid or missing authorization. Check the database connection")

        query = remove_linebreaks_and_extra_spaces(query)
        
        # Run the explain query
        reset = execute_sql_query(CONNECTIONS[token], "SELECT pg_stat_reset();")
        print("reset", reset)
        time.sleep(5)
        results = run_explain_query(CONNECTIONS[token], query)
        # time.sleep(5)
        stats = execute_sql_query(CONNECTIONS[token], "SELECT * FROM pg_statio_user_tables;")
        blocks = execute_sql_query(CONNECTIONS[token], "SELECT relname AS table_name, pg_relation_size(pg_class.oid) / current_setting('block_size')::integer AS total_blocks FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE relkind = 'r' AND nspname NOT IN ('pg_catalog', 'information_schema');")
        
        blocks = [{key: value for key, value in blocks}]
        logging.debug(blocks)
        # If the method to explain query didn't work properly and instead just gave an empty response, this condition is executed
        if results is None:
            raise Exception("Failed to execute query")
        # Only to print out the tree in the terminal
        # TODO: Delete this portion later
        else:
            print("EXPLAIN Plan Tree:")
            for plan in results:
                print_plan(plan["Plan"])

        return {"results": results, "statistics": stats, "count": blocks}, 200

    except jwt.ExpiredSignatureError:
        return {"error": "Token has expired"}, 400
    except Exception as e:
        return {"error": str(e)}, 400


def backend():
    logging.basicConfig(level=logging.INFO)
    app.run()

# if __name__ == '__main__':
#     app.run()