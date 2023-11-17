from flask import Flask, request, jsonify
import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta
from sql_metadata import Parser
import sqlparse
from sqlparse.sql import Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML
import re
import pdb

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
        return explain_json

    except Exception as e:
        print(f"Error executing the query: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
   

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

        return {"token": token}, 201
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return {"error": "Database connection failed. Try again!"}, 403


# @app.route('/tables', methods=['GET'])
# def database_tables():
#     token = request.headers.get('Authorization').split(" ")[0]
#     try:
#         tables = execute_sql_query(CONNECTIONS[token], "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname!='pg_catalog' AND schemaname!='information_schema'")
#         tables = [j for i in tables for j in i]
#         return {"Tables": tables}, 200
#     except Exception as e:
#         return {"Error:": str(e)}, 400
    

@app.route('/block', methods=['POST'])
def get_blocks_accessed():
    '''
    This function is an endpoint which helps visualise the blocks. It gets a table and its particular block as part of the body in the request, and returns
    tuples in that block and some statistics related to the block
    '''
    token = request.headers.get('Authorization').split(" ")[0]
    try:
        data = request.get_json()
        table = data.get('table')
        block = data.get('block')
        query = data.get('query')
        ctid_results = execute_sql_query(CONNECTIONS[token], f"SELECT ctid, * FROM {table} WHERE (ctid::text::point)[0] = {block-1} ORDER BY ctid")
        count = execute_sql_query(CONNECTIONS[token], f"SELECT COUNT(*) FROM {table} WHERE (ctid::text::point)[0] = {block-1}")
        # res = run_explain_query(CONNECTIONS[token], query, f"ANALYZE, BUFFERS ON, COSTS OFF, FORMAT JSON")
        return {"Count": count[0][0], "query": ctid_results}
    except Exception as e:
        return jsonify({"error": str(e)})

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
        stats = execute_sql_query(CONNECTIONS[token], "SELECT pg_stat_reset();")
        results = run_explain_query(CONNECTIONS[token], query)
        stats = execute_sql_query(CONNECTIONS[token], "SELECT * FROM pg_statio_user_tables;")

        # If the method to explain query didn't work properly and instead just gave an empty response, this condition is executed
        if results is None:
            raise Exception("Failed to execute query")
        # Only to print out the tree in the terminal
        # TODO: Delete this portion later
        else:
            print("EXPLAIN Plan Tree:")
            for plan in results:
                print_plan(plan["Plan"])

        return {"results": results, "statistics": stats}, 200

    except jwt.ExpiredSignatureError:
        return {"error": "Token has expired"}, 400
    except Exception as e:
        return {"error": str(e)}, 400


def backend():    
    app.run()