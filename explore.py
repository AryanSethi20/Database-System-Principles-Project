from flask import Flask, request, jsonify
import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta

# Storing the DB Connections and credentials in memory
CONNECTIONS = {}

app = Flask(__name__)

SECRET_KEY = 'DBSP_PROJECT_2'

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

        return jsonify({"token": token})
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return jsonify({"error": "Unable to connect to the database"})

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
        return None
    finally:
        cur.close()


def run_explain_query(conn, query):
    try:
        cur = conn.cursor()
        # Execute an EXPLAIN (FORMAT JSON) query
        cur.execute(f"EXPLAIN (BUFFERS ON, ANALYZE ON, COSTS ON, VERBOSE ON, SUMMARY ON, TIMING ON, FORMAT JSON) {query}")
        # Fetch the result
        result = cur.fetchone()
        # Convert the result to JSON
        explain_json = json.loads(json.dumps(result[0]))
        return explain_json

    except Exception as e:
        print(f"Error executing the query: {e}")
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


@app.route('/query', methods=['POST'])
def handle_query():
    token = request.headers.get('Authorization').split(" ")[0]
    try:
        # Parse the data in the request body
        data = request.get_json()
        query = data.get('query')
        type = data.get('type')
        
        # If the token is not in connections, that means a connection to the database was never established
        if token not in CONNECTIONS:
            return jsonify({"error": "Invalid or missing authorization. Check the database connection"})

        # check the type of response required and execute the function accordingly
        if type=='explain':
            results = run_explain_query(CONNECTIONS[token], query)
        elif type=='execute':
            results = execute_sql_query(CONNECTIONS[token], query)

        # If the method to either execute or explain query didn't work porpoerly and instead just gave an empty response, this condition is executed
        if results is None:
            return jsonify({"error": "Failed to execute query."})
        # Only to print out the tree in the terminal
        # TODO: Delete this portion later
        elif results!=None and type=='explain':
            print("EXPLAIN Plan Tree:")
            for plan in results:
                print_plan(plan["Plan"])

        return results

    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"})
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run()
