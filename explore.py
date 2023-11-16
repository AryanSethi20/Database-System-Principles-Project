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

SECRET_KEY = 'DBSP_PROJECT_2'

def extract_string_before_word(text, word):
    '''
    Function used to extract the subquery from a list of subqueries
    '''
    # Find the position of the word
    word_pos = text.find(word)
    if word_pos == -1:
        return None  # Word not found

    # Iterate backwards to find the first closing bracket before the word
    for i in range(word_pos, -1, -1):
        if text[i] == ')':
            closing_bracket_pos = i
            break
    else:
        return None  # Closing bracket not found

    # print(closing_bracket_pos)
    # Use a stack to find the corresponding opening bracket
    stack = []
    for i in range(closing_bracket_pos, -1, -1):
      if text[i]==')':
        stack.append(')')
      elif text[i]=='(':
        stack.pop()
        if len(stack)==0:
          return text[i+1:closing_bracket_pos]
    return None

def extract_table_names(query):
    '''
    Function used to find out all the tables in the queries and its aliases
    '''
    parser = Parser(query)
    query = parser.generalize
    parser = Parser(query)
        
    relations = parser.tables
    # print(relations)
    pattern_for_subquery_alias = r'\(.SELECT.*?\)\s+AS\s+(\w+)'
    # pattern_for_subquery_columns = r'\(.SELECT.)'
    # pattern = r'\(([^()]*\([^()]*\))*[^()]*\)\s+AS\s+(\w+)'

    # Find all matches in the query
    matches = re.findall(pattern_for_subquery_alias, query, re.IGNORECASE | re.DOTALL)
    print("Aliases: ", matches)
    sql_keywords = [
        'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'JOIN',
        'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'OUTER JOIN', 'ON', 'AS',
        'DISTINCT', 'UNION', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
        'DELETE', 'CREATE', 'TABLE', 'ALTER', 'ADD', 'DROP', 'INDEX',
        'CONSTRAINT', 'PRIMARY KEY', 'FOREIGN KEY', 'REFERENCES', 'CASCADE',
        'TRUNCATE', 'COMMIT', 'ROLLBACK', 'BEGIN', 'END', 'IF', 'ELSE',
        'CASE', 'WHEN', 'THEN', 'EXISTS'
        ]
    subqueries = []
    # pdb.set_trace()
    for match in matches:
        subquery = extract_string_before_word(query,match)
        if subquery!=None:
            subqueries.append(subquery)
    # for k, v in matches:
    #     for i in range(len(relations)):
    #         if relations[i]==v and k.upper() not in sql_keywords:
    #             relations[i] = k
    #             break
    print(subqueries)
    return relations

def ctid_query(query):
    '''Extract block and position in block using ctid'''
    print('ctid_query')
    relations=extract_table_names(query)
    #ctid_list=[]
    
    modified_query_ctid="SELECT "
    if 'group by' in query.lower(): #If there is GROUP BY clause
        for i,relation in enumerate(relations):
            modified_query_ctid+=f'ARRAY_AGG({relation}.ctid) AS {relation}_ctid '
            #ctid_list.append(f'{relation}_ctid')
            if i+1<len(relations):
                modified_query_ctid+=', '
    else:
        for i,relation in enumerate(relations):
                modified_query_ctid +=f"{relation}.ctid "
                if i+1<len(relations):
                    modified_query_ctid+=', '
    
    from_index = query.upper().find('FROM')
    modified_query_ctid+=query[from_index:]
    
    order_index =modified_query_ctid.upper().find('ORDER BY')
    modified_query_ctid=modified_query_ctid[:order_index]

    return modified_query_ctid

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

def add_ctids_to_query(query, tables):
    '''
    This function takes a query as an input and returns a list of all the tables being accessed in that list
    '''
    query = query.upper()
    parser = Parser(query)

    if tables==None:
        tables = parser.tables

    print("Tables:", tables)
    # Splitting the query into parts
    parts = query.split("SELECT", 1)

    # Handling edge case where "SELECT" isn't found or is used in column names, table names, etc.
    if len(parts) < 2:
        raise ValueError("Query does not contain a SELECT statement or is malformed.")

    before_select, after_select = parts[0], "SELECT" + parts[1]

    # Adding ctid for each table
    ctid_selects = [f"{table}.ctid AS {table}_ctid" for table in tables]
    ctid_group_by_selects = [f"{table}.ctid" for table in tables]
    modified_select = after_select.replace("SELECT", "SELECT " + ", ".join(ctid_selects) + ", ", 1)
    modified_select = modified_select.replace("GROUP BY", "GROUP BY "+ ", ".join(ctid_group_by_selects) + ", ", 1)

    # Reassembling the query
    modified_query = before_select + modified_select
    order_index = modified_query.upper().find('ORDER BY')
    modified_query=modified_query[:order_index]
    
    print(modified_query)
    return modified_query

def find_alias(sql_query):
    '''
    Function receives a subquery token (more than one), and returns all the aliases
    '''
    stack = []
    aliases = []
    i = 0

    while i < len(sql_query):
        char = sql_query[i]

        if char == '(':
            stack.append(i)
        elif char == ')' and stack:
            start = stack.pop()

            if not stack:  # Stack is empty, indicating the end of a subquery
                # Extract alias using regex
                match = re.search(r'\)\s+AS\s+(\w+)', sql_query[i:])
                if match:
                    aliases.append(match.group(1))
                    i += match.end()  # Skip ahead to after the matched alias

        i += 1

    return aliases

def is_subselect(parsed):
    if not isinstance(parsed[0], Parenthesis):
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def find_subquery_aliases(query):
    # pdb.set_trace()
    aliases = []
    parsed = sqlparse.parse(query)

    for statement in parsed:
        for token in statement.tokens:
            if is_subselect(token):
                # Find the next token after the parenthesis which should be the alias
                next_token_index = statement.token_index(token) + 1
                if next_token_index < len(statement.tokens):
                    next_token = statement.tokens[next_token_index]
                    if isinstance(next_token, Identifier):
                        aliases.append(next_token.get_name())
            elif isinstance(token, Identifier):
                # This is for handling nested subqueries
                for subtoken in token.tokens:
                    if is_subselect(subtoken):
                        alias = token.get_name()
                        if alias and alias not in aliases:
                            aliases.append(alias)

    return aliases

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

        return jsonify({"token": token})
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return jsonify({"error": "Unable to connect to the database"})


@app.route('/tables', methods=['GET'])
def database_tables():
    token = request.headers.get('Authorization').split(" ")[0]
    try:
        tables = execute_sql_query(CONNECTIONS[token], "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname!='pg_catalog' AND schemaname!='information_schema'")
        tables = [j for i in tables for j in i]
        return {"Tables": tables}
    except Exception as e:
        return {"Error:": str(e)}
    

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
        type = data.get('type')
        
        # If the token is not in connections, that means a connection to the database was never established
        if token not in CONNECTIONS:
            return jsonify({"error": "Invalid or missing authorization. Check the database connection"})

        query = remove_linebreaks_and_extra_spaces(query)
        
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
    query = """select
      supp_nation,
      cust_nation,
      l_year,
      sum(volume) as revenue
    from
      (
        select
          n1.n_name as supp_nation,
          n2.n_name as cust_nation
        from
          supplier,
        where
          s_suppkey = l_suppkey
      ) as shipping,
      (
        select
          n1.n_name as supp_nation,
          n2.n_name as cust_nation
        from
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2
        where
          s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
          )
          and l_shipdate between '1995-01-01' and '1996-12-31'
          and o_totalprice > 100
          and c_acctbal > 10
      ) as dropshipping
    group by
      supp_nation,
      cust_nation,
      l_year
    order by
      supp_nation,
      cust_nation,
      l_year;"""

    # Example usage
#     query = """
#     SELECT s_suppkey, s_name, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
# FROM supplier
#          JOIN lineitem ON s_suppkey = l_suppkey
# GROUP BY s_suppkey, s_name
# ORDER BY total_revenue DESC
#     """
    # modified_query = add_ctids_to_query(query)
    # print(modified_query)
    # add_ctids_to_query(query)
    # query = remove_linebreaks_and_extra_spaces(query)
    # print(query)
    # print(extract_table_names(query))
    app.run()
