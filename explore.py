import json
import psycopg2

operatorSeq = []
parents = []
info = []
time = []
annotations = []

def beginQEPAnnotation():
    deleteQEPAnnotation()
    getQEP("queryplan.json")
    getQEPAnnotation()
    return annotations

def deleteQEPAnnotation():
    operatorSeq.clear()
    parents.clear()
    info.clear()
    time.clear()
    annotations.clear()

def getQEP(filename):
    with open(f"{filename}") as file:
        data = json.load(file)
        plan = data[0][0][0]["Plan"]
        global executionTime
        global planningTime
        global totalCost
        executionTime = data[0][0][0]["Execution Time"]
        planningTime = data[0][0][0]["Planning Time"]
        totalCost = data[0][0][0]["Plan"]["Total Cost"]  
    iterateOverQEP(plan, -1)
    for i in info:
        if 'Plans' in i:
            i.pop('Plans')

def iterateOverQEP(queryplan, parentNo):
    lastNum = parentNo
    operatorSeq.append(queryplan['Node Type'])
    parents.append(parentNo)
    info.append(queryplan)
    time.append(queryplan["Actual Total Time"])
    lastNum = len(parents) - 1
    if "Plans" in queryplan:
        for plan in queryplan["Plans"]:
            iterateOverQEP(plan, lastNum)
    else:
        return
        
def getQEPAnnotation():
    tables = []
    tableCount = 1
    count = 1
    steps = ''
    i = len(operatorSeq) - 1
    while i >= 0:
        nodeType = operatorSeq[i]
        
        if nodeType == 'Aggregate':
            if info[i]['Strategy'] == "Hashed":
                steps += f'Step {count}: Aggregate performed to hash rows of {tables[-1]} based on key {info[i]["Group Key"]}, and the resulting rows will be returned.\n'
            if info[i]['Strategy'] == "Sorted":
                steps += f'Step {count}: Aggregate performed to sort rows of {tables[-1]} based on key {info[i]["Group Key"]}, and the resulting rows will be returned.\n'
            if info[i]['Strategy'] == "Plain":
                steps += f'Step {count}: Aggregate performed on {tables[-1]} and resulting rows will be returned.\n'

        elif nodeType == 'Append':
            steps += f'Step {count}: Append results from {tables.pop(0)} to {tables.pop(0)} to get intermediate table T{tableCount}.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Bitmap Heap Scan':
            steps += f'Step {count}: Bitmap Heap Scan performed on \'{info[i]["Relation Name"]}\' to decode bitmap'
            if 'Filter' in info[i]:
                steps+= f' with filter {info[i]["Filter"]}'
            tables.append(info[i]["Relation Name"])
            steps += '.\n'

        elif nodeType == 'CTE_Scan':
            steps += f'Step {count}: CTE_Scan performed on relation'
            tables.append(info[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in info[i]:
                steps += f' with condition(s): {info[i]["Index Cond"]}.\n'

        elif nodeType == 'Gather':
            steps += f'Step {count}: Gather performed on {tables[-1]} to get intermediate table T{tableCount}.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Gather Merge':
            steps += f'Step {count}: Gather Merge performed on {tables[-1]}.\n'

        elif nodeType == 'Hash':
            steps += f'Step {count}: Hashing performed on \'{tables[-1]}\'.\n'

        elif nodeType == 'Hash Join':
            steps += f'Step {count}: Hash {info[i]["Join Type"]} join performed under condition(s) of {info[i]["Hash Cond"]} for {tables.pop(0)} and {tables.pop(0)} to generate intermediate table T{tableCount} as it is ideal to process inputs that are large, unsorted and non-indexed.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Index Only Scan':
            steps += f'Step {count}: Index Only scan performed on \'{info[i]["Index Name"]}\'as data could be access from indexes directly'
            tables.append(info[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in info[i]:
                steps += f' under condition(s): {info[i]["Index Cond"]}'
            if 'Filter' in info[i]:
                steps+= f' with filter{info[i]["Filter"]}'
            steps += '.\n'

        elif nodeType == 'Index Scan':
            steps += f'Step {count}: Index Scan performed on \'{info[i]["Relation Name"]}\' due to high selectivity of'
            if 'Filter' in info[i]:
                steps+= f' {info[i]["Filter"]}'
            tables.append(info[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in info[i]:
                steps += f' {info[i]["Index Cond"]}'
            steps += '.\n'

        elif nodeType == 'Incremental Sort':
            steps += f'Step {count}: Incremental Sort performed on {tables[-1]} with Sort Key {info[i]["Sort Key"]} \n'

        elif nodeType == 'Limit':
            steps += f'Step {count}: Perform Limit to limit scanning to {info[i]["Plan Rows"]} rows.\n'

        elif nodeType == 'Materialize':
            steps += f'Step {count}: Materialization performed on {tables[-1]}.\n'

        elif nodeType == 'Memoize':
            steps += f'Step {count}: Perform Memoize of {tables[-1]} with respect to Cache key: {info[i]["Cache Key"]}.\n'

        elif nodeType == 'Merge Append':
            steps += f'Step {count}: Results from table {tables.pop(0)} are appended to {tables.pop(0)} to generate intermediate table T{tableCount}.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Merge Join':
            steps += f'Step {count}: Merge Join performed for {tables.pop(0)} and {tables.pop(0)} under condition(s) of {info[i]["Merge Cond"]} to generate intermediate table T{tableCount}. This is done because join inputs are large and are sorted on their join attributes.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Nested Loop':
            steps += f'Step {count}: Nested Loop operation performed for {tables.pop(0)} and {tables.pop(0)} to generate intermediate table T{tableCount} as one join input is small  (e.g., fewer than 10 rows) and other join input is large and indexed on its join attributes.\n'
            tables.append(f'T{tableCount}')
            tableCount += 1

        elif nodeType == 'Seq Scan':
            steps += f'Step {count}: Sequential Scan performed on \'{info[i]["Relation Name"]}\''
            if 'Filter' in info[i]:
                steps+= f' due to low selectivity of {info[i]["Filter"]}'
            steps += '.\n'
            tables.append(info[i]["Relation Name"])

        elif nodeType == 'SetOp':
            steps += f'Step {count}: Set operation performed on {tables[-1]}\n'

        elif nodeType == 'Sort':
            steps += f'Step {count}: Sort performed on rows in {tables[-1]} based on {info[i]["Sort Key"]})\n'
            if info[i]["Sort Key"][0].find("INC") != -1:
                steps += f'in ascending order.\n'
            if info[i]["Sort Key"][0].find("DESC") != -1:
                steps += f'in descending order.\n'

        else:
            steps += f'Undefined node found!.\n'

        count += 1
        i -= 1
    
    steps += f"\nExecution Time = {executionTime} ms\n"
    steps += f"Planning Time = {planningTime} ms\n"
    steps += f"Total Cost = {totalCost}\n"
    annotations.append(steps)
    return steps

def executeQuery(text, port_value, host_value, database_value, user_value, password_value):
    try:
        con = psycopg2.connect(
            port=port_value,
            host=host_value,
            database=database_value,
            user=user_value,
            password=password_value
        )
        cursor = con.cursor()
        cursor.execute("EXPLAIN (ANALYZE, VERBOSE, COSTS, FORMAT JSON) " + text)
        queryOutput = cursor.fetchall()
        queryExecuted = True
        cursor.close()
    except(Exception, psycopg2.DatabaseError) as error:
        queryExecuted = False

    with open('queryplan.json', 'w') as f:
        json.dump(queryOutput, f, ensure_ascii=False, indent=2)

    return queryExecuted

# function to create the QEP Tree Diagram TO-DO
def createQEPTree():

    with open('queryplan.json') as json_file:
        data = json.load(json_file)
    dict_plan_inner = data[0][0]

    #TO-DO