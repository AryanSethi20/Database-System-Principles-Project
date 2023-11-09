import json
import psycopg2

operatorSeqList = []
parents = []
informations = []
timing = []
listofAnnotations = []
executionTime = 0
planningTime = 0
totalCost = 0

def startAnnotation():
    clearAnnotation()
    queryplan = "queryplan.json"
    plan = getPlan(queryplan)
    getFullPlan(plan)
    annotate()
    return listofAnnotations

def clearAnnotation():
    operatorSeqList.clear()
    parents.clear()
    informations.clear()
    timing.clear()
    listofAnnotations.clear()

def getPlan(filename):
    with open(f"{filename}") as file:
        data = json.load(file)
        plan = data[0][0][0]["Plan"]
        global executionTime
        global planningTime
        global totalCost
        executionTime = data[0][0][0]["Execution Time"]
        planningTime = data[0][0][0]["Planning Time"]
        totalCost = data[0][0][0]["Plan"]["Total Cost"]
    return plan

def getFullPlan(plan):
    planIterator(plan, -1)
    for i in informations:
        if 'Plans' in i:
            i.pop('Plans')

def planIterator(queryplan, parentNo):
    lastNum = parentNo
    operatorSeqList.append(queryplan['Node Type'])
    parents.append(parentNo)
    informations.append(queryplan)
    timing.append(queryplan["Actual Total Time"])
    lastNum = len(parents) - 1
    if "Plans" in queryplan:
        for plan in queryplan["Plans"]:
            planIterator(plan, lastNum)
    else:
        return
        
def annotate():
    intermediateTables = []
    intermediateTableCount = 1
    count = 1
    descriptions = ''
    i = len(operatorSeqList) - 1
    while i >= 0:
        nodeType = operatorSeqList[i]
        # If performing Sequential Scan
        if nodeType == 'Seq Scan':
            descriptions += f'Step {count}: Sequential Scan is performed on \'{informations[i]["Relation Name"]}\''
            if 'Filter' in informations[i]:
                descriptions+= f' due to low selectivity of {informations[i]["Filter"]}'
            descriptions += '.\n'
            intermediateTables.append(informations[i]["Relation Name"])
        # If performing Index Scan
        elif nodeType == 'Index Scan':
            descriptions += f'Step {count}: Index Scan is performed on \'{informations[i]["Relation Name"]}\' due to high selectivity of'
            if 'Filter' in informations[i]:
                descriptions+= f' {informations[i]["Filter"]}'
            intermediateTables.append(informations[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in informations[i]:
                descriptions += f' {informations[i]["Index Cond"]}'
            descriptions += '.\n'
        # If performing Index Only Scan
        elif nodeType == 'Index Only Scan':
            descriptions += f'Step {count}: Index Only scan is performed on \'{informations[i]["Index Name"]}\'as data could be access from indexes directly'
            intermediateTables.append(informations[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in informations[i]:
                descriptions += f' under condition(s): {informations[i]["Index Cond"]}'
            if 'Filter' in informations[i]:
                descriptions+= f' with filter{informations[i]["Filter"]}'
            descriptions += '.\n'
        # If performing Bitmap Heap Scan operation
        elif nodeType == 'Bitmap Heap Scan':
            descriptions += f'Step {count}: Bitmap Heap Scan is performed on \'{informations[i]["Relation Name"]}\' to decode bitmap'
            if 'Filter' in informations[i]:
                descriptions+= f' with filter {informations[i]["Filter"]}'
            intermediateTables.append(informations[i]["Relation Name"])
            descriptions += '.\n'
        # If performing Hash operation
        elif nodeType == 'Hash':
            descriptions += f'Step {count}: Hashing is performed on \'{intermediateTables[-1]}\'.\n'
        # If performing Hash Join operation
        elif nodeType == 'Hash Join':
            descriptions += f'Step {count}: Hash {informations[i]["Join Type"]} join is performed under condition(s) of {informations[i]["Hash Cond"]} for {intermediateTables.pop(0)} and {intermediateTables.pop(0)} to generate intermediate table T{intermediateTableCount} as it is ideal to process inputs that are large, unsorted and non-indexed.\n'
            intermediateTables.append(f'T{intermediateTableCount}')
            intermediateTableCount += 1
        # If performing Merge Join operation
        elif nodeType == 'Merge Join':
            descriptions += f'Step {count}: Merge Join is performed for {intermediateTables.pop(0)} and {intermediateTables.pop(0)} under condition(s) of {informations[i]["Merge Cond"]} to generate intermediate table T{intermediateTableCount}. This is done because join inputs are large and are sorted on their join attributes.\n'
            intermediateTables.append(f'T{intermediateTableCount}')
            intermediateTableCount += 1
        # If performing Merge Append operation
        elif nodeType == 'Merge Append':
            descriptions += f'Step {count}: Results from table {intermediateTables.pop(0)} are appended to {intermediateTables.pop(0)} to generate intermediate table T{intermediateTableCount}.\n'
            intermediateTables.append(f'T{intermediateTableCount}')
            intermediateTableCount += 1
        # If performing Aggregation operation
        elif nodeType == 'Aggregate':
            if informations[i]['Strategy'] == "Hashed":
                descriptions += f'Step {count}: Aggregate is performed to hash rows of {intermediateTables[-1]} based on {informations[i]["Group Key"]}, and resulting rows will be returned.\n'
            if informations[i]['Strategy'] == "Sorted":
                descriptions += f'Step {count}: Aggregate is performed to sort rows of {intermediateTables[-1]} based on {informations[i]["Group Key"]}, and resulting rows will be returned.\n'
            if informations[i]['Strategy'] == "Plain":
                descriptions += f'Step {count}: Aggregate is performed on {intermediateTables[-1]} and resulting rows will be returned.\n'
        # If performing Gather operation
        elif nodeType == 'Gather':
            descriptions += f'Step {count}: Gather is performed on {intermediateTables[-1]} to get intermediate table T{intermediateTableCount}.\n'
            intermediateTables.append(f'T{intermediateTableCount}')
            intermediateTableCount += 1
        # If performing Gather Merge operation
        elif nodeType == 'Gather Merge':
            descriptions += f'Step {count}: Gather Merge is performed on {intermediateTables[-1]}.\n'
        # If performing Append operation
        elif nodeType == 'Append':
            descriptions += f'Step {count}: Append results from {intermediateTables.pop(0)} to {intermediateTables.pop(0)} to get intermediate table T{intermediateTableCount}.\n'
            intermediateTables.append(f'T{intermediateTableCount}')
            intermediateTableCount += 1
        # If performing Sort operation
        elif nodeType == 'Sort':
            descriptions += f'Step {count}: Sort is performed on rows in {intermediateTables[-1]} based on {informations[i]["Sort Key"]})\n'
            if informations[i]["Sort Key"][0].find("INC") != -1:
                descriptions += f'in ascending order.\n'
            if informations[i]["Sort Key"][0].find("DESC") != -1:
                descriptions += f'in descending order.\n'
        # If performing Incremental Sort operation
        elif nodeType == 'Incremental Sort':
            descriptions += f'Step {count}: Incremental Sort is performed on {intermediateTables[-1]} with Sort Key {informations[i]["Sort Key"]} \n'
        # If performing Materialize operation
        elif nodeType == 'Materialize':
            descriptions += f'Step {count}: Materialization is performed on {intermediateTables[-1]}.\n'
        # If performing Modify Table operation
        elif nodeType == 'ModifyTable':
            descriptions += f'Step {count}: Modification is performed on \'{informations[i]["Relation Name"]}\'.\n'
        # If performing CTE_Scan operation
        elif nodeType == 'CTE_Scan':
            descriptions += f'Step {count}: CTE_Scan is performed on relation'
            intermediateTables.append(informations[i]["Relation Name"])
            # If index cond exist
            if "Index Cond" in informations[i]:
                descriptions += f' with condition(s): {informations[i]["Index Cond"]}.\n'
        # If performing Nested Loop operation
        elif nodeType == 'Nested Loop':
            descriptions += f'Step {count}: Nested Loop operation is performed for {intermediateTables.pop(0)} and {intermediateTables.pop(0)} to generate intermediate table T{intermediateTableCount} as one join input is small  (e.g., fewer than 10 rows) and other join input is large and indexed on its join attributes.\n'
            intermediateTables.append(f'T{intermediateTableCount}')
            intermediateTableCount += 1
        # If performing Limit operation
        elif nodeType == 'Limit':
            descriptions += f'Step {count}: Perform Limit to limit scanning to {informations[i]["Plan Rows"]} rows.\n'
        elif nodeType == 'SetOp':
            descriptions += f'Step {count}: Set operation is performed on {intermediateTables[-1]}\n'
        elif nodeType == 'Memoize':
            descriptions += f'Step {count}: Perform Memoize of {intermediateTables[-1]} with respect to Cache key: {informations[i]["Cache Key"]}.\n'
        else:
            descriptions += f'Undefined node found!.\n'
        count += 1
        i -= 1
    descriptions += f"\nExecution Time = {executionTime} ms\n"
    descriptions += f"Planning Time = {planningTime}ms\n"
    descriptions += f"Total Cost = {totalCost}\n"
    listofAnnotations.append(descriptions)
    return descriptions

def runQuery(text, port_value, host_value, database_value, user_value, password_value):
    sql_string = "EXPLAIN (analyze, verbose, costs, format JSON) " + text

    try:
        con = psycopg2.connect(
            port=port_value,
            host=host_value,
            database=database_value,
            user=user_value,
            password=password_value
        )
        cursor = con.cursor()
        cursor.execute(sql_string)
        queryOutput = cursor.fetchall()
        queryExecuted = True
        cursor.close()
    except(Exception, psycopg2.DatabaseError) as error:
        queryOutput = "Please check your sql statement: \n" + text
        queryExecuted = False

    with open('queryplan.json', 'w') as f:
        json.dump(queryOutput, f, ensure_ascii=False, indent=2)

    return queryExecuted

# function to create the QEP Tree Diagram TO-DO
def createQEPTreeDiagram():

    with open('queryplan.json') as json_file:
        data = json.load(json_file)
    dict_plan_inner = data[0][0]

    #TO-DO