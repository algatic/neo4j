from neo4j import GraphDatabase, RoutingControl
import pymongo
from pymongo import MongoClient
#from win32con import NULL

URI = "bolt://localhost:7687"
AUTH = ""

result = []
records = []
order_save = []
objectID = ""

username = ''
password = ''
host = 'localhost'
port = 27017
database = 'software'

client = MongoClient(f"mongodb://{host}:{port}/{database}")
db = client["sca"]
collection = db[database]

driver = GraphDatabase.driver(URI, auth=(AUTH))

def process_node_ids(node_ids):
    if not node_ids:
        return []

    # 初始化结果二维数组
    result_rt = []
    current_row = [node_ids[0][0]]  # 初始化第一行
    j = 0
    # 遍历数组中的每个节点id
    for i in range(1, len(node_ids)):
        prev_id = node_ids[i - 1][0]
        current_id = node_ids[i][0]

        # 查询prev_id和current_id是否是depend关系
        if node_ids[i-1][1] == 0:
            current_row.append(current_id)
        else:
            #print("进了else")
            result_rt.append(current_row)
            head_id = save_node[j]
            j += 1
            current_row = [head_id, current_id]

    # 添加最后一行到结果中
    result_rt.append(current_row)
    # 返回最终的结果
    return result_rt

def get_adj(n_id):
    dep_all = []
    result = session.run("MATCH (c:Software)-[r:DEPEND]-> (n:Software) where id(c)=$neo_id return id(n)",neo_id = n_id)
    for record in result:
        dep_all.append(record["id(n)"])
    return dep_all

def dfs_neo4j(n_id, visited=None):
        if visited is None:
            visited = set()

        visited.add(n_id)
        #print(id)  # 在这里处理每个访问的节点，可以根据需要修改
        i = 0
        adj = get_adj(n_id)  # 获得该点的全部第一层
        if(len(adj) == 0):
            i = 1
        order_save.append([n_id,i])#如果后边是1，意味着下一个点是重新开始的点了

        j = 0
        for neighbor in adj:
            j += 1
            if neighbor not in visited and len(order_save)<5:
                if j > 1:#遇到分岔了，上一个遍历到的neibor是分岔的新开始
                    save_node.append(n_id)#放在dfs前面，不然就会出现后面的叶节点在根节点前先被append到分岔node队列里的情况
                dfs_neo4j(neighbor, visited)

            #save_node.append(n_id)

if __name__ == "__main__":
    #collection_software = db['software']
    for document in collection.find():
        with driver.session() as session:
            #print(document["neo_id"])
            order_save = []
            save_node = []
            dfs_neo4j(int(document["neo_id"]))
            #print(order_save,"1")
            final_result = process_node_ids(order_save)
            #print(final_result)
            query = {'neo_id': document["neo_id"]}
            new_value = {'$set': {'relation_graph': final_result}}
            collection.update_one(query, new_value)
    driver.close()