from py2neo import Graph, Node, Relationship
graph1 = Graph("http://localhost:7474/db/data/",user="neo4j",password="864997032")
tx = graph1.begin()
graph1.data("create(n:year)")
b= graph1.data("match(n) return(n)")
print b
tx.commit() 