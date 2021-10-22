### les fonctions mapper et reducer sont les pseudos-codes du cours traduits en Python

def mapper(tupleA):
    """ Fonction mapper """
    key, value = tupleA[0], tupleA[1]
    new_graph = []
    if value[1] == "GRIS":
        for fils in value[0].split(','):
            if fils == "":
                continue
            fils_couleur = "GRIS"
            fils_profondeur = int(value[2]) + 1
            fils_t = ["", fils_couleur, fils_profondeur]
            new_graph.append((fils, fils_t))
        value[1] = "NOIR"
    new_graph.append((key, value))
    return new_graph

def reducer(list_values):
    """ Fonction reducer """
    global compteur
    h_children, h_depth, h_color = [], -1, "BLANC"
    dic = {
        "BLANC": 0,
        "GRIS": 1,
        "NOIR": 2
    }
    for val in list_values:
        if len(val[0].split(",")) >= len(h_children):
            h_children = val[0].split(",")
        if int(val[2]) > h_depth:
            h_depth = int(val[2])
        if dic[val[1]] > dic[h_color]:
            h_color = val[1]
    if h_color != "NOIR":
        compteur.add(1)
    new_node = [",".join(h_children), h_color, h_depth]
    return new_node

#compteur = sc.accumulator(1)
graph_input = sc.textFile("/graphe.txt")
graph_input.collect() # On obtient ['1\t2,5|GRIS|0', '2\t3,4|BLANC|-1', '3\t6|BLANC|-1', '4\t|BLANC|-1', '5\t6|BLANC|-1', '6\t|BLANC|-1']
graph_splitted1 = graph_input.map(lambda x: x.split("\t"))
graph_splitted1.collect() # On obtient [['1', '2,5|GRIS|0'], ['2', '3,4|BLANC|-1'], ['3', '6|BLANC|-1'], ['4', '|BLANC|-1'], ['5', '6|BLANC|-1'], ['6', '|BLANC|-1']]
graph_splitted2 = graph_splitted1.mapValues(lambda x: x.split('|'))
graph_splitted2 = graph_splitted2.sortByKey(lambda x: x[0])
graph_splitted2.collect() 
# On obtient [('1', ['2,5', 'GRIS', '0']), ('2', ['3,4', 'BLANC', '-1']), ('3', ['6', 'BLANC', '-1']), ('4', ['', 'BLANC', '-1']), ('5', ['6', 'BLANC', '-1']), ('6', ['', 'BLANC', '-1'])]
compteur = sc.accumulator(1)
iteration=0
while compteur.value > 0:
    iteration=iteration+1
    print("-----------------", "ITERATION ",iteration, "-----------------")
    compteur = sc.accumulator(0)
    #map
    node1 = graph_splitted2.flatMap(mapper)
    print("RESULTAT MAP :")
    node1.collect()
    # shuffle 1
    node2 = node1.groupByKey()
    #node2.collect()
    # reduce 1
    node3 = node2.mapValues(reducer)
    node3 = node3.sortBy(lambda x: x[0])
    print("RESULTAT REDUCE :")
    node3.sortByKey(lambda x: x[0]).collect()
    graph_splitted2=node3

# s'assurer que le nom de fichier n'existe pas déjà!
graph_splitted2.saveAsTextFile("graph_results")

### Pour consulter les resultats dans le fichier
#  hadoop fs -ls /user/mbds/*
#  hadoop fs -cat /user/mbds/graph_results/*