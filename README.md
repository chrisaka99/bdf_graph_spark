# bdf_graph_spark

Ce projet Spark simule le parcours en largeur d'un graphe parallélisable. Chaque étape de profondeur dans le parcours correspond l'exécution d'une tâche map/reduce – avec une fonction map executée pour chaque nœud à chaque étape.

- clonez le projet
  
    `git clone https://github.com/boomiin/bdf_graph_spark.git`


- sur la machine virtuelle:

    + mettre le fichier graphe.txt sur hdfs

        `hadoop fs -put graphe.txt /`

    + definir l'environnement python sur python 3


        `export PYSPARK_PYTHON=python3`

    + lancer l'interpreteur avec spark

        `pyspark --master "local[2]"`

    + copier les instructions du graph_spark.py dans l'interpreteur python
    + Après avoir fini de copier les instructions, sortir de l'interpreteur python


        `exit()`

    +  consulter les resultats finaux dans le fichier de sortie:
  
        `hadoop fs -cat /user/mbds/graph_results/*`

