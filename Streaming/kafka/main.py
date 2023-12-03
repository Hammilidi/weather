import threading
import os

def execute_cassandra_script():
    os.system('python cassandra.py')

def execute_mongodb_script():
    os.system('python mongo.py')

# Créer des threads pour exécuter les scripts en parallèle
cassandra_thread = threading.Thread(target=execute_cassandra_script)
mongodb_thread = threading.Thread(target=execute_mongodb_script)

# Démarrer les threads
cassandra_thread.start()
mongodb_thread.start()

# Attendre que les threads se terminent
cassandra_thread.join()
mongodb_thread.join()
