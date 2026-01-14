# Etapes d'Installation

> Pour ce projet nous allons utiliser miniconda  

Activation de l'environnement et installation des bibliotèques avec les bonnes versions (le nom de l'environnement datadebat peut etre changer):
```
conda create -n datadebat python=3.11
conda activate datadebat
pip install -r requirements.txt
```

Pour lancer l'ETL : src/etl/orchestrator.py
    Pour lancer sur plusieurs annees : 
        aller dans le main 
        changer les parametre en  
        download : si vous voulez télécharger les fichiers bruts les .taz
        parallel : si vous voulez paralléliser le code 
        max workers : nombre de coeurs utiliser pendant la parallélisation 
