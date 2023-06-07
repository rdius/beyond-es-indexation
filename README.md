# beyond-es-indexation
![alt tag](./img/beyond.png)

#### Please, provide the data-examples folder with these files: 

```
-   taxon_habitats = "./data-examples/relations.full.tsv"
-   taxon_phenotypes = "./data-examples/phenotype-relations.full.tsv"
-   taxon_uses = "./data-examples/uses-relations.full.tsv"
```
#### Build the Jsonl file by using :

```
beyond_relations_preprocess.ipynb
```
#### Index Data to ES using Logstash

```
sudo /usr/share/logstash/bin/logstash -f 'beyond_conf.conf' #manage to give the absolute path for the .conf
```

### NB : It's supposed that you have already installed ES and Logstash on your computer (or server). If not, check the guid below

#### ElasticSearch

```
https://www.elastic.co/guide/en/elasticsearch/reference/current/deb.html
```

#### kibana

```
https://www.elastic.co/guide/en/kibana/current/deb.html
```

#### START

```
sudo systemctl start elasticsearch.service #start lesaticserach
```

```
sudo systemctl start kibana.service #start kibana
```
------------------------------------------------------------------------------


# Dasboard for Beyond Advanced Queries
![alt tag](./img/queries.png)

### Dependances : 

> This app use [streamlit](https://docs.streamlit.io/library/get-started/installation) as fontend. Use pip to install it

```
pip install streamlit
```

> To run the app

```
streamlit run beyond_app.py --global.dataFrameSerialization="legacy"
```

> ELK shoud be already installed & Data should be already indexed on an existing Index
---

### Interface & Examples
---

![alt tag](./img/q1.png)
---

![alt tag](./img/q2.png)
---

![alt tag](./img/q3.png)
---

![alt tag](./img/q4.png)
