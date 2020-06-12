# Notícias

## Inicializando as partes

Abra um terminal na pasta onde descompactou sua instalação do spark

* Mestre
	`./sbin/start-master.sh -i 0.0.0.0 -p 7077 --webui-port 8080`  
	Abra [http://0.0.0.0:8080](http://0.0.0.0:8080) e veja a página de relatório do mestre

* Worker
	`./sbin/start-slave.sh spark://0.0.0.0:7077 --webui-port 8081`  
	Abra [http://0.0.0.0:8081](http://0.0.0.0:8081) e veja a página de relatório do worker  


* Terminal
	`./bin/spark-shell --master spark://0.0.0.0:7077`  
	Vamos trabalhar no shell a patir de agora

## Interagindo com os dados

No shell, carregue o dataframe:

```scala
scala> val caminhoParaODataset = ...
scala> val noticias = spark.read.json(caminhoParaODataset)
noticias: org.apache.spark.sql.DataFrame = [_corrupt_record: string, categories: string ... 11 more fields]
```

Vamos visualizar o schema:
```scala
scala> noticias.printSchema
root
 |-- categories: string (nullable = true)
 |-- content: string (nullable = true)
 |-- contents: string (nullable = true)
 |-- description: string (nullable = true)
 |-- link: string (nullable = true)
 |-- publishedAt: string (nullable = true)
 |-- title: string (nullable = true)
 |-- updatedAt: string (nullable = true)
 |-- uri: string (nullable = true)
 |-- domain: string (nullable = true)
 |-- data: date (nullable = true)
 |-- hora: integer (nullable = true)
```

Aqui, apenas os campos *updatedAt, description, uri, contents, link, categories, content, title, publishedAt* estão contidos nos arquivos, Os demais são partições.

Vamos pegar uma amostra das primeiras 20 linhas do dataset:

```scala
scala> noticias.show(20)
+----------+--------------------+--------+--------------------+----+-----------+--------------------+---------+--------------------+-------------+----------+----+
|categories|             content|contents|         description|link|publishedAt|               title|updatedAt|                 uri|       domain|      data|hora|
+----------+--------------------+--------+--------------------+----+-----------+--------------------+---------+--------------------+-------------+----------+----+
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Problemas polític...|         |https://g1.globo....|pox.globo.com|2020-04-17|  18|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Wuhan tem festa d...|         |https://g1.globo....|pox.globo.com|2020-04-08|   7|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |1º de maio: manif...|         |https://g1.globo....|pox.globo.com|2020-05-01|  11|
|      [G1]|<!DOCTYPE HTML>
<...|      []|  Está por dentro...|  []|           |QUIZ de notícias ...|         |https://g1.globo....|pox.globo.com|2020-05-29|  16|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Diretor-geral da ...|         |https://g1.globo....|pox.globo.com|2020-05-14|  13|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Imprensa internac...|         |https://g1.globo....|pox.globo.com|2020-03-30|  13|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Kim Jong-un mando...|         |https://g1.globo....|pox.globo.com|2020-04-27|  19|
|      [G1]|<!DOCTYPE HTML>
<...|      []|  Nos 4 primeiros...|  []|           |Número de MEIs no...|         |https://g1.globo....|pox.globo.com|2020-04-27|  15|
|      [G1]|<!DOCTYPE HTML>
<...|      []|  Analistas esper...|  []|           |Bolsa de Xangai t...|         |https://g1.globo....|pox.globo.com|2020-04-10|  11|
|      [G1]|<!DOCTYPE HTML>
<...|      []|  Saída de Robert...|  []|           |Pressão dos EUA c...|         |https://g1.globo....|pox.globo.com|2020-05-14|  16|
|      [G1]|<!DOCTYPE HTML>
<...|      []|  Índice caiu 0,4...|  []|           |Preços ao consumi...|         |https://g1.globo....|pox.globo.com|2020-04-10|  11|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Arábia Saudita an...|         |https://g1.globo....|pox.globo.com|2020-05-11|  13|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Adolescente de 13...|         |https://g1.globo....|pox.globo.com|2020-04-01|   9|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Petrobras aumenta...|         |https://g1.globo....|pox.globo.com|2020-05-13|  13|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Apoio a Mandetta ...|         |https://g1.globo....|pox.globo.com|2020-04-08|  12|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Recuperado, Johns...|         |https://g1.globo....|pox.globo.com|2020-04-27|   8|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Ministro da Saúde...|         |https://g1.globo....|pox.globo.com|2020-05-14|   8|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |A silenciosa epid...|         |https://g1.globo....|pox.globo.com|2020-03-23|  16|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |'O presidente rev...|         |https://g1.globo....|pox.globo.com|2020-05-06|  17|
|      [G1]|<!DOCTYPE HTML>
<...|      []|   <img src="http...|  []|           |Como a pandemia a...|         |https://g1.globo....|pox.globo.com|2020-05-13|   5|
+----------+--------------------+--------+--------------------+----+-----------+--------------------+---------+--------------------+-------------+----------+----+
only showing top 20 rows
```


Vamos pegar uma amostra dos títulos:

```scala
scala> noticias.select("title").show(5)
+--------------------+
|               title|
+--------------------+
|Problemas polític...|
|Wuhan tem festa d...|
|1º de maio: manif...|
|QUIZ de notícias ...|
|Diretor-geral da ...|
+--------------------+
only showing top 5 rows
```

Vamos criar um novo dataframe com os diferentes domínios:

```scala
scala> val dominios = noticias.select("domain").distinct
dominios: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [domain: string]

scala> dominios.show(50)
+--------------------+                                                          
|              domain|
+--------------------+
|   brasil.elpais.com|
|feeds.folha.uol.c...|
|       pox.globo.com|
|  esporte.uol.com.br|
|      rss.uol.com.br|
| rss.home.uol.com.br|
+--------------------+
```

Vamos contar os registros por domínio:

```
scala> noticias.groupBy("domain").count().show(20)
+--------------------+-----+                                                    
|              domain|count|
+--------------------+-----+
|   brasil.elpais.com|   33|
|feeds.folha.uol.c...| 3478|
|       pox.globo.com| 2821|
|  esporte.uol.com.br| 7506|
|      rss.uol.com.br| 2258|
| rss.home.uol.com.br| 2054|
+--------------------+-----+
```