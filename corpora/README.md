# Exercícios

1. Crie um dataset chamado corpora com a coluna corpora (como descrita acima), as colunas dominio, categorias e data.

```scala
scala> val corpora = noticiasFinal.select("dominio", "categorias", "data")
corpora: org.apache.spark.sql.DataFrame = [dominio: string, categorias: array<string> ... 1 more field]
```
```scala
scala> corpora.show()
+------------------+----------+----------+
|           dominio|categorias|      data|
+------------------+----------+----------+
|     pox.globo.com|      [G1]|2020-03-24|
|     pox.globo.com|      [G1]|2020-04-27|
|     pox.globo.com|      [G1]|2020-04-27|
|     pox.globo.com|      [G1]|2020-02-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
|esporte.uol.com.br|        []|2020-03-05|
+------------------+----------+----------+
only showing top 20 rows
```
Salve este dataframe particionado por data e dominio.

```scala
scala> corpora.write.partitionBy("data", "dominio").format("json").save("corpora")
```
2. Gere um dataset que conte o número de registros por data

```scala
scala> corpora.groupBy("data").count().show(20)

+----------+-----+
|      data|count|
+----------+-----+
|2020-01-21|  110|
|2019-09-22|  139|
|2019-11-01|  134|
|2019-11-18|  156|
|2019-11-21|  138|
|2020-04-30|  112|
|2018-08-08|    1|
|2019-05-27|    1|
|2020-03-13|  111|
|2020-03-07|   94|
|2020-02-04|  108|
|2020-02-15|  108|
|2020-05-23|  117|
|2018-05-26|    1|
|2019-10-05|  135|
|2020-02-12|  114|
|2018-04-18|    2|
|2019-07-08|  122|
|2019-10-24|  154|
|2019-05-14|    2|
+----------+-----+
only showing top 20 rows
```
3. Gere um outro dataset que contém o número de registros por domínio

```scala
corpora.groupBy("dominio").count().show(20)

+--------------------+-----+
|             dominio|count|
+--------------------+-----+
|   brasil.elpais.com|   60|
|feeds.folha.uol.c...| 5678|
|       pox.globo.com| 4618|
|  esporte.uol.com.br|12543|
|      rss.uol.com.br| 3676|
| rss.home.uol.com.br| 3423|
+--------------------+-----+
```