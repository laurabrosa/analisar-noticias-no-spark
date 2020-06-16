# Notícias

Baixe o pacote do spark em [spark.apache.org](spark.apache.org).

Descompacte o pacote baixado, e entre na pasta gerada seguindo o comando a seguir (os nomes dependem da versão do spark que você escolheu):

```shell
tar -xzvf <pacote baixado>.tgz
cd <pasta gerada>
```

Baixe o pacote `JSoup` no [neste link](https://repo1.maven.org/maven2/org/jsoup/jsoup/1.13.1/jsoup-1.13.1.jar) e copie-o para o diretório `jars` dentro da pasta de instalação do spark (pasta gerada).

Por exemplo, pode fazer:

```shell
cd jars
wget -c https://repo1.maven.org/maven2/org/jsoup/jsoup/1.13.1/jsoup-1.13.1.jar
cd ..
```

Agora vamos iniciar os pacotes spark:


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
 |-- _corrupt_record: string (nullable = true)
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

O campo *categories* é uma string, apesar de ter formato de lista. Vamos transformar essa coluna em um campo do tipo lista usando a seguinte transformação de mapeamento no dataframe.

Primeiro, execute o conteúdo de `udfs.scala` para disponibilizar as UDFs que vamos usar (basta copiar, colar e executar).

Em seguida, vamos fazer a tranformaçao da coluna:

```scala
scala> val noticiasComCategoria = noticias.withColumn("categorias", parseList(col("categories")))
noticiasComCategoria: org.apache.spark.sql.DataFrame = [_corrupt_record: string, categories: string ... 12 more fields]
```

Analisando o novo schema do dataset vemos que o campo "categorias" foi criado, e que é um array de strings:

```scala
scala> noticiasComCategoria.printSchema
root
 |-- _corrupt_record: string (nullable = true)
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
 |-- categorias: array (nullable = true)
 |    |-- element: string (containsNull = true)
 ```

 Os campos *description* e *content* são códigos HTML, códigos que renderizam numa pequena introdução ao texto e ao conteúdo em si, vamos remover as tags HTML e transformar em texto legível.

 ```scala
 scala> val noticiasComCategoriaParseado = noticiasComCategoria.withColumn("descricao", parseHTML(col("description"))).withColumn("conteudo", parseHTML(col("content")))
noticiasComCategoriaParseado: org.apache.spark.sql.DataFrame = [_corrupt_record: string, categories: string ... 14 more fields]
```

E vemos que o schema agora tem dois novos campos: conteudo e descricao, que ainda são strings:

```scala
scala> noticiasComCategoriaParseado.printSchema
root
 |-- _corrupt_record: string (nullable = true)
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
 |-- categorias: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- descricao: string (nullable = true)
 |-- conteudo: string (nullable = true)
 ```

Apesar de terem o mesmo tipo, agora a coluna *descricao* não contém mais tags HTML, e sim um texto devidamente parseado, vamos comparar *descricao* e *description* em cada linha numa amostra:

```scala
scala> noticiasComCategoriaParseado.select("descricao", "description").show()
+--------------------+--------------------+
|           descricao|         description|
+--------------------+--------------------+
|Está por dentro d...|  Está por dentro...|
|Está por dentro d...|  Está por dentro...|
|1º dia teve Madon...|   <img src="http...|
|'Daniel Azulay é ...|   <img src="http...|
|Caixa diz que usu...|   <img src="http...|
|4 de outubro - Ho...|   <img src="http...|
|Intervenções drás...|   <img src="http...|
|Países como Bolív...|   <img src="http...|
|Autoridades perma...|   <img src="http...|
|5 de fevereiro - ...|   <img src="http...|
|Veja fotos da com...|   <img src="http...|
|O Dia do Trabalho...|   <img src="http...|
|Está por dentro d...|  Está por dentro...|
|Diplomata brasile...|   <img src="http...|
|Veículos de Franç...|   <img src="http...|
|Correspondência o...|   <img src="http...|
|Nas últimas seman...|   <img src="http...|
|Nos 4 primeiros m...|  Nos 4 primeiros...|
|Analistas esperam...|  Analistas esper...|
|Saída de Roberto ...|  Saída de Robert...|
+--------------------+--------------------+
only showing top 20 rows
```

O mesmo pode ser visto para a relação entre *conteudo* e *content*:

```scala
scala> noticiasComCategoriaParseado.select("conteudo", "content").show()
+--------------------+--------------------+
|            conteudo|             content|
+--------------------+--------------------+
|QUIZ de notícias ...|<!DOCTYPE HTML>
<...|
|QUIZ de notícias ...|<!DOCTYPE HTML>
<...|
|Enem 2019: veja i...|<!DOCTYPE HTML>
<...|
|Daniel Azulay, ví...|<!DOCTYPE HTML>
<...|
|Auxílio emergenci...|<!DOCTYPE HTML>
<...|
|Imagens da semana...|<!DOCTYPE HTML>
<...|
|Coronavírus: inér...|<!DOCTYPE HTML>
<...|
|Problemas polític...|<!DOCTYPE HTML>
<...|
|Wuhan tem festa d...|<!DOCTYPE HTML>
<...|
|Imagens da semana...|<!DOCTYPE HTML>
<...|
|Ano Novo 2020; FO...|<!DOCTYPE HTML>
<...|
|1º de maio: manif...|<!DOCTYPE HTML>
<...|
|QUIZ de notícias ...|<!DOCTYPE HTML>
<...|
|Diretor-geral da ...|<!DOCTYPE HTML>
<...|
|Imprensa internac...|<!DOCTYPE HTML>
<...|
|Kim Jong-un mando...|<!DOCTYPE HTML>
<...|
|Coronavírus: o av...|<!DOCTYPE HTML>
<...|
|Número de MEIs no...|<!DOCTYPE HTML>
<...|
|Bolsa de Xangai t...|<!DOCTYPE HTML>
<...|
|Pressão dos EUA c...|<!DOCTYPE HTML>
<...|
+--------------------+--------------------+
only showing top 20 rows
```

Fica mais claro se virmos ambos separados (a quebra de linha atrapalha)

```scala
scala> noticiasComCategoriaParseado.select("conteudo").show(5)
+--------------------+
|            conteudo|
+--------------------+
|QUIZ de notícias ...|
|QUIZ de notícias ...|
|Enem 2019: veja i...|
|Daniel Azulay, ví...|
|Auxílio emergenci...|
+--------------------+
only showing top 5 rows
```

```scala
scala> noticiasComCategoriaParseado.select("content").show(5)
+--------------------+
|             content|
+--------------------+
|<!DOCTYPE HTML>
<...|
|<!DOCTYPE HTML>
<...|
|<!DOCTYPE HTML>
<...|
|<!DOCTYPE HTML>
<...|
|<!DOCTYPE HTML>
<...|
+--------------------+
only showing top 5 rows
```

Mais um ponto, vamos ver os valores distintos da coluna *updatedAt*:

```scala
scala> noticiasComCategoriaParseado.select("updatedAt").distinct().show
+---------+                                                                     
|updatedAt|
+---------+
|     null|
|         |
+---------+
```

Essa coluna é inútil, já que não tem informação, vamos eliminá-la. Vamos aproveitar para *redeclarar* o dataset notícias, o original não nos interessa mais. Tenha em mente que falamos de redeclaração por que não é possível alterar o valor da variável que aponta para o dataset. Para que fique mais claro, vamos tentar alterar o valor da variável:

```scala
scala> noticias = noticiasComCategoriaParseado.drop("updatedAt")
<console>:30: error: reassignment to val
       noticias = noticiasComCategoriaParseado.drop("updatedAt")
```

Recebemos um erro, se veriricarmos o schema de noticias, veremos que a coluna updatedAt continua lá:

```scala
scala> noticias.printSchema
root
 |-- _corrupt_record: string (nullable = true)
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


Mas se fizermos uma redeclaração, teremos um resultado diferente:

```
scala> val noticias = noticiasComCategoriaParseado.drop("updatedAt")
noticias: org.apache.spark.sql.DataFrame = [_corrupt_record: string, categories: string ... 13 more fields]
```

E o schema foi alterado:

```scala
scala> noticias.printSchema
root
 |-- _corrupt_record: string (nullable = true)
 |-- categories: string (nullable = true)
 |-- content: string (nullable = true)
 |-- contents: string (nullable = true)
 |-- description: string (nullable = true)
 |-- link: string (nullable = true)
 |-- publishedAt: string (nullable = true)
 |-- title: string (nullable = true)
 |-- uri: string (nullable = true)
 |-- domain: string (nullable = true)
 |-- data: date (nullable = true)
 |-- hora: integer (nullable = true)
 |-- categorias: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- descricao: string (nullable = true)
 |-- conteudo: string (nullable = true)
 ```

 Outra coluna com valores sempre nulos é *publishedAt*, como podemos ver a seguir:

 ```
 scala> noticias.select("publishedAt").distinct.show
+-----------+                                                                   
|publishedAt|
+-----------+
|       null|
|           |
+-----------+
```

Vamos removê-la.

```
scala> val noticiasSemPublishedAt = noticias.drop("publishedAt")
noticiasSemPublishedAt: org.apache.spark.sql.DataFrame = [_corrupt_record: string, categories: string ... 12 more fields]
```

Vamos criar um conjunto de dados limpo, com as devidas transformações e apenas as colunas que nos interessam para uma próxima etapa. Primeiro, vamos renomear a colluna *title* para *titulo*, e a coluna *domain* para *dominio* mantendo o novo padrão do dataset limpo (com colunas em português).

```scala
scala> val noticiasPreFinal = noticiasSemPublishedAt.withColumnRenamed("title", "titulo").withColumnRenamed("domain", "dominio")
preFinal: org.apache.spark.sql.DataFrame = [_corrupt_record: string, categories: string ... 12 more fields]

scala> noticiasPreFinal.printSchema
root
 |-- _corrupt_record: string (nullable = true)
 |-- categories: string (nullable = true)
 |-- content: string (nullable = true)
 |-- contents: string (nullable = true)
 |-- description: string (nullable = true)
 |-- link: string (nullable = true)
 |-- titulo: string (nullable = true)
 |-- uri: string (nullable = true)
 |-- dominio: string (nullable = true)
 |-- data: date (nullable = true)
 |-- hora: integer (nullable = true)
 |-- categorias: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- descricao: string (nullable = true)
 |-- conteudo: string (nullable = true)

```

Perceba que agora não temos mais *title* e *domain*, mas temos *titulo* e *dominio*:

Vamos separar apenas as colunas que nos interessam:

```scala
scala> val noticiasFinal = noticiasPreFinal.select("data", "hora", "dominio", "titulo", "descricao", "conteudo", "categorias")
noticiasFinal: org.apache.spark.sql.DataFrame = [data: date, hora: int ... 5 more fields]
```

Agora vamos salvar esse resultado, vamos salva-lo particionando

```scala
noticiasFinal.write.partitionBy("data", "hora", "dominio").format("json").save("noticias-final")
```

## Exercícios
Execute os seguintes exercícios usando o dataset *noticias-final*, gerado no exemplo.

1. Você pode concatenar as colunas como no exemplo abaixo.

```scala
val noticiasFinal = spark.read.json("noticias-final")
scala> val concatenado = noticiasFinal.select(concat(col("titulo"), lit("\n"), col("descricao"), lit("\n"), col("conteudo")).as("corpora"))
scala> concatenado.show()
+--------------------+
|             corpora|
+--------------------+
|QUIZ de notícias ...|
|QUIZ de notícias ...|
|Enem 2019: veja i...|
|Daniel Azulay, ví...|
|Auxílio emergenci...|
|Imagens da semana...|
|Coronavírus: inér...|
|Problemas polític...|
|Wuhan tem festa d...|
|Imagens da semana...|
|Ano Novo 2020; FO...|
|1º de maio: manif...|
|QUIZ de notícias ...|
|Diretor-geral da ...|
|Imprensa internac...|
|Kim Jong-un mando...|
|Coronavírus: o av...|
|Número de MEIs no...|
|Bolsa de Xangai t...|
|Pressão dos EUA c...|
+--------------------+
only showing top 20 rows
```

Crie um dataset chamado *corpora* com a coluna corpora (como descrita acima), as colunas *dominio*, *categorias* e *data*.

Salve este dataframe particionado por *data* e *dominio*.

2. Gere um dataset que conte o número de registros por data
3. Gere um outro dataset que contém o número de registros por domínio


# Resposta dos Exercícios

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