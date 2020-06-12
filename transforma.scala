import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.apache.spark.sql.functions.udf


/// Prepara UDFs
def parseHtmlFunc(htmlString: String): String = {
	Option(htmlString) match {
		case Some(source) =>
			val htmlDocument: Document = Jsoup.parse(source)
			htmlDocument.text
		case None =>
			""
	}
}

val parseHTML = udf(parseHtmlFunc _)


def parseListFunc(categoriasStr: String): Seq[String] = {
	Option(categoriasStr) match {
		case Some(categorias) =>
			categorias
				.drop(1)
				.dropRight(1)
				.split(",")
				.map(_.trim)
		case None =>
			Seq()
	}
}

val parseList = udf(parseListFunc _)

/// Efetiva transformações
val caminhoParaODataset = "data30k"
val noticias = spark.read.json(caminhoParaODataset)
val noticiasComCategoria = noticias.withColumn("categorias", parseList(col("categories")))
val noticiasComCategoriaParseado = noticiasComCategoria.withColumn("descricao", parseHTML(col("description"))).withColumn("conteudo", parseHTML(col("content")))
val noticias = noticiasComCategoriaParseado.drop("updatedAt")
val noticiasSemPublishedAt = noticias.drop("publishedAt")
val noticiasPreFinal = noticiasSemPublishedAt.withColumnRenamed("title", "titulo").withColumnRenamed("domain", "dominio")
val noticiasFinal = noticiasPreFinal.select("data", "hora", "dominio", "titulo", "descricao", "conteudo", "categorias")

// Salva os resultados
noticiasFinal.write.format("json").save("noticias-final")