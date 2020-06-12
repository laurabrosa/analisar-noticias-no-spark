import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.apache.spark.sql.functions.udf


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
