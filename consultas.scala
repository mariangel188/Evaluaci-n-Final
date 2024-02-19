import com.github.tototoshi.csv.*
import org.nspl._
import org.nspl.awtrenderer._
import scala.collection.mutable.ListBuffer
import org.nspl.data.HistogramData
import java.io.File
import scala.io.Source
import org.nspl._
import org.nspl.saddle._
import org.saddle._
import org.saddle.order._
import org.saddle.csv._
import org.saddle.scalar.ScalarTagDouble
import org.saddle.index.InnerJoin

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object consultas {
  @main
  def consultas() =

    val path2DataFilePartidos: String = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsPartidosYGoles.csv"
    val path2DataFile2Alineaciones: String = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsAlineacionesXTorneo.csv"

    val readerPartidos = CSVReader.open(new File(path2DataFilePartidos))
    val readerAlineaciones = CSVReader.open(new File(path2DataFile2Alineaciones))

    val contentFilePartidos: List[Map[String,String]] = readerPartidos.allWithHeaders()
    val contentFileAlineaciones: List[Map[String, String]] = readerAlineaciones.allWithHeaders()

    readerPartidos.close()
    readerAlineaciones.close()


    // 1.Calcula la capacidad minima, maxima y el promedio
    val capacidadS: List[Int] = contentFilePartidos
      .flatMap(_.get("stadiums_stadium_capacity")
        .filter(_.forall(_.isDigit)).map(_.toInt))

    val min = capacidadS.min
    val max = capacidadS.max
    val promedio = capacidadS.sum / capacidadS.length

    println(s"Su capacidad minima es: $min")
    println(s"Su capacidad maxima es: $max")
    println(s"El promedio de su capacidad es: $promedio")


  // 2. Calcular el periodo en el que se marcaron goles en un año en especifico

    val gol = contentFilePartidos
      .filter(_("tournaments_year") == "2014")

    val minutos = gol.flatMap(_("goals_match_period").split(","))
    val periodo = minutos
      .groupBy(identity)
      .maxBy(_._2.length)._1

    println(s"El periodo en el que se marcaron goles en 2014 es: $periodo")

  // 3. Calcular la cantidad de veces que un estadio fue utilizado en el mundial de un año especifico (2014)

    val veces = contentFilePartidos
      .filter(_("tournaments_year") == "2014")
      .groupBy(_("stadiums_stadium_name"))
      .mapValues(_.length)

    veces.foreach { case (estadio,cantidad) =>
      println(s"En el año 2014, el estadio $estadio fue utilizado  $cantidad veces.") }

  // 4. Calcular el numero de goles marcados en un torneo en especifico

    val total = contentFilePartidos
      .filter(_("tournaments_tournament_name") == "1954 FIFA Men's World Cup")
      .map(_("matches_home_team_score").toInt).sum

    println(s"El numero total de goles marcado en el torneo son: $total")

  // 5. Calcular el torneo con mas goles en la historia

    val totalG = contentFilePartidos
      .groupBy(_("tournaments_tournament_name"))
      .mapValues(_.map(_("matches_home_team_score")).map(_.toInt).sum)
      .maxBy(_._2)._1
    println(s"El torneo de la copa del mundo con mas goles marcados es: $totalG")
}
