import breeze.plot.*
import java.io.File
import com.github.tototoshi.csv.CSVReader
import org.knowm.xchart.{BitmapEncoder, CategoryChartBuilder, PieChart, PieChartBuilder, QuickChart, XYChart}
import scala.jdk.CollectionConverters.*
import scala.util.Try
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
import org.knowm.xchart.{CategoryChart, CategoryChartBuilder}
import javax.swing.{JFrame, WindowConstants}
import org.jfree.chart.ChartFactory
import org.jfree.chart.JFreeChart
import org.jfree.chart.axis.{CategoryAxis, CategoryLabelPosition, CategoryLabelPositions}
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.jfree.chart.ChartUtils

object graficas {
  @main
  def main(): Unit =

    val path2DataFilePartidos: String = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsPartidosYGoles.csv"
    val path2DataFile2Alineaciones: String = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsAlineacionesXTorneo.csv"

    val readerPartidos = CSVReader.open(new File(path2DataFilePartidos))
    val readerAlineaciones = CSVReader.open(new File(path2DataFile2Alineaciones))

    val contentFilePartidos: List[Map[String, String]] = readerPartidos.allWithHeaders()
    val contentFileAlineaciones: List[Map[String, String]] = readerAlineaciones.allWithHeaders()

    readerPartidos.close()
    readerAlineaciones.close()

    //density(contentFileAlineaciones)
    //capacidad(contentFilePartidos)
    //periodo(contentFilePartidos)
    //estadios(contentFilePartidos)
    //torneo(contentFilePartidos)
    //partidosAway(contentFilePartidos)
    //compararMundiales(contentFilePartidos)
    //partidosHome(contentFilePartidos)
    //golesLV(contentFilePartidos)
    //jugadores(contentFileAlineaciones)


    // 1.la densidad de la de camiseta de los defensores por su posición en el equipo.
    def density(data: List[Map[String, String]]): Unit = {
      val camiseta: List[Double] = data
        .filter(row => row("squads_position_name") == "defender" && row("squads_shirt_number") != "1")
        .map(row => row("squads_shirt_number").toDouble)

      val f = Figure()
      val p = f.subplot(0)

      p += breeze.plot.hist(camiseta, bins = 20)

      p.xlabel = "Camiseta"
      p.ylabel = "Densidad"
      p.title = "Camisetas por posición"

      f.saveas("C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\graficas.png")
    }

  // 2.la capacidad maxima de los estadios
    def capacidad(data: List[Map[String, String]]): Unit = {
      val capacidades = data.flatMap(row => row.get("stadiums_stadium_capacity").flatMap(_.toIntOption))
      val capacidadM = capacidades.max

      val f = Figure()
      val p = f.subplot(0)

      p += breeze.plot.hist(capacidades, bins = 20)

      p.xlabel = "Capacidad de los estadios"
      p.ylabel = "Frecuencia"
      p.title = "Capacidad máxima de los estadios"

      f.saveas("C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica2.png")
    }

    //3. El periodo en el que se marcaron goles
    def periodo(data: List[Map[String, String]]): Unit = {
      val conteo = data.groupBy(_("goals_match_period")).mapValues(_.length).toMap
      val dataset = new DefaultCategoryDataset()
      val periodos = Seq("first half", "second half")
      periodos.foreach { periodo =>
        dataset.addValue(conteo.getOrElse(periodo, 0), "Goles", periodo)
      }
      val chart: JFreeChart = ChartFactory.createBarChart(
        "Goles por periodo", // titulo
        "Periodo del partido", // eje x
        "Cantidad de goles", // eje y
        dataset, // dataset
        PlotOrientation.VERTICAL,
        false,
        true,
        false
      )
      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica3.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }

    // 4. Cantidad de veces que un estadio fue utilizado en el mundial
    def estadios(data: List[Map[String, String]]): Unit = {
      val year = "2022"
      val conteoE = data
        .filter(row => row("tournaments_year") == year)
        .groupBy(row => row("stadiums_stadium_name"))
        .mapValues(_.length)
        .toMap
      val dataset = new DefaultCategoryDataset()
      conteoE.foreach { case (estadio, conteo) =>
        dataset.addValue(conteo, "Cantidad de veces utilizado", estadio)
      }
      val chart: JFreeChart = ChartFactory.createBarChart(
        s"Estadios utilizados en el mundial $year", // titulo
        "Estadio", // eje x
        " Cantidad de veces", // eje y
        dataset, // dataset
        PlotOrientation.VERTICAL,
        false,
        true,
        false
      )
      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica4.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }

    // 5. El torneo con mas goles en la historia
    def torneo(data: List[Map[String, String]]): Unit = {
      val goles = data
        .groupBy(_("tournaments_tournament_name")).mapValues { partidosT =>
          partidosT.map { partido =>
            partido("matches_home_team_score").toInt + partido("matches_away_team_score").toInt
          }.sum
        }
      val dataset = new DefaultCategoryDataset()
      goles.foreach { case (torneo, totalGoles) =>
        dataset.addValue(totalGoles, "Goles", torneo)
      }
      val chart: JFreeChart = ChartFactory.createBarChart(
        s"Goles por torneo ", // titulo
        "Torneo", // eje x
        " Cantidad de goles", // eje y
        dataset // dataset
      )
      val plot = chart.getCategoryPlot
      val domainAxis = plot.getDomainAxis
      domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica5.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 1200, 800)
    }

    // 6. calcular la cantidad de veces que se a jugado en una region visitante
    def partidosAway(data: List[Map[String, String]]): Unit = {
      val demasRegiones = data.map(_("away_region_name")).distinct

      val partidoRegion = demasRegiones.map { region =>
        val partidosRegion = data.count(_("away_region_name") == region)
        (region, partidosRegion)
      }
      val chart: PieChart = new PieChartBuilder().width(800).height(600)
        .title("Cantidad de partidos en la región visitante").build()

      partidoRegion.foreach { case (region, cantidad) =>
        chart.addSeries(region, cantidad)
      }
      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica6.png"
      BitmapEncoder.saveBitmap(chart, outputPath, BitmapFormat.PNG)
    }

    // 7. Comparar la cantidad de mundiales femeninos y masculinos
    def compararMundiales(data: List[Map[String, String]]): Unit = {
      val partidosMasculinos = data
        .filter(row => row("tournaments_tournament_name").contains("FIFA Men's World Cup"))
        .map(row => row("tournaments_year"))
        .distinct
        .length

      val partidosFemeninos = data
        .filter(row => row("tournaments_tournament_name").contains("FIFA Women's World Cup"))
        .map(row => row("tournaments_year"))
        .distinct
        .length

      val chart: PieChart = new PieChartBuilder().width(800).height(600)
        .title("Cantidad de Mundiales").build()

      chart.addSeries("Masculinos", partidosMasculinos)
      chart.addSeries("Femeninos", partidosFemeninos)

      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica7.png"
      BitmapEncoder.saveBitmap(chart, outputPath, BitmapFormat.PNG)
    }

    // 8.calcular la cantidad de veces que se a jugado en una region local

    def partidosHome(data: List[Map[String, String]]): Unit = {
      val masRegiones = data.map(_("home_region_name")).distinct
      val partidoRegion = masRegiones.map { region =>
        val partidosRegion = data.count(_("home_region_name") == region)
        (region, partidosRegion)
      }
      val chart: PieChart = new PieChartBuilder().width(800).height(600).title("Cantidad de partidos en la region local").build()

      partidoRegion.foreach { case (region, cantidad) =>
        chart.addSeries(region, cantidad)
      }
      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica8.png"
      BitmapEncoder.saveBitmap(chart, outputPath, BitmapFormat.PNG)
    }

    // 9.Comparación de goles marcados visitante y local
    def golesLV(data: List[Map[String, String]]): Unit = {
      val equipo = "Brazil"
      val partidosLocal = data.filter(_("home_team_name") == equipo)
      val partidosVisitante = data.filter(_("away_team_name") == equipo)

      val golesLocal = partidosLocal.map(_("matches_home_team_score")).flatMap(s => Try(s.toInt).toOption).sum
      val golesVisitante = partidosVisitante.map(_("matches_away_team_score")).flatMap(s => Try(s.toInt).toOption).sum

      val dataset = new DefaultCategoryDataset()
      dataset.addValue(golesLocal, "Goles", "Local")
      dataset.addValue(golesVisitante, "Goles", "Visitante")

      val chart = ChartFactory.createBarChart(
        "Comparación de goles marcados visitante y local", // Título
        "Condición de juego", // eje x
        "Cantidad de goles", // eje y
        dataset, // Conjunto de datos
        org.jfree.chart.plot.PlotOrientation.VERTICAL,
        false,
        true,
        false
      )

      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica9.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }

    // 10. La cantidad de veces que un jugador estuvo en un torneo
    def jugadores(data: List[Map[String, String]]): Unit = {
      val nombres = List("Ariel Ortega", "Diego Maradona")
      val dataset = new DefaultCategoryDataset()
      nombres.foreach { nombreCompleto =>
        val nombreApellido = nombreCompleto.split(" ")
        val nombreJ = nombreApellido(0)
        val apellidoJ = nombreApellido(1)

        val torneosJ = data.filter { row =>
          row("players_given_name") == nombreJ && row("players_family_name") == apellidoJ
        }
        val torneos = torneosJ.map(_("squads_tournament_id")).distinct.size
        dataset.addValue(torneos, "Torneos", nombreCompleto)
      }
      val chart: JFreeChart = ChartFactory.createBarChart(
        "Cantidad de torneos que participó un jugador",
        "Jugador",
        "Cantidad de Torneos",
        dataset,
        org.jfree.chart.plot.PlotOrientation.VERTICAL,
        true,
        true,
        false
      )
      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica10.png"
      ChartUtils.saveChartAsPNG(new java.io.File(outputPath), chart, 800, 600)
    }

}

