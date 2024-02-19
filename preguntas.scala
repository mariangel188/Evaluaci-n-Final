import doobie.{ConnectionIO, *}
import doobie.implicits.*
import org.jfree.chart.axis.CategoryLabelPositions
import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import java.io.File
import cats.effect.IO
import doobie.util.transactor.Transactor
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartUtils, JFreeChart}

case class torneos( id:String, ganador: String)
case class estadios(nombre: String, capacidad:Int)
case class players(player_id:String ,given_name: String, family_name: String)
case class squads(numCamiseta: Int,team_id: String,player_id:String)
case class goles(player_id: String, own_goal: Int)
case class Match(tournamentName: String, numMatches: Int)
case class TournamentPlayers(tournament_id: String, players_count: Long)


object preguntas {

  def main(args: Array[String]): Unit = {

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/mundialesbd",
      user = "root",
      password = "Mariangel19.",
      logHandler = None)

    //graficaPartidos(partidos().transact(xa).unsafeRunSync()) si
    //graficaTorneos(torneo().transact(xa).unsafeRunSync()) si
    //graficaEstadio(estadioE().transact(xa).unsafeRunSync()) si
    //grafica(jugadores().transact(xa).unsafeRunSync())
    barras(playerst().transact(xa).unsafeRunSync())


    //1. Cuantos
    def partidos(): ConnectionIO[List[estadios]] = {
      sql"""
              SELECT DISTINCT name as 'Nombre de los estadios' , capacity
              FROM stadiums s
              INNER JOIN matches m ON s.stadium_id = m.stadium_id
              INNER JOIN tournaments t ON m.tournament_id= t.tournament_id
              WHERE t.tournament_id = 'WC-2022';
      """
        .query[estadios]
        .to[List]
    }

    def graficaPartidos(partidos: List[estadios]): Unit = {
      val dataset = new DefaultCategoryDataset()
      partidos.foreach { estadio =>
        dataset.addValue(estadio.capacidad, "Capacidad", estadio.nombre)
      }

      val chart: JFreeChart = ChartFactory.createBarChart(
        "Cantidad de veces que se uso un estadio ", // Título del gráfico
        "Estadios", // Etiqueta del eje X
        "Capacidad", // Etiqueta del eje Y
        dataset
      )

      val plot = chart.getCategoryPlot
      val domainAxis = plot.getDomainAxis
      domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica11.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }

    //2.  torneo con la mayor cantidad de partidos:
    def torneo(): ConnectionIO[List[Match]] = {
      sql"""
          SELECT t.tournament_name AS 'Nombre del torneo', COUNT(m.match_id) AS 'Cantidad de partidos'
          FROM matches m
          INNER JOIN tournaments t ON m.tournament_id = t.tournament_id
          GROUP BY t.tournament_name
          ORDER BY  t.tournament_name DESC;
        """
        .query[Match]
        .to[List]
    }

    def graficaTorneos(torneos: List[Match]): Unit = {
      val dataset = new DefaultCategoryDataset()

      // Agregar los datos al dataset
      torneos.foreach { torneo =>
        dataset.addValue(torneo.numMatches, "Cantidad de partidos", torneo.tournamentName)
      }

      // Crear la gráfica de barras
      val chart: JFreeChart = ChartFactory.createBarChart(
        "Cantidad de Partidos por Torneo", // Título del gráfico
        "Torneo", // Etiqueta del eje X
        "Cantidad de Partidos", // Etiqueta del eje Y
        dataset
      )
      val plot = chart.getCategoryPlot
      val domainAxis = plot.getDomainAxis
      domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\grafica12.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }

    //3.
    def estadioE(): ConnectionIO[List[estadios]] =
      sql"""
        SELECT s.name, s.capacity
        FROM stadiums s
        WHERE s.city_name = 'Buenos Aires'
      """
        .query[estadios]
        .to[List]

    def graficaEstadio(data: List[estadios]): Unit = {
      val dataset = new DefaultCategoryDataset()
      data.foreach { estadio =>
        dataset.addValue(estadio.capacidad, "Capacidad del Estadio", estadio.nombre)
      }
      val chart: JFreeChart = ChartFactory.createBarChart(
        "Capacidad de los estadios de la ciudad de Buenos Aires",
        "Nombre del estadio",
        "Capacidad",
        dataset,
        PlotOrientation.VERTICAL,
        true,
        true,
        false
      )
      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\graficas13.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }

    //4. Los jugadores que forman parte de un equipo en un torneo
    def jugadores(): ConnectionIO[List[Match]] = {
      sql"""
            SELECT t.tournament_name AS 'Nombre del torneo', COUNT(g.goal_id) AS 'Cantidad de autogoles'
            FROM goals g
            INNER JOIN tournaments t ON g.tournament_id = t.tournament_id
            WHERE g.own_goal = 1
            GROUP BY t.tournament_name
            ORDER BY t.tournament_name DESC
      """
        .query[Match]
        .to[List]
    }

    def grafica(matches: List[Match]): Unit = {
      val dataset = new DefaultCategoryDataset()

      matches.foreach { matchData =>
        dataset.addValue(matchData.numMatches, "Cantidad de autogoles", matchData.tournamentName)
      }

      val chart: JFreeChart = ChartFactory.createBarChart(
        "Cantidad de Autogoles por Torneo", // Título del gráfico
        "Torneo", // Etiqueta del eje X
        "Cantidad de Autogoles", // Etiqueta del eje Y
        dataset
      )
      val plot = chart.getCategoryPlot
      val domainAxis = plot.getDomainAxis
      domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\graficas14.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }


    //5. Nombres e id de un j
    def playerst(): ConnectionIO[List[TournamentPlayers]] = {
      sql"""
          SELECT m.tournament_id, COUNT(DISTINCT s.player_id) AS players_count
          FROM matches m
          INNER JOIN squads s ON m.tournament_id = s.tournament_id
          GROUP BY m.tournament_id;
        """
        .query[TournamentPlayers]
        .to[List]
    }


    def barras(tournamentPlayers: List[TournamentPlayers]): Unit = {
      val dataset = new DefaultCategoryDataset()
      tournamentPlayers.foreach { case TournamentPlayers(tournamentId, playerCount) =>
        dataset.addValue(playerCount, "Jugadores", tournamentId)
      }
      val chart: JFreeChart = ChartFactory.createBarChart(
        "Cantidad de Jugadores por Torneo",
        "Torneo",
        "Cantidad de Jugadores",
        dataset,
        org.jfree.chart.plot.PlotOrientation.VERTICAL,
        true,
        true,
        false
      )
      val plot = chart.getCategoryPlot
      val domainAxis = plot.getDomainAxis
      domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

      val outputPath = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\proyecto2B\\graficas15.png"
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 800, 600)
    }
  }
}


