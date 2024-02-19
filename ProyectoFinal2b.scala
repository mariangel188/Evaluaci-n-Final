import com.github.tototoshi.csv.*
import doobie.util.transactor

import java.io.File
import java.io.{BufferedWriter, FileWriter} // import para escribir datos en un txt

import doobie._
import doobie.implicits._

import cats._
import cats.effect._
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.implicits._

object proyectoFinal2b {
  @main
  def ej() =

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost: 3306/mundialesbd",
      user = "root",
      password = "Mariangel19.",
      logHandler = None)

    val ruta = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsAlineacionesXTorneo.csv"
    val reader = CSVReader.open(new File(ruta))
    val contentFile: List[Map[String, String]] =
      reader.allWithHeaders()

    val ruta2 = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsPartidosYGoles.csv"
    val reader2 = CSVReader.open(new File(ruta2))
    val contentFile2: List[Map[String, String]] =
      reader2.allWithHeaders()

    def valoresI(valor: String) = {
      if (valor == "not available" || valor == "not applicable" || valor == "NA" || valor == "\\s") {
        0
      } else {
        valor.toDouble
      }
    }

    def caracteres(valor: String) = {

      val newValor = valor.replace("'", "\\'")
      newValor
    }

    def notNullDate(data: String) = {
      if (data.contains("not available") || data.isEmpty) {
        "1000-01-01"
      } else {
        data
      }
    }
//----------------------------------------------------------------------------------
    def escribirtxt(nombre: String, archivo: String): Unit =
      val ruta = "C:/Users/Usuario PC/OneDrive/Escritorio/scripts nuevo/"
      val rutaF = ruta + nombre

      val escritor = new BufferedWriter(new FileWriter(rutaF, true))
      try {
        escritor.write(archivo)
        escritor.newLine()
      } finally {
        escritor.close()
      }

    def generarDatosTablaCountryTXT(data: List[Map[String, String]]): Unit = {
      val nombreTxt = "country.txt"
      val formatoInsert = "INSERT INTO country(country_name,country_region) VALUES('%s','%s');"
      val valores = data
        .map(x => (
          x("stadiums_country_name").trim,
          x("away_region_name").trim
        ))
        .distinct
        .map(x => escribirtxt(nombreTxt, formatoInsert.format(x._1, x._2)))
    }

    def generarDatosTablaTeamsTXT(data: List[Map[String, String]]): Unit = {
      val nombreTxt = "teams.txt"
      val formatoInsert = "INSERT INTO teams(team_id, country_id, mens_team, womens_team) " +
        "VALUES ('%s', (SELECT country_id FROM country WHERE country_name = '%s'), %s, %s);"

      val valores = data
        .map(x => (
          caracteres(x("goals_team_id").trim),
          x("stadiums_country_name").trim,
          valoresI(x("away_mens_team")),
          valoresI(x("away_womens_team"))
        ))
        .distinct
        .map(x => escribirtxt(nombreTxt, formatoInsert.format(x._1, x._2, x._3, x._4)))
    }

    def generarDatosTablaTournamentsTXT(data: List[Map[String, String]]): Unit = {
      val nombreTxt = "tournaments.txt"
      val formatoInsert = "INSERT INTO tournaments(tournament_id, tournament_name," +
        "year,count_teams, tournament_winner) " +
        "VALUES('%s','%s', %d, %s, '%s');"
      val valores = data
        .map(x => (
          x("matches_tournament_id").trim,
          caracteres(x("tournaments_tournament_name")).trim,
          x("tournaments_year").toInt,
          x("tournaments_count_teams").trim,
          x("tournaments_winner").trim
        ))
        .distinct
        .map(x => escribirtxt(nombreTxt, formatoInsert.format(x._1, x._2, x._3, x._4, x._5)))
    }

    def generateDataStadiumTableTXT(data: List[Map[String, String]]): Unit = {
      val nombretxt = "stadiums.txt"
      val insertFormat = "INSERT INTO stadiums(stadium_id, name, city_name, capacity, country_id) " +
        "VALUES('%s', '%s', '%s', %d,(SELECT country_id FROM country WHERE country_name = '%s'LIMIT 1));"
      val valores = data
        .map(x => (
          caracteres(x("matches_stadium_id").trim),
          caracteres(x("stadiums_stadium_name").trim),
          caracteres(x("stadiums_city_name").trim),
          valoresI(x("stadiums_stadium_capacity")).toInt,
          x("stadiums_country_name")
        ))
        .distinct
        .map(x => escribirtxt(nombretxt, insertFormat.format(x._1, x._2, x._3, x._4, x._5)))
    }

    def generarDatosTablaPlayersTXT(data: List[Map[String, String]]): Unit = {
      val nombreTxt = "players.txt"
      val formatoInsert = "INSERT INTO players(player_id, family_name, given_name, birth_date," +
        " female, goal_keeper, defender, midfielder,forward) " +
        "VALUES('%s','%s', '%s', '%s', %d, %d, %d, %d, %d);"
      val valores = data
        .map(x => (
          x("squads_player_id").trim,
          caracteres(x("players_family_name")).trim,
          caracteres(x("players_given_name")).trim,
          notNullDate(x("players_birth_date")).trim,
          x("players_female").toInt,
          x("players_goal_keeper").toInt,
          x("players_defender").toInt,
          x("players_midfielder").toInt,
          x("players_forward").toInt
        ))
        .distinct
        .map(x => escribirtxt(nombreTxt, formatoInsert.format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)))
    }

    def generarDatosTablaMatchesTXT(data: List[Map[String, String]]): Unit = {
      val nombreTxt = "matches.txt"
      val formatoInsert = "INSERT INTO matches(match_id, match_date, match_time, " +
        "stage_name, home_team_score, away_team_score, extra_time, penalty_shootout," +
        "home_team_score_penalties, away_team_score_penalties, result, " +
        "stadium_id, tournament_id, away_team_id, home_team_id ) " +
        "VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');"
      val valores = data
        .map(x => (
          x("matches_match_id").trim,
          notNullDate(x("matches_match_date")).trim,
          x("matches_match_time").trim,
          x("matches_stage_name").trim,
          x("matches_home_team_score").toInt,
          x("matches_away_team_score").toInt,
          x("matches_extra_time").toInt,
          x("matches_penalty_shootout").toInt,
          x("matches_home_team_score_penalties").toInt,
          x("matches_away_team_score_penalties").toInt,
          caracteres(x("matches_result")).trim,
          caracteres(x("matches_stadium_id").trim),
          caracteres(x("matches_tournament_id")),
          caracteres(x("matches_away_team_id")),
          x("matches_home_team_id")
        ))
        .distinct
        .map(x => escribirtxt(nombreTxt, formatoInsert.format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15)))
    }

    def generarDatosTablaGoalsTXT(data: List[Map[String, String]]): Unit = {
      val nombretxt = "goals.txt"
      val insertFormat = "INSERT INTO goals(goal_id, minute_label, minute_regulation, " +
        "minute_stoppage, match_period, own_goal , penalty, match_id, player_id," +
        "team_id, tournament_id) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s' ,'%s',  '%s', '%s', '%s');"
      val values = data.filterNot(row => row("goals_goal_id") == "NA")
        .map(row =>
          (
            caracteres(row("goals_goal_id")).trim,
            caracteres(row("goals_minute_label")).trim,
            caracteres(row("goals_minute_regulation")),
            caracteres(row("goals_minute_stoppage")),
            caracteres(row("goals_match_period")),
            caracteres(row("goals_own_goal")),
            caracteres(row("goals_penalty")),
            caracteres(row("matches_match_id")).trim,
            caracteres(row("goals_player_id")).trim,
            caracteres(row("goals_team_id")).trim,
            caracteres(row("matches_tournament_id")).trim
          )
        )
        .distinct
        .sorted
        .map(x => escribirtxt(nombretxt, insertFormat.format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11)))

      println("Datos de goals generados correctamente.")
    }

    def generarDatosTablaSquadsTXT(data: List[Map[String, String]]): Unit = {
      val nombreTxt = "squads.txt"
      val formatoInsert = "INSERT INTO squads(player_id, shirt_number, " +
        "position_name, team_id, tournament_id) VALUES('%s', %s, '%s', '%s','%s');"
      val valores = data
        .map(x => (
          x("squads_player_id").trim,
          x("squads_shirt_number").toInt,
          x("squads_position_name"),
          x("squads_team_id").trim,
          caracteres(x("squads_tournament_id")).trim
        ))
        .distinct
        .map(x => escribirtxt(nombreTxt, formatoInsert.format(x._1, x._2, x._3, x._4, x._5)))
      println("Generando datos para la tabla squads...")
    }




    //---------------------------------------------------------------------------------------------------------------------

    def generarDatosTablaTeams(data: List[Map[String, String]]) =
      val v = data
        .map(x => (caracteres(x("goals_team_id").trim),
          x("stadiums_country_name").trim,
          valoresI(x("away_mens_team")),
          valoresI(x("away_womens_team"))
        ))
        .distinct
        .map(t =>
        sql"""INSERT INTO teams(team_id, country_id, mens_team, womens_team) " +
                "VALUES (${t._1},${t._2},${t._3},${t._4})"""
          .stripMargin
          .update)
      v

    def generateDataTournamentsTable(data: List[Map[String, String]]) =
      val v = data
        .map(x => (x("matches_tournament_id").trim,
          caracteres(x("tournaments_tournament_name")).trim,
          x("tournaments_year").toInt,
          x("tournaments_count_teams").trim,
          x("tournaments_winner").trim))
        .distinct
        .map(t =>
          sql"""INSERT INTO tournaments(tournament_id, tournament_name,year,count_teams, tournament_winner) VALUES(${t._1},${t._2},${t._3},${t._4},${t._5})"""
            .stripMargin
            .update)
      v

    def generarDatosTablaPlayers(data: List[Map[String, String]]) =
      val v = data
        .map(x => ( x("squads_player_id").trim,
          caracteres(x("players_family_name")).trim,
          caracteres(x("players_given_name")).trim,
          notNullDate(x("players_birth_date")).trim,
          x("players_female").toInt,
          x("players_goal_keeper").toInt,
          x("players_defender").toInt,
          x("players_midfielder").toInt,
          x("players_forward").toInt
        ))
        .distinct
        .map(t =>
        sql""""INSERT INTO players(player_id, family_name, given_name, birth_date," +
          " female, goal_keeper, defender, midfielder,forward) VALUES(${t._1},${t._2},${t._3},${t._4},${t._5},
                     ${t._6}, ${t._7}, ${t._8}, ${t._9})"""
        .stripMargin
        .update)
      v

    def generarDatosTablaMatches(data: List[Map[String, String]]) =
      val v = data
        .map(x => (x("matches_match_id").trim,
          notNullDate(x("matches_match_date")).trim,
          x("matches_match_time").trim,
          x("matches_stage_name").trim,
          x("matches_home_team_score").toInt,
          x("matches_away_team_score").toInt,
          x("matches_extra_time").toInt,
          x("matches_penalty_shootout").toInt,
          x("matches_home_team_score_penalties").toInt,
          x("matches_away_team_score_penalties").toInt,
          caracteres(x("matches_result")).trim,
          caracteres(x("matches_stadium_id").trim),
          caracteres(x("matches_tournament_id")),
          caracteres(x("matches_away_team_id")),
          x("matches_home_team_id")
        ))
        .distinct
        .map(t =>
        sql"""INSERT INTO matches(match_id, match_date, match_time, " +
                  "stage_name, home_team_score, away_team_score, extra_time, penalty_shootout," +
                  "home_team_score_penalties, away_team_score_penalties, result, " +
                  "stadium_id, tournament_id, away_team_id, home_team_id ) " +
                  "VALUES(${t._1}, ${t._2}, ${t._3}, ${t._4}, ${t._5}, ${t._6}, ${t._7}, ${t._8}, ${t._9}, ${t._10},
                                                      ${t._11}, ${t._12}, ${t._13}, ${t._14}, ${t._15})"""
        .stripMargin
        .update)
      v

    def generateDataGoalsTable(data: List[Map[String, String]]) =
      val v = data
        .map(x => (caracteres(x("goals_goal_id").trim),
          caracteres(x("goals_team_id").trim),
          caracteres(x("goals_player_id").trim),
          caracteres(x("goals_player_team_id").trim),
          caracteres(x("goals_minute_label").trim),
          valoresI(x("goals_minute_regulation").trim).toInt,
          valoresI(x("goals_minute_stoppage").trim).toInt,
          caracteres(x("goals_match_period").trim),
          valoresI(x("goals_own_goal").trim).toInt,
          valoresI(x("goals_penalty").trim).toInt)
        )
        .distinct
        .map(t =>
          sql"""INSERT INTO goals(goals_goal_id, goals_team_id, goals_player_id, goals_player_team_id,
              goals_minute_label, goals_minute_regulation, goals_minute_stoppage,
              goals_match_period,goals_own_goal , goals_penalty ) VALUES(${t._1}, ${t._2}, ${t._3}, ${t._4}, ${t._5},
              ${t._6}, ${t._7}, ${t._8}, ${t._9}, ${t._10})"""
            .stripMargin
            .update)
      v

    def generateDataSquadsTable(data: List[Map[String, String]]) = {
      val v = data
        .map(x => (
          x("squads_player_id").trim,
          x("squads_shirt_number").toInt,
          x("squads_position_name"),
          x("squads_team_id").trim,
          caracteres(x("squads_tournament_id")).trim
        ))
        .distinct
        .map(t =>
          sql"""INSERT INTO squads(player_id, shirt_number, " +
                "position_name, team_id, tournament_id) VALUES(${t._1}, ${t._2}, ${t._3}, ${t._4}, ${t._5})"""
            .stripMargin
            .update)

      v
    }
    

// ------------------ manipulación directa de la base de datos ----------------------------------------
    //generateDataTournamentsTable(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())
    //generateDataGoalsTable(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())
    //generarDatosTablaMatches(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())
    //generarDatosTablaPlayers(contentFile).foreach(i => i.run.transact(xa).unsafeRunSync())
    //generarDatosTablaTeams(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())
    //generateDataSquadsTable(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())

// ------------- generar script -------------------
  //generateDataTournamentsTableTXT(contentFile2)
  //generateDataStadiumTableTXT(contentFile2)
  //generateDataMatchesTableTXT(contentFile2)
  //generateDataGoalsTXT(contentFile2)
    //generarDatosTablaPlayersTXT(contentFile)
  //generarDatosTablaTeamsTXT(contentFile2)
  //generarDatosTablaSquadsTXT(contentFile2)

}

