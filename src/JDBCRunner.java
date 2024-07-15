import java.sql.*;
import java.util.Scanner;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class JDBCRunner {
    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";
    private static final String DATABASE_NAME = "gamesDB";
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";
    private static final String URL_REMOTE = "host.docker.internal:5432/";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;


    public static void main(String[] args) throws SQLException {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            checkDriver();
            checkDB();
//        WithDrawGamesFor2019();
//        printAllRPGGenreGames();
//        getGames(connection);
//        addGames(connection,"Ведьмак 4",2026,"Action/RPG",100,11);
//        correctGameRating(connection,83,95);
//        removeGame(connection,83);
//        getGamesByStudio(connection,2);
//        getGamesByPublisher(connection,4);
//        getTheBestGameInTheGenre(connection);
//        getTheNumberOfGamesByRating(connection);
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    public static void WithDrawGamesFor2019() throws SQLException {
        Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM games WHERE year_of_release = ?;");
        statement.setInt(1, 2019);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String gameTitle = resultSet.getString("game_title");
            int ratingOfTheGame = resultSet.getInt("rating_of_the_game");

            System.out.printf("%d. %s - %d \n", id, gameTitle, ratingOfTheGame);
        }

        connection.close();
        statement.close();
    }

    static void getGames(Connection connection) throws SQLException {
        int param0 = -1, param2 = -1, param4 = -1, param5 = -1;
        String param1 = null, param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM games;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }


    public static void printAllRPGGenreGames() throws SQLException {
        Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM games WHERE genre = ?;");
        statement.setString(1, "Role-playing game");
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String gameTitle = resultSet.getString("game_title");
            int ratingOfTheGame = resultSet.getInt("rating_of_the_game");

            System.out.printf("%d. %s - %d \n", id, gameTitle, ratingOfTheGame);
        }

        connection.close();
        statement.close();
    }

    private static void addGames(Connection connection, String gameTitle, int yearOfRelease, String genre, int ratingOfTheGame, int idOfTheGameStudio) throws SQLException {
        if (gameTitle == null || gameTitle.isBlank() || yearOfRelease < 1958 || ratingOfTheGame < 0 || ratingOfTheGame > 100 || idOfTheGameStudio < 0 || genre == null || genre.isBlank())
            return;
        PreparedStatement statement = connection.prepareStatement("INSERT INTO games(game_title,year_of_release,genre,rating_of_the_game,id_of_the_game_studio) VALUES(?,?,?,?,?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, gameTitle);
        statement.setInt(2, yearOfRelease);
        statement.setString(3, genre);
        statement.setInt(4, ratingOfTheGame);
        statement.setInt(5, idOfTheGameStudio);
        int count = statement.executeUpdate();
        ResultSet resultSet = statement.getGeneratedKeys();
        if (resultSet.next()) {
            System.out.println("Идентификатор игры " + resultSet.getInt(1));
        }
        System.out.println("INSERTed " + count + " games");
        getGames(connection);
    }

    public static void correctGameRating(Connection connection, int id, int gameRating) throws SQLException {
        if (gameRating < 0 || gameRating > 100) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE games SET rating_of_the_game=? WHERE id=?;");
        statement.setInt(1, gameRating);
        statement.setInt(2, id);
        int count = statement.executeUpdate();
        System.out.println("UPDATEd " + count + " games");
        getGames(connection);
    }

    private static void removeGame(Connection connection, int id) throws SQLException {
        if (id == 0) return;
        PreparedStatement statement = connection.prepareStatement("DELETE from games WHERE id=?;");
        statement.setInt(1, id);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count + " games");
        getGames(connection);
    }

    private static void getGamesByStudio(Connection connection, int id) throws SQLException {
        if (id < 0 || id > 22) return;
        PreparedStatement statement = connection.prepareStatement("SELECT games.game_title,games.year_of_release, games.rating_of_the_game, game_studios.name_of_the_game_studio\n" +
                "FROM games\n" +
                "JOIN game_studios\n" +
                "ON  games.id_of_the_game_studio = game_studios.id\n" +
                "WHERE game_studios.id = ?");
        statement.setInt(1, id);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getInt(2) + " | " + rs.getInt(3) + " | " + rs.getString(4));
        }
    }

    private static void getGamesByPublisher(Connection connection, int id) throws SQLException {
        if (id < 0 || id > 9) return;
        PreparedStatement statement = connection.prepareStatement("SELECT games.game_title,games.year_of_release, games.rating_of_the_game, publisher.\"publisher's_name\"\n" +
                "FROM games\n" +
                "JOIN game_studios ON games.id_of_the_game_studio = game_studios.id\n" +
                "JOIN  publisher ON game_studios.\"publisher's_id\" = publisher.id\n" +
                "WHERE publisher.id = ?");
        statement.setInt(1, id);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getInt(2) + " | " + rs.getInt(3) + " | " + rs.getString(4));
        }
    }

    private static void getTheBestGameInTheGenre(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT  games.genre, game_title AS \"the_best_game_in_the_genre\", rating_of_the_game \n" +
                "FROM games\n" +
                "JOIN (SELECT genre, MAX(rating_of_the_game) AS max_rating\n" +
                "FROM games\n" +
                "GROUP BY genre) max_rating \n" +
                "ON games.rating_of_the_game = max_rating.max_rating AND games.genre = max_rating.genre\n" +
                "\n");
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getInt(3));
        }
    }

    private static void getTheNumberOfGamesByRating(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT\n" +
                "COUNT(*) AS total_games,\n" +
                "SUM(CASE WHEN rating_of_the_game < 50 THEN 1 ELSE 0 END) AS \"rating_less_than_50\",\n" +
                "SUM(CASE WHEN rating_of_the_game BETWEEN 50 AND 75 THEN 1 ELSE 0 END) AS \"rating from 50 to 75\",\n" +
                "SUM(CASE WHEN rating_of_the_game BETWEEN 75 AND 100 THEN 1 ELSE 0 END) AS \"rating from 75 to 100\"\n" +
                "FROM games;\n");
        while (rs.next()) {
            System.out.println("Total games: " + rs.getInt(1));
            System.out.println("games whose rating is less than 50: " + rs.getInt(2));
            System.out.println("games whose rating from 50 to 75: " + rs.getInt(3));
            System.out.println("games whose rating from 75 to 100: " + rs.getInt(4));
        }

    }
}


//    SELECT  games.genre, game_title AS "the_best_game_in_the_genre", rating_of_the_game
//        FROM games
//        JOIN (SELECT genre, MAX(rating_of_the_game) AS max_rating
//        FROM games
//        GROUP BY genre) max_rating
//        ON games.rating_of_the_game = max_rating.max_rating AND games.genre = max_rating.genre
