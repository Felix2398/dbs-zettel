package org.example;

import java.io.IOException;
import java.sql.*;

public class Main {
    static String databaseName;
    static String ip;
    static String port;
    static String username;
    static String password;
    static String keyword;
    static Connection conn;
    static String combined = "WITH combined AS (SELECT * FROM actor UNION ALL SELECT * FROM actress) ";
    static String space = "        ";


    public static void main(String[] args) {
        try {
            if (args.length == 0) throw new IOException("no arguments found");
            parse(args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String url = "jdbc:postgresql://" + ip + ":" + port + "/" + databaseName;

        try {
            conn = DriverManager.getConnection(url, username, password);
            printMovies();
            System.out.println();
            printActors();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    // parse command line
    //------------------------------------------------------------------------------------------------------------------
    private static void parse(String[] args) throws IOException {
        for (int i = 0; i < args.length; i += 2) {
            String value;
            try {
                value = args[i + 1];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException("missing argument after " + args[i] + " flag");
            }

            switch (args[i]) {
                case "-d" -> databaseName = value;
                case "-s" -> ip = value;
                case "-p" -> port = value;
                case "-u" -> username = value;
                case "-pw" -> password = value;
                case "-k" -> keyword = value;
                default -> throw new IOException("flag " + args[i] + " doesnt exist");
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    // print movies
    //------------------------------------------------------------------------------------------------------------------
    private static void printMovies() throws SQLException {
        String sql = "SELECT * " +
                "FROM movie " +
                "WHERE movie.title LIKE ? " +
                "ORDER BY movie.title";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setString(1, "%" + keyword + "%");
        ResultSet rs = statement.executeQuery();

        System.out.println("MOVIES");
        while (rs.next()) {
            String mid = rs.getString("mid");
            String title = rs.getString("title");
            String year = rs.getString("year");
            System.out.print(title);
            System.out.print(", " + year);
            printGenresOfMovie(mid);
            System.out.println();
            printInvolvedActors(mid);
            System.out.println();
        }
    }

    private static void printGenresOfMovie(String movieId) throws SQLException {
        String sql = "SELECT * " +
                "FROM genre " +
                "WHERE genre.movie_id  = ?";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setString(1, movieId);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            String genre = rs.getString("genre");
            System.out.print(", " + genre);
        }
    }

    private static void printInvolvedActors(String movieId) throws SQLException {
        String sql = combined +
                "SELECT name " +
                "FROM combined " +
                "WHERE combined.movie_id = ? " +
                "ORDER BY name " +
                "FETCH FIRST 5 ROWS ONLY";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setString(1, movieId);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            String name = rs.getString("name");
            System.out.println(space + name);
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    // print actors
    //------------------------------------------------------------------------------------------------------------------
    private static void printActors() throws SQLException {
        String sql = combined +
                "SELECT DISTINCT combined.name " +
                "FROM combined " +
                "WHERE combined.name LIKE ? " +
                "ORDER BY combined.name ";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setString(1, "%" + keyword + "%");
        ResultSet rs = statement.executeQuery();

        System.out.println("ACTORS");
        while (rs.next()) {
            String name = rs.getString("name");
            System.out.println(name);
            System.out.println(space + "PLAYED IN");
            printMoviesWithActor(name);
            System.out.println(space + "CO-STARS");
            printCoStars(name);
        }
    }

    private static void printMoviesWithActor(String actorName) throws SQLException {
        String sql = combined +
                "SELECT movie.title " +
                "FROM combined, movie " +
                "WHERE movie.mid = combined.movie_id AND name = ?";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setString(1, actorName);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            String title = rs.getString("title");
            System.out.println(space + space + title);
        }
    }

    private static void printCoStars(String actorName) throws SQLException {
        String sql = combined +
                "SELECT combined.name, COUNT(combined.name) AS total " +
                "FROM combined " +
                "WHERE combined.movie_id IN (SELECT movie_id FROM combined WHERE combined.name = ?) " +
                "AND combined.name <> ? " +
                "GROUP BY combined.name " +
                "ORDER BY total DESC, name ASC " +
                "FETCH FIRST 5 ROWS ONLY";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setString(1, actorName);
        statement.setString(2, actorName);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            String name = rs.getString("name");
            String total = rs.getString("total");
            System.out.println(space + space + name + " (" + total + ")");
        }
    }
}