package exp5.join;

import sun.tools.jconsole.Tab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Mohammad.Nosrati
 */
public class Main {
    public static void main(String[] args) {
//         demo();

        try (Connection connection = getConnection()) {
            // q1
            q1(connection);

            //q2 in the report.

            //q3
            Table tblDirector = directorUsingSql(connection);
            //q4
            Table tblMovie = movieUsingSql(connection);

            // q5
            Table joinUsingNestedLoop = joinUsingNestedLoop(tblDirector, tblMovie);

            // q6
            Table joinUsingSql = joinUsingSql(connection);

            // q7
            leftOuterJoinUsingSql(connection);
            // q8
            rightOuterJoinUsingSql(connection);

            //q9
            List<String> onCols =new ArrayList<>();
            onCols.add("did");
            leftOuterJoinUsingNestedLoop(tblMovie, tblDirector, onCols);
            rightOuterJoinUsingNestedLoop(tblMovie, tblDirector, onCols);




            joinUsingHashJoin(tblDirector, tblMovie);




            // Q1:


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void q1(Connection connection) throws SQLException {
        connection.createStatement().execute("INSERT INTO movies.director value (20, 'Stanley Kubrick');" +
                "INSERT INTO movies.movie VALUE (30, 'Amour', 2012, 127, null);");
    }

    private static Table rightOuterJoinUsingSql(Connection connection) {
        Table t = Table.tableFromSQLQuery(connection, "SELECT * from movies.director right outer join movies.movie on director.did = movie.did;", "rightOuterJoinUsingSql");
        System.out.println("rightOuterJoinUsingSql");
        System.out.println(t);
        return t;

    }

    private static Table leftOuterJoinUsingSql(Connection connection) {
        Table t =  Table.tableFromSQLQuery(connection, "SELECT * from movies.director left outer join movies.movie on director.did = movie.did;", "leftOuterJoinUsingSql");
        System.out.println("leftOuterJoinUsingSql:");
        System.out.println(t);
        return t;
    }

    private static Table leftOuterJoinUsingNestedLoop(Table movie, Table director, List<String> onCols) {
        Join j = new NestedLoopJoin();
        Table t = j.leftOuterJoin(movie, director, onCols);
        System.out.println("leftOuterJoinUsingNestedLoop:");
        System.out.println(t);
        return t;

    }
    private static Table rightOuterJoinUsingNestedLoop(Table movie, Table director, List<String> onCols) {
        Join j = new NestedLoopJoin();
        Table t =  j.rightOuterJoin(movie, director, onCols);
        System.out.println("rightOuterJoinUsingNestedLoop:");
        System.out.println(t);
        return t;

    }

    private static void joinUsingHashJoin(Table tblDirector, Table tblMovie) {
        // TODO
    }

    private static Table joinUsingNestedLoop(Table tblDirector, Table tblMovie) {
        Join j = new NestedLoopJoin();
        Table t = j.join(tblDirector, tblMovie);
        System.out.println("joinUsingNestedLoop:");
        System.out.println(t);
        return t;
    }

    private static Table joinUsingSql(Connection connection) {
        Table t = Table.tableFromSQLQuery(connection, "SELECT * from movies.director join movies.movie", "joinUsingSql");
        System.out.println("joinUsingSql:");
        System.out.println(t);
        return t;
    }

    private static Table movieUsingSql(Connection connection) {
        Table t = Table.tableFromSQLQuery(connection, "SELECT * from movies.movie", "movie");
        System.out.println("MOVIE: ");
        System.out.println(t);
        return t;
    }

    private static Table directorUsingSql(Connection connection) {
        Table tblDir = Table.tableFromSQLQuery(connection, "SELECT * FROM movies.director", "director");
        System.out.println("DIRECTOR: ");
        System.out.println(tblDir);
        return tblDir;
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mysql://localhost/movies?" +
                        "user=n1&password=123456@a&useSSL=false&allowPublicKeyRetrieval=true");

    }

    // this is just to show you how these classes work:
    private static void demo() {
        Table t1 = new Table("T1", Arrays.asList("id", "name"));
        Table t2 = new Table("T2", Arrays.asList("id", "x"));

        for (int i = 0; i < 10; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "name_" + new Random().nextInt(100));
            t1.getRows().add(row);
        }

        for (int i = 5; i < 15; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("x", "x_" + new Random().nextInt(100));
            t2.getRows().add(row);
        }

        System.out.println(t1);
        System.out.println(t2);

        Join j = new NestedLoopJoin();

        Table result = j.join(t1, t2);
        System.out.println(result.getName());
        System.out.println(result);
    }
}
