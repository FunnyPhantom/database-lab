import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

// build gradle dependency:
// compile group: 'mysql', name: 'mysql-connector-java', version: '8.0.15'

public class JDBCTest {
    private static final String URL_CONNECTION = "jdbc:mysql://localhost/university?" +
            "user=n1&password=123456@a&useSSL=false&allowPublicKeyRetrieval=true";

    public static void main(String[] args) {
        menu();
    }

    public static void menu() {
        System.out.println("[1] Display courses");
        System.out.println("[2] Copy all sections to 'Fall 2018'");
        System.out.println("[3] Add a student");
        System.out.println("[4] Edit a student");
        System.out.println("[5] Take a course");
        System.out.println("[6] Delete a course");
        System.out.println("[0] Exit");

        try (Scanner in = new Scanner(System.in)) {
            int input = in.nextInt();

            switch (input) {
                case 0:
                    return;
                case 1:
                    displayCourses();
                    break;
                case 2:
                    copyAllSections();
                    break;
                case 3:
                    addStudent();
                    break;
                case 4:
                    editStudent();
                    break;
                case 5:
                    takeCourse();
                    break;
                case 6:
                    deleteCourse();
                    break;
                default:
                    System.out.println("Invalid input. Try again.");
                    menu();
                    break;
            }
        }
    }

    private static void displayCourses() {
        showQueryResults("SELECT dept_name FROM department");
        try(Scanner in = new Scanner(System.in)) {
            int input = in.nextInt();
            System.out.println(input);

        }
        // TODO
    }

    private static void copyAllSections() {
        // TODO
        try (Connection connection = DriverManager.getConnection(URL_CONNECTION)) {
            try (Statement statement = connection.prepareStatement(
                    "INSERT IGNORE into section(course_id, sec_id, semester, year, building, room_number, time_slot_id) " +
                            "SELECT course_id, sec_id, 'Fall', 2018, building, room_number, time_slot_id FROM section")) {

                ((PreparedStatement) statement).executeUpdate();
            }
            try (Statement statement = connection.prepareStatement(
                    "INSERT IGNORE into teaches(ID, course_id, sec_id, semester, year) " +
                            "SELECT ID, course_id, sec_id, 'Fall', 2018 FROM teaches")) {

                ((PreparedStatement) statement).executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static void addStudent() {
        // TODO
    }

    private static void takeCourse() {
        // TODO
    }

    private static void deleteCourse() {
        // TODO
    }

    private static void editStudent() {
        // TODO
    }

    public static void showQueryResults(String query) {
        try (
//                Class.forName("com.mysql.jdbc.Driver");
                Connection connection = DriverManager.getConnection(
                        URL_CONNECTION)) {
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(query)) {

                ResultSetMetaData metaData = rs.getMetaData();

                System.out.print("     ");
                for (int c = 1; c <= metaData.getColumnCount(); c++) {
                    System.out.printf("%-30s", metaData.getColumnName(c));
                }
                System.out.println();

                int i = 1;
                while (rs.next()) {
                    System.out.printf("[%2d] ", i++);
                    for (int c = 1; c <= metaData.getColumnCount(); c++) {
                        System.out.printf("%-30s", rs.getString(c));
                    }
                    System.out.println();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}