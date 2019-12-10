

import common.Statics;

import java.sql.*;
import java.util.*;

import static common.Statics.*;

public class Exp4 {
    public Exp4() {
    }

    public static void main(String[] args) {
        Exp4 exp4 = new Exp4();
        exp4.showMenu();
    }

    private void showMenu() {
        System.out.println("[1] List Tables");
        System.out.println("[2] List Columns");
        System.out.println("[3] Search Keywords");
        System.out.println("[0] Exit");
        try (Scanner sc = new Scanner(System.in)) {
            int num = sc.nextInt();
            switch (num) {
                case 1:
                    listTablesMenu(sc);
                    return;
                case 2:
                    listColumnsMenu(sc);
                    return;
                case 3:
                    searchKeywordMenu(sc);
                    return;
                default:
            }
        }
    }

    private void searchKeywordMenu(Scanner sc) {
        System.out.println("Enter Keyword:");
        String keyword = sc.next();
        System.out.println("This may take a while, please wait...");
        try (Connection connection = DriverManager.getConnection(URL_CONNECTION);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA = 'university'")) {
            ArrayList<ResultSet> ans = new ArrayList<>();
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                Statement colState = connection.createStatement();
                ResultSet colRes = colState.executeQuery("SELECT COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA = 'university' and TABLE_NAME = '" + tableName + "'");
                ArrayList<String> cols = new ArrayList<>();
                while (colRes.next()){
                    cols.add(colRes.getString(1));
                }
                Statement searchStatement = connection.createStatement();
                ResultSet searchResult = searchStatement.executeQuery("SELECT * from " + tableName + " where " + makeORLikeQuery(cols, keyword));
                ans.add(searchResult);
            }
            System.out.println();
            ans.forEach(resultSet1 -> {
                try {
                    System.out.println("Result for table " + resultSet1.getMetaData().getTableName(1) + ":");
                    if (!resultSet1.isBeforeFirst()) {
                        System.out.println("\t\tNo Match found in this table.");
                        System.out.println();
                    } else {
                        showQueryResults(resultSet1);
                        System.out.println();
                    }


                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    private String makeORLikeQuery(ArrayList<String> cols, String searchQuery) {
        StringBuilder sb = new StringBuilder();
        return cols.stream()
            .map(col -> col + " like '" + searchQuery + "'")
                .reduce((s, s2) -> s + " or " + s2).orElse("");
    }

    private void listColumnsMenu(Scanner sc) {
        System.out.println("[1] Information Schema Method");
        System.out.println("[2] Metadata.getTables() Method");
        System.out.println("[0] Exit");
        int input = sc.nextInt();

        if (input == 1 || input == 2) {
            System.out.println("Enter Table Name:");
            String tableName = sc.next();
            switch (input) {
                case 1:
                    informationSchemaShowColumn(tableName);
                    return;
                case 2:
                    tablesMetadataShowColumn(tableName);
                    return;
                default:
            }
        }

    }

    private void tablesMetadataShowColumn(String tableName) {
        try {
            Connection c = DriverManager.getConnection(URL_CONNECTION);
            DatabaseMetaData meta = c.getMetaData();
            ResultSet rs = meta.getColumns("university", null, null, null);
            Set<Integer> s = new HashSet<Integer>(1);
            s.add(rs.findColumn("COLUMN_NAME"));

            showQueryResults(rs, s);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void informationSchemaShowColumn(String tableName) {
        showQueryResults("SELECT COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA = 'university' and TABLE_NAME = '" + tableName + "'");
    }


    private void listTablesMenu(Scanner sc) {
        System.out.println("[1] Information Schema Method");
        System.out.println("[2] Metadata.getTables() Method");
        System.out.println("[0] Exit");
        int input = sc.nextInt();
        switch (input) {
            case 1:
                informationSchemaShowTables();
                return;
            case 2:
                tablesMetadataShowTables();
                return;
            default:
        }
    }

    private void tablesMetadataShowTables() {
        try {
            Connection c = DriverManager.getConnection(URL_CONNECTION);
            DatabaseMetaData meta = c.getMetaData();
            ResultSet rs = meta.getTables("university", null, null, null);
            Set<Integer> s = new HashSet<Integer>(1);
            s.add(rs.findColumn("TABLE_NAME"));

            showQueryResults(rs, s);

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void informationSchemaShowTables() {
        showQueryResults("SELECT TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA = 'university'");
    }


}
