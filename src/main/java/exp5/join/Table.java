package exp5.join;

import java.sql.*;
import java.util.*;

/**
 * @author Mohammad.Nosrati
 */
public class Table {
    private String name;
    private final List<String> columns;
    private final Set<Map<String, Object>> rows = new LinkedHashSet<>();

    public Table(String name, List<String> columns) {
        this.name = name;
        this.columns = columns;
    }

    public static Table tableFromSQLQuery(Connection connection, String query, String name) {
        Table result = null;
        try (PreparedStatement st = connection.prepareStatement(query)) {
            try (ResultSet rs = st.executeQuery()) {

                ResultSetMetaData metaData = rs.getMetaData();
                List<String> columns = new ArrayList<>();
                for (int c = 1; c <= metaData.getColumnCount(); c++) {
                    columns.add(metaData.getColumnName(c));
                }
                result = new Table(name, columns);

                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();

                    for (int c = 1; c <= metaData.getColumnCount(); c++) {
                        row.put(metaData.getColumnName(c), rs.getObject(c));
                    }
                    result.getRows().add(row);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<Map<String, Object>> getRows() {
        return rows;
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        int width = 20;

        for (String col: columns) {
            sb.append(String.format("%20s|", col));
        }
        sb.append("\n");

        for (int i = 0; i < columns.size(); i++) {
            for (int j = 0; j < width; j++) {
                sb.append("-");
            }
            sb.append("+");
        }
        sb.append("\n");

        for (Map<String, Object> row : rows) {
            for (String col: columns) {
                sb.append(String.format("%20s|", row.get(col)));
            }
            sb.append('\n');
        }

        return sb.toString();

    }
}
