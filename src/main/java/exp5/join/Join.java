package exp5.join;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Mohammad.Nosrati
 */
public abstract class Join {

    private List<String> getAllColumns(Table left, Table right) {
        List<String> columns = new ArrayList<>();
        columns.addAll(left.getColumns().stream()
                .map(col -> left.getName() + "." + col).collect(Collectors.toList()));
        columns.addAll(right.getColumns().stream()
                .map(col -> right.getName() + "." + col).collect(Collectors.toList()));
        return columns;
    }

    public Table join(Table t1, Table t2) {
        // columns:
        List<String> columns = getAllColumns(t1, t2);

        // get common columns in both tables:
        List<String> commonColumns = t1.getColumns().stream()
                .filter(c -> t2.getColumns().contains(c)).collect(Collectors.toList());

        // resulting table:
        Table joinResult = new Table(t1.getName() + " |><| " + t2.getName(), columns);

        joinAlgorithm(t1, t2, commonColumns, joinResult);

        return joinResult;
    }

    public Table leftOuterJoin(Table left, Table right, List<String> onCols) {
        List<String> columns = getAllColumns(left, right);


        Table joinResult = new Table("Result", columns);

        leftOuterJoinAlgorithm(left, right, onCols, joinResult);
        return joinResult;

    }


    public Table rightOuterJoin(Table left, Table right, List<String> onCols) {
        return leftOuterJoin(right, left, onCols);
    }

    protected boolean rowsMatch(Map<String, Object> t1Row, Map<String, Object> t2Row, List<String> commonColumns) {
        boolean matched = true;
        for (String commonColumn : commonColumns) {
            if (t1Row.get(commonColumn) == null) {
                matched = false;
            } else if (!t1Row.get(commonColumn).equals(t2Row.get(commonColumn))) {
                matched = false;
            }
        }
        return matched;
    }

    protected void joinRows(Map<String, Object> t1Row, Map<String, Object> t2Row, Table t1, Table t2, Table joinResult) {
        Map<String, Object> joinedRow = new LinkedHashMap<>();

        for (String t1Column : t1.getColumns()) {
            joinedRow.put(t1.getName() + "." + t1Column, t1Row.get(t1Column));
        }
        for (String t2Column : t2.getColumns()) {
            joinedRow.put(t2.getName() + "." + t2Column, t2Row.get(t2Column));
        }

        joinResult.getRows().add(joinedRow);
    }

    protected abstract void joinAlgorithm(Table t1, Table t2, List<String> commonColumns, Table joinResult);
    protected abstract void leftOuterJoinAlgorithm(Table left, Table right, List<String> onCols, Table joinResult);
}
