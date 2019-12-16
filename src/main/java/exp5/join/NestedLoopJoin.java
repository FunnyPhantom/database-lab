package exp5.join;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Mohammad.Nosrati
 */
public class NestedLoopJoin extends Join {

    @Override
    protected void joinAlgorithm(Table t1, Table t2, List<String> commonColumns, Table joinResult) {
        for (Map<String, Object> t1Row : t1.getRows()) {
            for (Map<String, Object> t2Row : t2.getRows()) {
                if (rowsMatch(t1Row, t2Row, commonColumns)) {
                    joinRows(t1Row, t2Row, t1, t2, joinResult);
                }
            }
        }
    }

    @Override
    protected void leftOuterJoinAlgorithm(Table left, Table right, List<String> onCols, Table joinResult) {
        for (Map<String, Object> t1Row : left.getRows()) {
            boolean found = false;
            for (Map<String, Object> t2Row : right.getRows()) {
                if (rowsMatch(t1Row, t2Row, onCols)) {
                    joinRows(t1Row, t2Row, left, right, joinResult);
                    found =true;
                }
            }
            if (!found) {
                joinRows(t1Row, new HashMap<>(), left, right, joinResult);
            }

        }
    }

}
