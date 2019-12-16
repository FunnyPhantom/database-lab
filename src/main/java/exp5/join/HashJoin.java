package exp5.join;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashJoin extends Join {
    @Override
    protected void joinAlgorithm(Table t1, Table t2, List<String> commonColumns, Table joinResult) {
        Map<String, Map<String, Object>> lookup = new HashMap<>();
        for (Map<String, Object> row: t1.getRows()) {
            lookup.put(makeHash(row, commonColumns), row);
        }
        for (Map<String, Object> t2Row : t2.getRows()) {
            String hash = makeHash(t2Row, commonColumns);
            if (lookup.containsKey(hash)) {
                joinRows(lookup.get(hash), t2Row, t1, t2, joinResult);
            }
        }
    }

    private String makeHash(Map<String, Object> row, List<String> cols) {
        StringBuilder sb = new StringBuilder();
        for (String col:
             cols) {
            sb.append(row.get(col));
        }
        return sb.toString();
    }

    @Override
    protected void leftOuterJoinAlgorithm(Table left, Table right, List<String> onCols, Table joinResult) {
        //felan bikhi;
    }
}
