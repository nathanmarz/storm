package org.apache.storm.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import java.util.List;

public class SqlCreateFunction extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator(
            "CREATE_FUNCTION", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
                SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... o) {
            assert functionQualifier == null;
            return new SqlCreateFunction(pos, (SqlIdentifier) o[0], o[1]);
        }

        @Override
        public void unparse(
                SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            SqlCreateFunction t = (SqlCreateFunction) call;
            UnparseUtil u = new UnparseUtil(writer, leftPrec, rightPrec);
            u.keyword("CREATE", "FUNCTION").node(t.functionName).keyword("AS").node(t.className);
        }
    };

    private final SqlIdentifier functionName;
    private final SqlNode className;

    public SqlCreateFunction(SqlParserPos pos, SqlIdentifier functionName, SqlNode className) {
        super(pos);
        this.functionName = functionName;
        this.className = className;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(functionName, className);
    }


    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        getOperator().unparse(writer, this, leftPrec, rightPrec);
    }

    public String functionName() {
        return functionName.toString();
    }

    public String className() {
        return ((NlsString)SqlLiteral.value(className)).getValue();
    }
}
