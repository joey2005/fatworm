package fatworm.driver;

import java.sql.SQLException;

import fatworm.indexing.data.BooleanType;
import fatworm.indexing.data.CharType;
import fatworm.indexing.data.DataType;
import fatworm.indexing.data.DateTimeType;
import fatworm.indexing.data.DecimalType;
import fatworm.indexing.data.FloatType;
import fatworm.indexing.data.IntegerType;
import fatworm.indexing.data.TimestampType;
import fatworm.indexing.data.VarcharType;
import fatworm.indexing.schema.Schema;

public class ResultSetMetaData implements java.sql.ResultSetMetaData {
	
	private Schema schema;

	public ResultSetMetaData(Schema schema) {
		this.schema = schema;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return schema.getColumnCount();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		if (schema == null) {
			return -1;
		}
		try {
			DataType type = schema.getAttributeOf(column - 1).getType();
			if (type instanceof BooleanType) {
				return java.sql.Types.BOOLEAN;
			}
			if (type instanceof CharType) {
				return java.sql.Types.CHAR;
			}
			if (type instanceof DateTimeType) {
				return java.sql.Types.TIMESTAMP;
			}
			if (type instanceof DecimalType) {
				return java.sql.Types.DECIMAL;
			}
			if (type instanceof FloatType) {
				return java.sql.Types.FLOAT;
			}
			if (type instanceof IntegerType) {
				return java.sql.Types.INTEGER;
			}
			if (type instanceof TimestampType) {
				return java.sql.Types.TIMESTAMP;
			}
			if (type instanceof VarcharType) {
				return java.sql.Types.VARCHAR;
			}
			return java.sql.Types.OTHER;
		} catch (Exception ex) {
			throw new SQLException("Wrong Type!");
		}
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

}
