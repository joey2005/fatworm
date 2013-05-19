package fatworm.indexing.table;

import java.util.List;

import fatworm.indexing.data.Data;
import fatworm.indexing.schema.Schema;

public class Record {
	
	private List<Data> datas;
	private Schema schema;
	
	public Record(List<Data> datas, Schema schema) {
		this.datas = datas;
		this.schema = schema;
	}

	/**
	 * get data from specific column
	 * @param columnIndex
	 * @return
	 */
	public Data getFromColumn(int columnIndex) {
		return datas.get(columnIndex);
	}

	/**
	 * get data from variable name
	 * @param name
	 * @return
	 */
	public Data getFromVariableName(String name) {
		Data result = null;
		for (int i = 0; i < schema.getColumnCount(); ++i) {
			if (schema.getFields(i).getColumnName().equals(name)) {
				result = datas.get(i);
			}
		}
		return result;
	}
}
