package fatworm.indexing.table;

import java.util.*;

import fatworm.indexing.data.Data;
import fatworm.indexing.schema.Schema;

public class Record {
	
	private List<Data> datas;
	private Schema schema;
	
	public Record(List<Data> datas, Schema schema) {
		this.datas = datas;
		this.schema = schema;
	}
	
	public Schema schema() {
		return schema;
	}
	
	public List<Data> getData() {
		return datas;
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
		int index = schema.indexOf(name);
		if (index < 0 || index >= datas.size()) {
			return null;
		} else {
			return datas.get(index);
		}
	}
}
