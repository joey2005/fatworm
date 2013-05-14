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
	public Object getFromColumn(int columnIndex) {
		return datas.get(columnIndex).getValue();
	}
}
