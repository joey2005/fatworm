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
	
	public void setValue(int columnIndex, Data data) {
		List<Data> tmp = new ArrayList<Data>();
		for (int i = 0; i < datas.size(); ++i) {
			if (i != columnIndex) {
				tmp.add(datas.get(i));
			} else {
				tmp.add(data);
			}
		}
		datas = tmp;
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
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Record)) {
			return false;
		}
		Record r = (Record) o;
		if (r.datas.size() != datas.size()) {
			return false;
		}
		for (int i = 0; i < datas.size(); ++i) {
			if (!(datas.get(i).equals(r.datas.get(i)))) {
				return false;
			}
		}
		return true;
	}
}
