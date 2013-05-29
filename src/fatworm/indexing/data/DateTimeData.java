package fatworm.indexing.data;

import java.sql.Timestamp;

public class DateTimeData extends Data {

	private Timestamp d;
	private DateTimeType type;
	
	public DateTimeData(Timestamp d, DateTimeType type) {
		this.d = d;
		this.type = type;
	}

	@Override
	public int compareTo(Data args0) {
		if (args0 instanceof DateTimeData) {
			DateTimeData o = (DateTimeData)args0;
			if (isNull() && o.isNull()) {
				return 0;
			}
			if (isNull() || o.isNull()) {
				return 0x7f7f7f7f;
			}
			return d.compareTo(o.d);
		}
		return 0x0fffffff;
	}

	@Override
	public boolean isNull() {
		return d == null;
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public String toString() {
		if (d == null) {
			return null;
		}
		return d.toString();
	}

	@Override
	public Object getValue() {
		return d;
	}

	@Override
	public boolean equals(Object obj) {
		return this.compareTo((Data)obj) == 0;
	}

	@Override
	public String storageValue() {
		if (d == null) {
			return "null";
		}
		return d.toString();
	}

}
