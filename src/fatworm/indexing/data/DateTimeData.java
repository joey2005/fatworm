package fatworm.indexing.data;

import java.sql.Date;

public class DateTimeData extends Data {
	
	public DateTimeData(Date d, DateTimeType type) {
		this.d = d;
		this.type = type;
	}

	@Override
	public int compareTo(Data args0) {
		if (args0 instanceof DateTimeData) {
			DateTimeData o = (DateTimeData)args0;
			if (this.isNull()) {
				return o.isNull() ? 0 : -1;
			}
			if (o.isNull()) {
				return 1;
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

	private Date d;
	private DateTimeType type;
}
