package fatworm.indexing.data;

import java.sql.Timestamp;

public class TimestampData extends Data {

	public TimestampData(Timestamp time, TimestampType type) {
		this.time = time;
		this.type = type;
	}

	@Override
	public int compareTo(Data arg0) {
		if (arg0 instanceof TimestampData) {
			TimestampData o = (TimestampData)arg0;
			if (isNull()) {
				return o.isNull() ? 0 : -1;
			}
			if (o.isNull()) {
				return 1;
			}
			return this.compareTo(o);
		}
		return 0x0fffffff;
	}

	@Override
	public boolean isNull() {
		return time == null;
	}

	@Override
	public DataType getType() {
		return type;
	}

	@Override
	public String toString() {
		if (time == null) {
			return null;
		}
		return time.toString();
	}

	@Override
	public Object getValue() {
		return time;
	}

	
	private Timestamp time;
	private TimestampType type;
	
}
