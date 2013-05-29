package fatworm.indexing.data;

import java.sql.Timestamp;

public class TimestampData extends Data {
	
	private Timestamp time;
	private TimestampType type;
	
	public TimestampData(Timestamp time, TimestampType type) {
		this.time = time;
		this.type = type;
	}

	@Override
	public int compareTo(Data arg0) {
		if (arg0 instanceof TimestampData) {
			TimestampData o = (TimestampData)arg0;
			if (isNull() && o.isNull()) {
				return 0;
			}
			if (isNull() || o.isNull()) {
				return 0x7f7f7f7f;
			}
			return time.compareTo(o.time);
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

	@Override
	public boolean equals(Object obj) {
		return this.compareTo((Data)obj) == 0;
	}

	@Override
	public String storageValue() {
		if (time == null) {
			return "null";
		}
		return time.toString();
	}
}
