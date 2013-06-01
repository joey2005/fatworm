package fatworm.indexing.data;

import java.sql.Timestamp;

public class DateTimeType extends DataType {
	
	public DateTimeType() {
	}

	@Override
	public int storageRequired() {
		return DataType.DATETIME_STORAGE_REQUIRED;
	}

	@Override
	public Data getDefaultValue() {
		return new DateTimeData(new Timestamp(System.currentTimeMillis()), this);
	}

	@Override
	public Data valueOf(String c) {
		if (c == null || c.equals("null")) {
			return new DateTimeData(null, this);
		}
		return new DateTimeData(Timestamp.valueOf(c), this);
	}
	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof DateTimeData) {
			DateTimeData date = (DateTimeData)data;
			return new DateTimeData((Timestamp)date.getValue(), this);
		}
		throw new Exception("Data Format Wrong");
	}

	@Override
	public String toString() {
		return "DateTime";
	}

	@Override
	public int encode() {
		return DATETIME;
	}

}
