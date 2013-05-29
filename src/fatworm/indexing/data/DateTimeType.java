package fatworm.indexing.data;

import java.sql.Date;
import java.sql.Timestamp;

public class DateTimeType extends DataType {

	private DateTimeData defaultValue = null;
	
	public DateTimeType() {
	}

	@Override
	public int storageRequired() {
		return DataType.DATETIME_STORAGE_REQUIRED;
	}

	@Override
	public Data getDefaultValue() {
		if (defaultValue == null) {
			defaultValue = new DateTimeData(new Timestamp(0), this);
		}
		return defaultValue;
	}

	@Override
	public Data valueOf(String c) {
		return new DateTimeData(Timestamp.valueOf(c), this);
	}
	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof DateTimeData) {
			DateTimeData date = (DateTimeData)data;
			return new DateTimeData((Timestamp)data.getValue(), this);
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
