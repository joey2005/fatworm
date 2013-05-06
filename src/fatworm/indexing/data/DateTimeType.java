package fatworm.indexing.data;

import java.sql.Date;

public class DateTimeType extends DataType {
	
	public DateTimeType() {
	}

	@Override
	public int storageRequired() {
		return DataType.DATETIME_STORAGE_REQUIRED;
	}

	@Override
	public Data getDefaultValue() {
		if (defaultValue == null) {
			defaultValue = new DateTimeData(new Date(0), this);
		}
		return defaultValue;
	}

	@Override
	public Data valueOf(String c) {
		return new DateTimeData(new Date(Long.parseLong(c)), this);
	}
	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof DateTimeData) {
			DateTimeData date = (DateTimeData)data;
			return new DateTimeData((Date)data.getValue(), this);
		}
		throw new Exception("Data Format Wrong");
	}

	@Override
	public String toString() {
		return "DateTime";
	}

	private DateTimeData defaultValue = null;
}
