package fatworm.indexing.data;

import java.sql.Timestamp;

public class TimestampType extends DataType {
	
	public TimestampType() {
	}

	@Override
	public int storageRequired() {
		return DataType.TIMESTAMP_STORAGE_REQUIRED;
	}

	@Override
	public Data getDefaultValue() {
		return new TimestampData(new Timestamp(System.currentTimeMillis()), this);
	}

	@Override
	public Data valueOf(String c) {
		return new TimestampData(Timestamp.valueOf(c), this);
	}

	@Override
	public Data valueOf(Data data) throws Exception {
		if (data instanceof TimestampData) {
			TimestampData tdata = (TimestampData)data;
			return new TimestampData((Timestamp)tdata.getValue(), this);
		}
		throw new Exception("Data Format Wrong");
	}

	@Override
	public String toString() {
		return "Timestamp";
	}

	@Override
	public int encode() {
		return TIMESTAMP;
	}

}
