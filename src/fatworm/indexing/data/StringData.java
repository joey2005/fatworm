package fatworm.indexing.data;

public abstract class StringData extends Data {

	public int compareTo(Data o) {
		if (o instanceof StringData) {
			StringData str = (StringData) o;
			if (isNull() || o.isNull()) {
				return 1;
			}
			return getValue().compareTo(str.getValue());
		}
		return 0x0fffffff;
	}

	@Override
	public boolean isNull() {
		return getValue() == null;
	}

	@Override
	public abstract String getValue();
}
