package fatworm.indexing.data;

public abstract class StringData extends Data {

	public int compareTo(Data o) {
		if (o instanceof StringData) {
			StringData str = (StringData) o;
			if (isNull() && o.isNull()) {
				return 0;
			}
			if (isNull() || o.isNull()) {
				return 0x7f7f7f7f;
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
