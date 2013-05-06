package fatworm.indexing.data;

import java.math.BigDecimal;

public abstract class NumberData extends Data {

	public abstract int sign();
	
	public abstract Integer IntegerValue();
	
	public abstract Float FloatValue();
	
	public abstract BigDecimal DecimalValue();

}
