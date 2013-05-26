package fatworm.indexing.table;

import static fatworm.storage.file.Page.*;
import static fatworm.indexing.table.RecordPage.*;

import fatworm.indexing.data.IntegerType;
import fatworm.indexing.metadata.TableInfo;
import fatworm.indexing.schema.*;
import fatworm.storage.buffer.*;
import fatworm.storage.file.Page;

public class RecordFormatter implements PageFormatter {
	
	private TableInfo ti;
	
	public RecordFormatter(TableInfo ti) {
		this.ti = ti;
	}

   /** 
    * Formats the page by allocating as many record slots
    * as possible, given the record length.
    * Each record slot is assigned a flag of EMPTY.
    * Each integer field is given a value of 0, and
    * each string field is given a value of "".
    * @see fatworm.storage.buffer.PageFormatter#format(fatworm.storage.file.Page)
    */
	public void format(Page p) {
		int size = ti.recordLength() + INT_SIZE;
		for (int pos = 0; pos + size <= BLOCK_SIZE; pos += size) {
			p.setInt(pos, EMPTY);
			makeDefaultRecord(p, pos);
		}
	}

	private void makeDefaultRecord(Page page, int pos) {
		for (AttributeField af : ti.schema().getAllFields()) {
			int offset = ti.offset(af.getColumnName());
			if (af.getType() instanceof IntegerType) {
				page.setInt(pos + INT_SIZE + offset, 0);
			} else {
				page.setString(pos + INT_SIZE + offset, "");
			}
		}
	}
}
