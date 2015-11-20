/**
 * 
 */
package simpledb;

import java.io.IOException;

/**
 * Simple nested loop join.
 * @author ashish
 *
 */
public class SNLJoin extends AbstractJoin {

    /**
     * Last used tuple in outer relation.
     */
    private Tuple _outerRecent=null;
    
    /**
     * Last used tuple in inner relation.
     */
    private Tuple _innerRecent=null;
    
	public SNLJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
		super(p, child1, child2);
    }
	
	/* (non-Javadoc)
	 * @see simpledb.DbIterator#open()
	 */
	@Override
	public void open() throws DbException, TransactionAbortedException, IOException {
		super.open();
		
    	_outerRelation.open();
    	_innerRelation.open();

    	_outerRecent = null;
    	_innerRecent = null;
	}
	
	/* (non-Javadoc)
	 * @see simpledb.DbIterator#close()
	 */
	@Override
	public void close() {
		super.close();
		
    	_outerRelation.close();
    	_innerRelation.close();
	}
	
	/* (non-Javadoc)
	 * @see simpledb.AbstractDbIterator#readNext()
	 */
	@Override
	protected Tuple readNext() throws DbException, TransactionAbortedException {
		try {
			while(_outerRelation.hasNext() || (_outerRecent != null)) {
				if (_outerRecent == null) {
					_outerRecent = _outerRelation.next();
				}
				while ((_outerRecent != null) && _innerRelation.hasNext()) {
					_innerRecent = _innerRelation.next();
					if (_innerRecent != null) {
						++_numComp;
						if (_predicate.filter(_outerRecent, _innerRecent)) {
							++_numMatches;
							return joinTuple(_outerRecent, _innerRecent, getTupleDesc());
						}
					}
				}
				
				if (_outerRelation.hasNext()) {
					_innerRelation.rewind();
				}
				
				_outerRecent = null; // To ensure getting next tuple (if any) in following iteration.
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return null;
	}
}
