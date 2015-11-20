/**
 * 
 */
package simpledb;

import java.io.IOException;

import simpledb.Predicate.Op;

/**
 * @author ashish
 *
 */
public class SMJJoin extends AbstractJoin {


    private Tuple _outerRecent=null;
    private Tuple _innerRecent=null;
    private Tuple _firstMatch = null;
    
	public SMJJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
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
		Tuple result = null;
		
		try {
			while((_outerRelation.hasNext() && _innerRelation.hasNext()) || (_outerRecent != null)) {
				if (_outerRecent == null) {
					_outerRecent = _outerRelation.next();
				}
				
				if (_innerRecent == null) {
					if (_innerRelation.hasNext()) {
						_innerRecent = _innerRelation.next();
					} else {
						if (_outerRelation.hasNext()) {
							_outerRecent = _outerRelation.next();
							if (_firstMatch != null && _predicate.filter(_outerRecent, _firstMatch)) {
								_innerRecent = _innerRelation.seek(_firstMatch.getRecordID());
							}
							
							_firstMatch = null;
						}
					}
					
					if (_innerRecent == null) {
						break;
					}
				}
				
				if (_predicate.filter(_outerRecent, _innerRecent)) {
					++_numMatches;
					++_numComp;

					result = joinTuple(_outerRecent, _innerRecent, getTupleDesc());

					if (_firstMatch == null) {
						_firstMatch = _innerRecent;
					}

					_innerRecent = null;
					
					break;
				} else if (_predicate.getLeftField(_outerRecent).compare(Op.LESS_THAN, _predicate.getRightField(_innerRecent))) {
					++_numComp;

					if (_outerRelation.hasNext()) {
						_outerRecent = _outerRelation.next();
						if (_firstMatch != null && _predicate.filter(_outerRecent, _firstMatch)) {
							_innerRecent = _innerRelation.seek(_firstMatch.getRecordID());
						}
						
						_firstMatch = null;
					} else {
						_outerRecent = null;
					}
				} else {
					++_numComp;
					_innerRecent = null;
					
					if (_firstMatch != null) {
						_firstMatch = null;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return result;
	}

}
