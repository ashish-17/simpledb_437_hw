/**
 * 
 */
package simpledb;

import java.io.IOException;

import simpledb.Predicate.Op;

/**
 * Sort merge join.
 * @author ashish
 *
 */
public class SMJJoin extends AbstractJoin {

    /**
     * Last used tuple from outer relation.
     */
    private Tuple _outerRecent=null;
    
    /**
     * Last used tuple from inner relation.
     */
    private Tuple _innerRecent=null;
    
    /**
     * First match for outer tuple in inner relation's partition.
     */
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
						// If we are at end of inner relation, then increment the outer relation and reset the inner relation's iterator back to first match.
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
				
				// If predicate matches then join the tuples and proceed to next tuple in inner relation
				if (_predicate.filter(_outerRecent, _innerRecent)) {
					++_numMatches;
					++_numComp;

					result = joinTuple(_outerRecent, _innerRecent, getTupleDesc());

					// If this is the first tuple in partition then store its value to later jump to this position using its record id.
					if (_firstMatch == null) {
						_firstMatch = _innerRecent;
					}

					_innerRecent = null;
					
					break;
				} else if (_predicate.getLeftField(_outerRecent).compare(Op.LESS_THAN, _predicate.getRightField(_innerRecent))) {
					++_numComp;

					if (_outerRelation.hasNext()) {
						_outerRecent = _outerRelation.next();
						
						/* If the tuples in the outer relation have duplicates then we need to join them too with the
							previous matches in the inner relation, so we take the inner relation's iterator back to the first match's position.
						*/
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
