/**
 * 
 */
package simpledb;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Page nested Loop join.
 * @author ashish
 *
 */
public class PNLJoin extends AbstractJoin {

    /**
     * A Page wise iterator on DB File for outer relation.
     */
    private BufferedDBIterator _outerPageIt;
    
    /**
     * A Page wise iterator on DB file for inner relation.
     */
    private BufferedDBIterator _innerPageIt;
    
    /**
     * Last page read for outer relation
     */
    private HeapPage _outerRecentPage = null;
    
    /**
     * Last heap page read for inner relation.
     */
    private HeapPage _innerRecentPage = null;
    
    /**
     * Tuple iterator for tuples in outer relation's page.
     */
    private Iterator<Tuple> _outerTupleIt = null;
    
    /**
     * Tuple iterator for tuples in inner relation's page.
     */
    private Iterator<Tuple> _innerTupleIt = null;
    
    /**
     * Last used tuple in outer relation.
     */
    private Tuple _outerRecent=null;
    
    /**
     * Last used tuple in inner relation.
     */
    private Tuple _innerRecent=null;
    
	public PNLJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
		super(p, child1, child2);

    	this._outerPageIt = new BufferedDBIterator(this._outerRelation);
    	this._innerPageIt = new BufferedDBIterator(this._innerRelation);
	}

	/* (non-Javadoc)
	 * @see simpledb.Join#open()
	 */
	@Override
	public void open() throws DbException, NoSuchElementException, TransactionAbortedException, IOException {
		super.open();
		
		_outerPageIt.open();
    	_innerPageIt.open();

    	_outerRecent = null;
    	_innerRecent = null;

    	_outerRecentPage = null;
        _innerRecentPage = null;
        _outerTupleIt = null;
        _innerTupleIt = null;
	}

	/* (non-Javadoc)
	 * @see simpledb.Join#close()
	 */
	@Override
	public void close() {
		super.close();
		
		this._outerPageIt.close();
    	this._innerPageIt.close();
	}

	/* (non-Javadoc)
	 * @see simpledb.Join#readNext()
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		try {
			while ((_outerRecentPage != null) || _outerPageIt.hasNext()) {
				if (_outerRecentPage == null) {
					_outerRecentPage = _outerPageIt.next();
				}
				if (_outerTupleIt == null) {
					_outerTupleIt = _outerRecentPage.iterator();
				}
				
				while ((_innerRecentPage != null) || _innerPageIt.hasNext()) {
					if (_innerRecentPage == null) {
						_innerRecentPage = _innerPageIt.next();
					}
					
					if (_innerTupleIt == null) {
						_innerTupleIt = _innerRecentPage.iterator();
					}
					
					// Iterate over tuples in outer relation's page and join them with tuples in inner relation's page.
					while (_outerTupleIt.hasNext() || _outerRecent != null) {
						if (_outerRecent == null) {
							_outerRecent = _outerTupleIt.next();
						}
						
						if (_outerRecent != null) {
							while (_innerTupleIt.hasNext()) {
								_innerRecent = _innerTupleIt.next();
								if (_innerRecent != null) {
									++_numComp;
									if (_predicate.filter(_outerRecent, _innerRecent)) {
										++_numMatches;
										return joinTuple(_outerRecent, _innerRecent, getTupleDesc());
									}
								}
							}
							
							if (_outerTupleIt.hasNext()) {
								_innerTupleIt = _innerRecentPage.iterator(); // Reset the iterator to start position
							}
							
							_outerRecent = null;
						}
					}
					
					_outerTupleIt = _outerRecentPage.iterator(); // Reset the iterator to start position
					_outerRecent = null;
					
					_innerRecentPage = null;  // To ensure getting next page (if any) in following iteration.
					_innerTupleIt = null;
				}
				
				if (_outerPageIt.hasNext()) {
					_innerPageIt.rewind();
				}

				_outerRecentPage = null;  // To ensure getting next page (if any) in following iteration.
				_outerTupleIt = null;
			}
		} catch (NoSuchElementException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
}
