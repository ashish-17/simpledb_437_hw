/**
 * 
 */
package simpledb;

import java.io.IOException;

/**
 * @author ashish
 *
 */
public abstract class AbstractJoin extends AbstractDbIterator {

    protected JoinPredicate _predicate;
    protected DbIterator _outerRelation;
    protected DbIterator _innerRelation;

    protected int _numMatches =0;
    protected int _numComp=0;
    
    /**
     * Constructor.  Accepts to children to join and the predicate
     * to join them on
     *
     * @param p The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public AbstractJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
    	this._predicate = p;
    	this._outerRelation = child1;
    	this._innerRelation = child2;
    }
    
	/* (non-Javadoc)
	 * @see simpledb.DbIterator#open()
	 */
	@Override
	public void open() throws DbException, TransactionAbortedException, IOException {
    	_numMatches = 0;
    	_numComp = 0;
	}

	/* (non-Javadoc)
	 * @see simpledb.DbIterator#rewind()
	 */
	@Override
	public void rewind() throws DbException, TransactionAbortedException, IOException {
		close();
		open();
	}

	/* (non-Javadoc)
	 * @see simpledb.DbIterator#getTupleDesc() See simpledb.TupleDesc#combine(TupleDesc, TupleDesc) for possible implementation logic
	 */
	@Override
	public TupleDesc getTupleDesc() {
		return TupleDesc.combine(_outerRelation.getTupleDesc(), _innerRelation.getTupleDesc());
	}

	public int getNumMatches() {
		return _numMatches;
	}

	public int getNumComp() {
		return _numComp;
	}

	protected Tuple joinTuple(Tuple outer, Tuple inner, TupleDesc tupledesc){
    	Tuple combinedTuple = new Tuple(tupledesc);
    	for (int i = 0; i < tupledesc.numFields(); ++i) {
    		if (i < outer.getTupleDesc().numFields()) {
    			combinedTuple.setField(i, outer.getField(i));
    		} else {
    			combinedTuple.setField(i, inner.getField(i - outer.getTupleDesc().numFields()));
    		}
    	}
    	return combinedTuple;
    }
}
