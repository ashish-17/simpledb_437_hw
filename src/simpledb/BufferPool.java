package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool which check that the transaction has the appropriate locks
 * to read/write the page.
 */
public class BufferPool {
	/** Bytes per page, excluding header. */
	public static final int PAGE_SIZE = 4096;
	public static final int DEFAULT_PAGES = 100;
	public static final int DEFAULT_POLICY = 0;
	public static final int LRU_POLICY = 1;
	public static final int MRU_POLICY = 2;

	int replace_policy = DEFAULT_POLICY;
	int _numhits = 0;
	int _nummisses = 0;

	int maxPagesInBuffer;
	Map<PageId, Page> bufferedPages;
	PolicyData policyData;
	
	/**
	 * Here we store the data related to various policies and their implementation.
	 * @author ashish
	 *
	 */
	class PolicyData {
		Map<PageId, Integer> policyData = new HashMap<PageId, Integer>();
		int counter = 0;
		
		/**
		 * Update the policy maintenance data.
		 * @param pageId - Page id whose data needs to be updated.
		 */
		public void update(PageId pageId) {
			if (counter == Integer.MAX_VALUE) {
				counter = 0; // Reset the counter.
			}
			
			policyData.put(pageId, counter);
			counter++;
		}
		
		/**
		 * Remove the evicted value from policy data also.
		 * @param pageId - Page id which was evicted.
		 */
		public void remove(PageId pageId) {
			policyData.remove(pageId);
		}
		
		/**
		 * What's the next page to be evicted as per the required eviction policy.
		 * @param replacePolicy - The policy identifier.
		 * @return - The page id for the corresponding page to be evicted.
		 */
		public PageId evictInfo(int replacePolicy) {
			PageId page = null;
			switch (replacePolicy) {
			case LRU_POLICY: {
				Entry<PageId, Integer> lru = null;
				for (Entry<PageId, Integer> entry: policyData.entrySet()) {
					if ((lru == null) || (lru.getValue() > entry.getValue())) {
						lru = entry;
					}
				}
				
				page = lru.getKey();
				
				break;
			}
			
			case MRU_POLICY:
			default: {
				Entry<PageId, Integer> mru = null;
				for (Entry<PageId, Integer> entry: policyData.entrySet()) {
					if ((mru == null) || (mru.getValue() < entry.getValue())) {
						mru = entry;
					}
				}
				
				page = mru.getKey();
				
				break;
			}
			}

			return page;
		}
	}
	
	/**
	 * Constructor.
	 *
	 * @param numPages
	 *            number of pages in this buffer pool
	 */
	public BufferPool(int numPages) {
		this.maxPagesInBuffer = numPages;
		bufferedPages = new HashMap<PageId, Page>();
		policyData = new PolicyData();
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException, IOException {

		if (bufferedPages.containsKey(pid)) {
			_numhits++;
			policyData.update(pid); // Update the access counter in policy data.
			return bufferedPages.get(pid);
		} else {
			_nummisses++;

			Page page = Database.getCatalog().getDbFile(pid.tableid()).readPage(pid);
			if (page != null) {
				if (bufferedPages.size() >= maxPagesInBuffer) {
					// Evict a page if the buffer is full.
					evictPage();
				}
				
				bufferedPages.put(pid, page);
				policyData.update(pid); // Update the access counter in policy data.
				return page;
			} else {
				throw new DbException("No such table in file.");
			}
		}
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public synchronized void releasePage(TransactionId tid, PageId pid) {
		// no need to implement this

	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public synchronized void transactionComplete(TransactionId tid) throws IOException {
		// no need to implement this
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
		// no need to implement this
		return false;
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public synchronized void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// no need to implement this
	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to. May block if the
	 * lock cannot be acquired.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// no need to implement this

	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is added to. May block if the lock cannot be
	 * acquired.
	 *
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param t
	 *            the tuple to add
	 */
	public synchronized void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// no need to implement this

	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// no need to implement this
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// no need to implement this
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// no need to implement this
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// no need to implement this
	}

	/**
	 * Discards a page from the buffer pool. Return index of discarded page
	 */
	private synchronized int evictPage() throws DbException {
		PageId pageId = policyData.evictInfo(replace_policy); // Get this information from the policy class.
		bufferedPages.remove(pageId);
		policyData.remove(pageId);
		return pageId.pageno();
	}

	public int getNumHits() {
		return _numhits;
	}

	public int getNumMisses() {
		return _nummisses;
	}

	public void setReplacePolicy(int replacement) {
		this.replace_policy = replacement;
	}
}