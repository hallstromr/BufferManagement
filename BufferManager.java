import java.io.*;
import java.util.*;

/**
 * Buffer manager. Manages a memory-based buffer pool of pages.
 * @author Dave Musicant, with considerable material reused from the
 * UW-Madison Minibase project
 */
public class BufferManager
{
    public static class PageNotPinnedException extends RuntimeException {};
    public static class PagePinnedException extends RuntimeException {};

    /**
     * Value to use for an invalid page id.
     */
    public static final int INVALID_PAGE = -1;

    private static class FrameDescriptor
    {
        private int pageNum;
        private String fileName;
        private int pinCount;
        private boolean dirty;
	private boolean reference;
        
        public FrameDescriptor()
        {
            pageNum = INVALID_PAGE;
            pinCount = 0;
            fileName = null;
            dirty = false;
	    reference = false;
        }

    }

    // Here are some private variables to get you started. You'll
    // probably need more.
    private Page[] bufferPool;
    private FrameDescriptor[] frameTable;
    private Map<Integer, Integer> hashMap;
    private int clockHand;

    /**
     * Creates a buffer manager with the specified size.
     * @param poolSize the number of pages that the buffer pool can hold.
     */
    public BufferManager(int poolSize)
    {
	this.bufferPool = new Page[poolSize];
	this.frameTable = new FrameDescriptor[poolSize];
	for(int i = 0; i < poolSize; i++) {
	    this.bufferPool[i] = new Page();
	    this.frameTable[i] = new FrameDescriptor();
	}
	
	this.hashMap = new HashMap<Integer, Integer>();
	this.clockHand = 0;
    }

    /**
     * Returns the pool size.
     * @return the pool size.
     */
    public int poolSize()
    {
        return this.bufferPool.length;
    }

    /**
     * Checks if this page is in buffer pool. If it is, returns a
     * pointer to it. Otherwise, it finds an available frame for this
     * page, reads the page, and pins it. Writes out the old page, if
     * it is dirty, before reading.
     * @param pinPageId the page id for the page to be pinned
     * @param fileName the name of the database that contains the page
     * to be pinned
     * @param emptyPage determines if the page is known to be
     * empty. If true, then the page is not actually read from disk
     * since it is assumed to be empty.
     * @return a reference to the page in the buffer pool. If the buffer
     * pool is full, null is returned.
     * @throws IOException passed through from underlying file system.
     */
    public Page pinPage(int pinPageId, String fileName, boolean emptyPage)
        throws IOException
    {
	DBFile file = new DBFile(fileName);
	Page curPage = new Page();
	file.readPage(pinPageId, curPage);
	
	if(hashMap.get(curPage) != null) {
	    this.frameTable[hashMap.get(pinPageId)].pinCount += 1;
	    return this.bufferPool[hashMap.get(pinPageId)];
	}

	int count = 0;
	FrameDescriptor curFrame;
	while(count < 2*this.bufferPool.length) {
	    curFrame = this.frameTable[clockHand];

	    if(curFrame.pinCount < 1 && curFrame.reference == false) {
		//replace

		Page page = new Page();
		
		if(!emptyPage) {
		    file.readPage(curFrame.pageNum, page);
		}

		if(curFrame.dirty == true) {
		    this.flushPage(curFrame.pageNum, fileName);
		}
		int replaceIndex = clockHand;

		
		file.readPage(pinPageId, page);
		this.bufferPool[replaceIndex] = page;
		this.frameTable[replaceIndex].pageNum = pinPageId;
		this.frameTable[replaceIndex].fileName = fileName;
		this.frameTable[replaceIndex].pinCount = 1;
		hashMap.put(pinPageId, replaceIndex);
		count++;
		clockHand++;
		this.clockHand = clockHand%(this.bufferPool.length);
		return page;
		
	    } else if (curFrame.pinCount < 1 && curFrame.reference == true) {
		this.frameTable[clockHand].reference = false;
	    }
	    count++;
	    clockHand++;
	    clockHand = clockHand%(this.bufferPool.length);
	}
	
        return null;
    }

    /**
     * If the pin count for this page is greater than 0, it is
     * decremented. If the pin count becomes zero, it is appropriately
     * included in a group of replacement candidates.
     * @param unpinPageId the page id for the page to be unpinned
     * @param fileName the name of the database that contains the page
     * to be unpinned
     * @param dirty if false, then the page does not actually need to
     * be written back to disk.
     * @throws PageNotPinnedException if the page is not pinned, or if
     * the page id is invalid in some other way.
     * @throws IOException passed through from underlying file system.
     */
    public void unpinPage(int unpinPageId, String fileName, boolean dirty)
        throws IOException
    {
	DBFile file = new DBFile(fileName);
	int index = hashMap.get(unpinPageId);
	
	if(dirty == true) {
	    this.flushPage(unpinPageId, fileName);
	}
	this.frameTable[index].pinCount -= 1;
	this.frameTable[index].reference = true;
	
	
	
    }


    /**
     * Requests a run of pages from the underlying database, then
     * finds a frame in the buffer pool for the first page and pins
     * it. If the buffer pool is full, no new pages are allocated from
     * the database.
     * @param numPages the number of pages in the run to be allocated.
     * @param fileName the name of the database from where pages are
     * to be allocated.
     * @return an Integer containing the first page id of the run, and
     * a references to the Page which has been pinned in the buffer
     * pool. Returns null if there is not enough space in the buffer
     * pool for the first page.
     * @throws DBFile.FileFullException if there are not enough free pages.
     * @throws IOException passed through from underlying file system.
     */
    public Pair<Integer,Page> newPage(int numPages, String fileName)
        throws IOException
    {
	DBFile file = new DBFile(fileName);

	int firstPageNum = file.allocatePages(numPages);
	int lastAllocatedPageNum = firstPageNum;

	Page pinnable = new Page();
	Page curPage = new Page();

	//always try to pin first page
	if(this.frameTable[0].pageNum == INVALID_PAGE) {
	    file.readPage(lastAllocatedPageNum, curPage);
	    this.bufferPool[0] = curPage;
	    this.frameTable[0].fileName = fileName;
	    this.frameTable[0].pageNum = lastAllocatedPageNum;
	    hashMap.put(lastAllocatedPageNum, 0);

	    pinnable = this.pinPage(lastAllocatedPageNum, fileName, false);
	    
	    if(pinnable == null) {
		return null;
	    }
	}
	lastAllocatedPageNum++;
	
	for(int frameDesNum = 0; frameDesNum < numPages; frameDesNum++) {
	    if(this.frameTable[frameDesNum%this.frameTable.length].pageNum == INVALID_PAGE) {
		file.readPage(lastAllocatedPageNum, curPage);
		this.bufferPool[frameDesNum%this.frameTable.length] = curPage;
		this.frameTable[frameDesNum%this.frameTable.length].fileName = fileName;
		this.frameTable[frameDesNum%this.frameTable.length].pageNum = lastAllocatedPageNum;
		hashMap.put(lastAllocatedPageNum, frameDesNum%this.frameTable.length);
		lastAllocatedPageNum++;
	    }
	}
	return new Pair<Integer, Page>(firstPageNum, pinnable);
    }

    /**
     * Deallocates a page from the underlying database. Verifies that
     * page is not pinned.
     * @param pageId the page id to be deallocated.
     * @param fileName the name of the database from where the page is
     * to be deallocated.
     * @throws PagePinnedException if the page is pinned
     * @throws IOException passed through from underlying file system.
     */
    public void freePage(int pageId, String fileName) throws IOException
    {
    }

    /**
     * Flushes page from the buffer pool to the underlying database if
     * it is dirty. If page is not dirty, it is not flushed,
     * especially since an undirty page may hang around even after the
     * underlying database has been erased. If the page is not in the
     * buffer pool, do nothing, since the page is effectively flushed
     * already.
     * @param pageId the page id to be flushed.
     * @param fileName the name of the database where the page should
     * be flushed.
     * @throws IOException passed through from underlying file system.
     */
    public void flushPage(int pageId, String fileName) throws IOException
    {
	DBFile file = new DBFile(fileName);
	file.writePage(pageId, this.bufferPool[hashMap.get(pageId)]);
    }

    /**
     * Flushes all dirty pages from the buffer pool to the underlying
     * databases. If page is not dirty, it is not flushed, especially
     * since an undirty page may hang around even after the underlying
     * database has been erased.
     * @throws IOException passed through from underlying file system.
     */
    public void flushAllPages() throws IOException
    {
    }
        
    /**
     * Returns buffer pool location for a particular pageId. This
     * method is just used for testing purposes: it probably doesn't
     * serve a real purpose in an actual database system.
     * @param pageId the page id to be looked up.
     * @param fileName the file name to be looked up.
     * @return the frame location for the page of interested. Returns
     * -1 if the page is not in the pool.
    */
    public int findFrame(int pageId, String fileName)
    {
	FrameDescriptor curFrame = frameTable[hashMap.get(pageId)];
	if(curFrame.pageNum == pageId && curFrame.fileName == fileName) {
	    return hashMap.get(pageId);
	}
        return -1;
    }
}
