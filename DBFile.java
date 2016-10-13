import java.io.*;

/**
 * Low level database file. This abstraction allows the user to treat
 * a database as a collection of pages.
 * @author Dave Musicant, with considerable material reused from the
 * UW-Madison Minibase project
 */
public class DBFile
{
    public static class NonPositiveRunSizeException
        extends RuntimeException {};
    public static class FileFullException extends RuntimeException {};
    public static class BadPageNumberException
        extends RuntimeException {};
    public static class EmptyFileException extends RuntimeException {};
    public static class PageNotAllocatedException extends RuntimeException {};

    private String dataFileName;
    private String mapFileName;
    private int numPages;
    
    /**
     * Creates a database with the specified number of pages. The
     * number of pages in the database can never be increased.
     * @param name name to be given to database.
     * @param numPages maximum number of pages in database.
     * @throws IOException passed through from underlying filesystem.
     */
    public DBFile(String name, int numPages) throws IOException
    {
        // If numPages is too small, just create it with at least two pages
        if (numPages < 2)
            numPages = 2;

        // Create the file
        dataFileName = name;
        RandomAccessFile dataFile = new RandomAccessFile(dataFileName,"rw");

        // Make the file num_pages pages long.
        dataFile.setLength(numPages * Page.PAGESIZE);
        byte[] zzeros = new byte[numPages * Page.PAGESIZE];
        dataFile.write(zzeros);
        dataFile.close();

        // Create a separate space map for each file.
        mapFileName = name + ".map";
        RandomAccessFile mapFile = new RandomAccessFile(mapFileName,"rw");

        // Allocate one byte for each page in the data file.
        mapFile.setLength(numPages); 

        // Fill in map pages with zeros.
        byte[] zeros = new byte[numPages];
        mapFile.write(zeros);
        mapFile.close();

        this.numPages = numPages;
    }

    /**
     * Opens the database with the given name.
     * @param name name of the database.
     * @throws IOException passed through from underlying file system.
     */
    public DBFile(String name) throws IOException
    {
        // Open the file
        dataFileName = name;
        if ((new File(dataFileName)).exists())
        {
            mapFileName = name + ".map";
            RandomAccessFile mapFile = new RandomAccessFile(mapFileName,"rw");
            numPages = (int)(mapFile.length());
            mapFile.close();
        }
        else
            numPages = 0;
    }

    /**
     * Erases the database entirely from the filesystem. Dangerous to
     * do if still have a DBFile object that refers to this file. 
     * @param name name of the database.
     * @return true if operation succeeded.
     */
    public static boolean erase(String name)
    {
        boolean success;
        success = (new File(name)).delete();
        if (success)
            success = (new File(name + ".map")).delete();
        return success;
    }

    /**
     * Allocates a set of pages.
     * @param runSize number of pages to be allocated in the run.
     * @return page number of the first page of the allocated run.
     * @throws NonPositiveRunSizeException if the run size is less
     * than or equal to zero.
     * @throws FileFullException if there are not enough free pages.
     * @throws IOException passed through from underlying file system.
     */
    public int allocatePages(int runSize) throws IOException
    {
        if (runSize <= 0)
            throw new NonPositiveRunSizeException();

        // Load the space map into memory to look for runs of the
        // necessary size. Technically, should do this a page at a
        // time (since might not have enough memory). Going with a
        // simpler approach here for expediency.
        RandomAccessFile mapFile = new RandomAccessFile(mapFileName,"rw");
        mapFile.seek(0);
        byte[] map = new byte[numPages];
        mapFile.readFully(map);

        // Loop over run starting positions
        for (int i=0; i < mapFile.length() - (runSize-1); i++)
        {
            // Loop over entire possible run: give up if any spot in
            // the possible run already has a 1 (page is taken).
            int currentRunSize = 0;
            for (int j=i; j < i + runSize; j++)
            {
                if (map[j] == 1)
                    break;
                else
                    currentRunSize++;
            }

            // Found a run.
            if (currentRunSize == runSize)
            {
                // Indicate pages are now used
                byte[] mapUpdate = new byte[runSize];
                for (int j=0; j < runSize; j++)
                    mapUpdate[j] = 1;
                mapFile.seek(i);
                mapFile.write(mapUpdate);
                mapFile.close();
                return i;
            }
        }

        // If made it to here, then no run was found.
        mapFile.close();
        throw new FileFullException();
    }

    /**
     * Deallocates a set of pages. Does not ensure that the pages
     * being deallocated are in fact allocated to begin with. If the
     * pages were already deallocated, they remain so.
     * @param startPageNum page number at the beginning of the run to
     * be deallocated.
     * @param runSize number of pages to deallocate.
     * @throws NonPositiveRunSizeException if the run size is less
     * than or equal to zero.
     * @throws BadPageNumberException if startPageNum is illegal.
     * @throws IOException passed through from underlying file system.
     */
    public void deallocatePages(int startPageNum, int runSize)
        throws IOException
    {
        if (runSize <= 0)
            throw new NonPositiveRunSizeException();

        if (startPageNum < 0 || startPageNum > numPages-1 ||
            startPageNum + runSize - 1 > numPages-1)
            throw new BadPageNumberException();

        byte[] mapUpdate = new byte[runSize];
        for (int i=0; i < runSize; i++)
            mapUpdate[i] = 0;
        RandomAccessFile mapFile = new RandomAccessFile(mapFileName,"rw");
        mapFile.seek(startPageNum);
        mapFile.write(mapUpdate);
        mapFile.close();
    }

    /**
     * Reads the contents of the specified page from disk into the
     * page object provided.
     * @param pageNum the page number to be read.
     * @param page a reference to an already allocated Page object.
     * @throws BadPageNumberException if pageNum is not in the file.
     * @throws IOException passed through from underlying file system.
     * @throws PageNotAllocatedException if pageNum is not allocaated.
     */
    public void readPage(int pageNum, Page page) throws IOException
    {
        if (pageNum < 0 || pageNum > numPages-1)
            throw new BadPageNumberException();

        // Make sure that page has actually been allocated
        RandomAccessFile mapFile = new RandomAccessFile(mapFileName,"r");
        mapFile.seek(pageNum);
        byte[] map = new byte[1];
        mapFile.readFully(map);
        if (map[0] == 0)
            throw new PageNotAllocatedException();
        mapFile.close();

        // Read the actual page from the file
        RandomAccessFile dataFile = new RandomAccessFile(dataFileName,"r");
        dataFile.seek(pageNum * Page.PAGESIZE);
        dataFile.readFully(page.data);
        dataFile.close();
    }

    /**
     * Writes the contents of the specified page to disk.
     * @param pageNum the page number to be written.
     * @param page a Page object with data to be written.
     * @throws EmptyFileException() if the file has no pages within it.
     * @throws BadPageNumberException if pageNum is not in the file.
     * @throws IOException passed through from underlying file system.
     * @throws PageNotAllocatedException if pageNum is not allocaated.
     */
    public void writePage(int pageNum, Page page) throws IOException
    {
        if (numPages == 0)
            throw new EmptyFileException();

        if (pageNum < 0 || pageNum > numPages-1)
            throw new BadPageNumberException();

        // Make sure that page has actually been allocated
        RandomAccessFile mapFile = new RandomAccessFile(mapFileName,"r");
        mapFile.seek(pageNum);
        byte[] map = new byte[1];
        mapFile.readFully(map);
        if (map[0] == 0)
            throw new PageNotAllocatedException();
        mapFile.close();

        RandomAccessFile dataFile = new RandomAccessFile(dataFileName,"rw");
        dataFile.seek(pageNum * Page.PAGESIZE);
        dataFile.write(page.data);
        dataFile.close();
    }

    // Stub for testing.
    public static void main(String[] args) throws IOException
    {
        DBFile file = new DBFile("testing",5);

        file = new DBFile("testing");

        System.out.println(file.allocatePages(4));
        file.deallocatePages(1,2);
        System.out.println(file.allocatePages(2));
        System.out.println(file.allocatePages(1));

        Page page = new Page();
        page.data[1] = 100;
        page.data[2] = 18;
        file.writePage(2,page);
        file.readPage(0,page);
        for (int i=0; i < 100; i++)
            System.out.print(page.data[i] + " ");
        System.out.println();
        file.readPage(2,page);
        for (int i=0; i < 100; i++)
            System.out.print(page.data[i] + " ");
        System.out.println();

        DBFile file2 = new DBFile("testagain",7);
        System.out.println(file2.allocatePages(3));
        file2.writePage(2,page);

        file2.readPage(1,page);
        for (int i=0; i < 100; i++)
            System.out.print(page.data[i] + " ");
        System.out.println();
        file2.readPage(2,page);
        for (int i=0; i < 100; i++)
            System.out.print(page.data[i] + " ");
        System.out.println();

        file.readPage(0,page);
        for (int i=0; i < 100; i++)
            System.out.print(page.data[i] + " ");
        System.out.println();
        file.readPage(2,page);
        for (int i=0; i < 100; i++)
            System.out.print(page.data[i] + " ");
        System.out.println();


        try {
            file2.readPage(3,page);
        }
        catch (PageNotAllocatedException e) {
            System.out.println("Correctly caught unallocated page read");
        }

        try {
            file2.writePage(3,page);
        }
        catch (PageNotAllocatedException e) {
            System.out.println("Correctly caught unallocated page write");
        }

        System.out.println(DBFile.erase("testing"));
        System.out.println(DBFile.erase("testagain"));
    }
}
