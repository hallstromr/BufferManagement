import java.io.*;


public class BMTester
{

    public static interface Testable
    {
        void test(BufferManager bufMgr, String filename) throws Exception;
    }

    public static class TestFailedException extends RuntimeException
    {
        public TestFailedException(String explanation)
        {
            super(explanation);
        }
    }


    //----------------------------------------------------
    // test 1
    //      Testing pinPage, unpinPage, and whether a dirty page 
    //      is written to disk
    //----------------------------------------------------
    public static class Test1 implements Testable
    {
        public void test(BufferManager bufMgr, String filename)
            throws Exception
        {
            int first = 5;
            int last = first + bufMgr.poolSize() + 5;

            // Allocate some pages
            bufMgr.newPage(last+10,filename);
            bufMgr.unpinPage(0,filename,false);
            
            System.out.println("------- Test 1 -------");
            for (int i=first; i<=last; i++)
            {
                Page page = bufMgr.pinPage(i,filename,false);
                if (page == null)
                    throw new TestFailedException("Unable to pin page " +
                                                  "1st time");
                System.out.println("after pinPage " + i);
                byte[] data = ("This is test 1 for page " + i).getBytes();
                System.arraycopy(data,0,page.data,0,data.length);
                bufMgr.unpinPage(i,filename,true);
                System.out.println("after unpinPage " + i);
            }

            System.out.println();

            for (int i=first; i<=last; i++)
            {
                Page page = bufMgr.pinPage(i,filename,false);
                if (page == null)
                    throw new TestFailedException("Unable to pin page " +
                                                  "2nd time");
                String readBack = new String(page.data);
                String orig = "This is test 1 for page " + i;
                System.out.println("PAGE[" + i + "]: " +
                                 readBack.substring(0,orig.length()));
                if (!readBack.regionMatches(0,orig,0,orig.length()))
                    throw new TestFailedException("Page content incorrect");
                bufMgr.unpinPage(i,filename,false);
            }
        }
    }

    //-----------------------------------------------------------
    // test 2
    //      Testing replacement policy
    //------------------------------------------------------------
    public static class Test2 implements Testable
    {
        public void test(BufferManager bufMgr, String filename)
            throws Exception
        {
            System.out.println("------- Test 2 -------");

            // Allocate some pages
            bufMgr.newPage(5*bufMgr.poolSize(),filename);
            bufMgr.unpinPage(0,filename,false);

            // Pin and unpin a series of pages, the first half are loved,
            // the latter half are hated.
        
            //Pin all pages in the buffer and unpin them in reverse order
            //Clock will behave as MRU in that case
        
            int frame[] = new int[bufMgr.poolSize()];
            Page page = null;
            for (int i=0; i<bufMgr.poolSize(); i++)
            {
                page = bufMgr.pinPage(i+5,filename,false);
                if (page == null)
                    throw new TestFailedException("Unable to pin page");
                frame[i] = bufMgr.findFrame(i+5,filename);
                if (frame[i] < 0 || frame[i] >= bufMgr.poolSize())
                    throw new TestFailedException("Invalid frame returned");

                System.out.println("Page " + (i+5) +" at frame " + frame[i] +
                                   " is pinned.");
            }
        
            //Try pinning an extra page
            page = bufMgr.pinPage(bufMgr.poolSize()+6,filename,false);
            if (page != null)
                throw new TestFailedException("Pinned page in full buffer");

            //Start unpinning pages
            for (int i=bufMgr.poolSize()-1; i>=0 ;i--)
            {
                bufMgr.unpinPage(i+5,filename,true);
                System.out.println("Page " + (i+5) +" at frame " + frame[i] +
                                   " is unpinned.");
            }

            //Start pinning a new set of pages again.  The page frames
            //should be exactly the same order as the previous one
            //Clock in that case will resemble MRU
            for (int i=bufMgr.poolSize(); i < 2*bufMgr.poolSize(); i++){
                page = bufMgr.pinPage(i+5,filename,false);
                if (page == null)
                    throw new TestFailedException("Unable to pin page");

                int spot = bufMgr.findFrame(i+5,filename);
                System.out.println("Page " + (i+5) + " pinned in frame "
                                   + spot);

                if (spot != frame[i-bufMgr.poolSize()])
                    throw new TestFailedException("Frame number incorrect");
            }

            //Unpin half the pages in order
            for (int i=bufMgr.poolSize(); i < 2*bufMgr.poolSize(); i+=2)
            {
                bufMgr.unpinPage(i+5,filename,true);
                System.out.println("Page " + (i+5) +" at frame " +
                    frame[i-bufMgr.poolSize()] + " is unpinned.");
            }

            //Now, pin a new set of pages
            //Again, it should resemble the previous sequence
            //In this case, Clock behaves as LRU
            for (int i=2*bufMgr.poolSize(); i < 3*bufMgr.poolSize(); i+=2)
            {
                page = bufMgr.pinPage(i+5,filename,false);
                if (page == null)
                    throw new TestFailedException("Unable to pin page");
                int spot = bufMgr.findFrame(i+5,filename);
                bufMgr.unpinPage(i+5,filename,true);
                bufMgr.unpinPage(i-bufMgr.poolSize()+6,filename,true);
                System.out.println("Page "+(i+5)+" pinned in frame " + spot);
                if (spot != frame[i-2*bufMgr.poolSize()])
                    throw new TestFailedException("Frame number incorrect");
            }
        }
    }


    public static final String FILENAME = "__testing";
    public static final int NUMBUF = 20;


    public static boolean runTest(Testable testObj)
    {
        boolean success = true;
        DBFile dbfile = null;
        try
        {
            dbfile = new DBFile(FILENAME,NUMBUF+500);
            BufferManager bufMgr = new BufferManager(NUMBUF);
            testObj.test(bufMgr,FILENAME);
        }
        catch (Exception e)
        {
            success = false;
            e.printStackTrace();
        }

        DBFile.erase(FILENAME);

        return success;
    }


    public static void main(String[] args)
    {
        System.out.println("Running buffer manager tests.");

        DBFile.erase(FILENAME);
        
        // Run the tests.
        runTest(new Test1());
        runTest(new Test2());
        
        // Clean up
        DBFile.erase(FILENAME);
    }
}
