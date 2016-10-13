/**
 * Class to hold a page's worth of data in memory.
 * @author Dave Musicant, with considerable material reused from the
 * UW-Madison Minibase project
 */
public class Page
{
    /**
     * Size of a page in bytes.
     */
    public static final int PAGESIZE = 1024;

    /**
     * Array to actually contain page data.
     */
    public byte[] data;

    public Page()
    {
        data = new byte[PAGESIZE];
    }
}
