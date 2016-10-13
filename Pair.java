/**
 * Utility wrapper to allow one to effectively return two values from
 * a method.
 * @author Dave Musicant, with considerable material reused from the
 * UW-Madison Minibase project
 */
public class Pair<X,Y>
{ 
    public X first;
    public Y second;

    /**
     * Constructor
     * @param first the first value
     * @param second the second value
     */
    public Pair(X first, Y second)
    {
        this.first  = first;
        this.second = second;
    }
}
