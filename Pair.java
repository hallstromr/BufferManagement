import java.util.Objects;
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

    @Override
    public int hashCode() {
	return Objects.hash(first, second);
    }

    @Override
    public boolean equals(Object o) {
	if (o instanceof Pair) {
	    Pair<X,Y> other = (Pair<X,Y>) o;
	    return (first == other.first && second == other.second);
	}
	return false;
    }

    public static void main(String args[]) {
	Pair<Integer,String> pair1 = new Pair<Integer,String>(5,"same");
	Pair<Integer,String> pair2 = new Pair<Integer,String>(5,"same");
	Pair<Integer,String> pair3 = new Pair<Integer,String>(5,"different");
	Pair<Integer,String> pair4 = new Pair<Integer,String>(6,"same");

	System.out.println("should be true: " + pair1.equals(pair2));
	System.out.println("should be false: " + pair3.equals(pair4));
	System.out.println("should be true: " + pair1.equals(pair1));

	System.out.println("hashCode() for pair 1 and pair2: " + pair1.hashCode() + " ----- " + pair2.hashCode());
    }
}
