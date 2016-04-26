package wordcountv1.model;

public class ReviewRecord {
	public String reviewerID;
	public String asin;
	public String reviewerName;
	public int[] helpful;
	public String reviewText;
	public float overall;
	public String summary;
	public long unixReviewTime;
	public String reviewTime;
}
