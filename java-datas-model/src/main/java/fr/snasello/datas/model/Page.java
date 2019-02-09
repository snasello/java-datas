package fr.snasello.datas.model;

/**
 * Define a page with a limit (size of the page) and a offset (starting index).
 * <p>
 * To construct the first page, you can use de factory method {@link #firstPageWithLimit(int)}
 * </p>
 * 
 * @author Samuel Nasello
 */
public class Page {

	public static final int FIRST_OFFSET = 0;
	
	private final int offset;
	
	private final int limit;
	
	/**
	 * Constructor.
	 * 
	 * @param offset the offset (starting index)
	 * @param limit the size of the page
	 */
	public Page(int offset, int limit) {
		super();
		this.offset = offset;
		this.limit = limit;
	}
	
	/**
	 * Construct a first Page (offset=0) with a limit.
	 * 
	 * @param limit the size of the page
	 * @return the first Page
	 */
	public static Page firstPageWithLimit(
			final int limit) {
		
		return new Page(FIRST_OFFSET, limit);
	}
	
	/**
	 * Get next page, jump offset to next page, same limit.
	 * 
	 * @return the next page
	 */
	public Page nextPage() {
		return new Page( offset + limit, limit);
	}

	/**
	 * get the offset.
	 * @return the offset
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * get the limit.
	 * @return the limit
	 */
	public int getLimit() {
		return limit;
	}

}
