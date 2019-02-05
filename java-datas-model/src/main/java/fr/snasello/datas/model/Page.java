package fr.snasello.datas.model;

public class Page {

	public static final int FIRST_OFFSET = 0;
	
	private final int offset;
	
	private final int limit;
	
	public Page(int offset, int limit) {
		super();
		this.offset = offset;
		this.limit = limit;
	}
	
	public static Page firstPageWithLimit(
			final int limit) {
		
		return new Page(FIRST_OFFSET, limit);
	}
	
	public Page nextPage() {
		return new Page( offset + limit, limit);
	}

	public int getOffset() {
		return offset;
	}

	public int getLimit() {
		return limit;
	}

}
