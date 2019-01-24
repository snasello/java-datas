package fr.snasello.datas.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class Page {

	public static final int FIRST_OFFSET = 0;
	
	private final int offset;
	
	private final int limit;
	
	public static Page firstPageWithLimit(
			final int limit) {
		
		return new Page(FIRST_OFFSET, limit);
	}
	
	public Page nextPage() {
		return new Page( offset + limit, limit);
	}
	
}
