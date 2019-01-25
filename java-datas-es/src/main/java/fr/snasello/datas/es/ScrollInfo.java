package fr.snasello.datas.es;

import lombok.Value;

@Value
public class ScrollInfo {

	private final long keepAliveSecond;
	private final String scrollId;
	
	public ScrollInfo withScrollId(
			final String scrollId) {
		
		return new ScrollInfo(keepAliveSecond, scrollId);
	}
}
