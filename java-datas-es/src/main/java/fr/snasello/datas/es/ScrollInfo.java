package fr.snasello.datas.es;

import java.util.Objects;

/**
 * Internal use for scrolling datas
 * 
 * @author Samuel Nasello
 */
class ScrollInfo {

	private final long keepAliveSecond;
	private final String scrollId;
	
	public ScrollInfo(long keepAliveSecond, String scrollId) {
		super();
		
		this.keepAliveSecond = keepAliveSecond;
		this.scrollId = scrollId;
	}
	
	public ScrollInfo withScrollId(
			final String scrollId) {
		
		Objects.requireNonNull(scrollId);
		
		return new ScrollInfo(keepAliveSecond, scrollId);
	}

	public long getKeepAliveSecond() {
		return keepAliveSecond;
	}

	public String getScrollId() {
		return scrollId;
	}
	
}
