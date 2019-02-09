package fr.snasello.datas.es;

import java.util.List;
import java.util.Objects;

/**
 * Internal use for scrolling datas
 * 
 * @author Samuel Nasello
 *
 * @param <T> the type of Object in the dataset
 */
class ScrollData<T> {
	
	private final ScrollInfo info;
	
	private final List<T> datas;
	
	public ScrollData(ScrollInfo info, List<T> datas) {
		super();
		
		Objects.requireNonNull(info);
		Objects.requireNonNull(datas);
		
		this.info = info;
		this.datas = datas;
	}

	public boolean hasNext() {
		return !datas.isEmpty();
	}

	public ScrollInfo getInfo() {
		return info;
	}

	public List<T> getDatas() {
		return datas;
	}
	
}
