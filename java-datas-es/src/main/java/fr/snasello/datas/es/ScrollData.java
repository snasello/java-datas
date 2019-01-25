package fr.snasello.datas.es;

import java.util.List;

import lombok.Value;

@Value
public class ScrollData<T> {
	
	private final ScrollInfo info;
	
	private final List<T> datas;
	
	public boolean hasNext() {
		return !datas.isEmpty();
	}
}