package fr.snasello.datas.model;


import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Value;

@Value
public class PageData<T> {

	private final Optional<Page> page;
	
	private final long totalSize;
	
	private final List<T> datas;
	
	public <U> PageData<U> mapDatas(
			Function<? super T, ? extends U> mapper){
		
		List<U> newDatas = datas.stream()
				.map(mapper)
				.collect(Collectors.toList());
		return new PageData<>(page, totalSize, newDatas);
	}
	
	public static <T> PageData<T> empty(
			Optional<Page> page) {
		
		return new PageData<>(page, 0L, Collections.emptyList());
	}
}
