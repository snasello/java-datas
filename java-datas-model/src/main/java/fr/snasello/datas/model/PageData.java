package fr.snasello.datas.model;


import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Define a subset of a larger data set, for example from a database, which was truncate by a {@code Page}.
 * 
 * <p>the {@code Page} is Optional so it is possible to return a complete dataset with the same object</p>
 * <p>the totalSize represent the size of the complete dataset</p>
 * 
 * @param <T> the type of Object in the dataset
 * 
 * @author Samuel Nasello
 */
public class PageData<T> {

	private final Page page;
	
	private final long totalSize;
	
	private final List<T> datas;
	
	/**
	 * Constructor.
	 * @param page the page, can be null
	 * @param totalSize the totalsize of the data
	 * @param datas the datas, cannot be null
	 * @throws NullPointerException if {@code datas} is null
	 */
	public PageData(Page page, long totalSize, List<T> datas) {
		super();

		Objects.requireNonNull(datas);
		
		this.page = page;
		this.totalSize = totalSize;
		this.datas = datas;
	}

	/**
	 * get the page.
	 * @return the page
	 */
	public Optional<Page> getPage() {
		return Optional.ofNullable(page);
	}

	/**
	 * get the total size.
	 * @return the total size
	 */
	public long getTotalSize() {
		return totalSize;
	}

	/**
	 * return datas.
	 * @return the datas
	 */
	public List<T> getDatas() {
		return datas;
	}

	/**
	 * Transform current type of datas to a new type.
	 * 
	 * @param mapper A mapper
	 * @param <U> The new type
	 * @return {@code PageData} with the new type
	 */
	public <U> PageData<U> map(
			Function<? super T, ? extends U> mapper){
		
		List<U> newDatas = datas.stream()
				.map(mapper)
				.collect(Collectors.toList());
		return new PageData<>(page, totalSize, newDatas);
	}
	
	/**
	 * Return a {@code PageData} with no data.
	 * 
	 * @param page the page
	 * @param <T> the type of Object in the dataset
	 * @return an empty {@code PageData}
	 */
	public static <T> PageData<T> empty(
			Page page) {
		
		return new PageData<>(page, 0L, Collections.emptyList());
	}
}
