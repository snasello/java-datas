package fr.snasello.datas.es;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;

/**
 * Utility class to provide some common indexname pattern, like index by day, by month or by year.
 * 
 * @author Samuel Nasello
 *
 */
public final class IndexNameSupplierFactory {

	private IndexNameSupplierFactory() {

	}

	/**
	 * Static name, the index name is always the same.
	 * @param name the index name
	 * @return the supplier for generating index name 
	 */
	public static final Supplier<String> staticIndexNameSupplier(String name) {
		return () -> name;
	}

	/**
	 * Daily index name. the name will be prefix + yyyy.MM.dd
	 * @param prefix the prefix for the index name
	 * @return the supplier for generating index name 
	 */
	public static final Supplier<String> dailyIndexNameSupplier(String prefix) {
		return dailyIndexNameSupplier(prefix, localDateUTCSupplier());
	}

	static final Supplier<String> dailyIndexNameSupplier(String prefix, Supplier<LocalDate> dateSupplier) {
		return indexNameSupplier(prefix, "yyyy.MM.dd", dateSupplier);
	}

	/**
	 * Monthly index name. the name will be prefix + yyyy.MM
	 * @param prefix the prefix for the index name
	 * @return the supplier for generating index name 
	 */
	public static final Supplier<String> monthlyIndexNameSupplier(String prefix) {
		return monthlyIndexNameSupplier(prefix, localDateUTCSupplier());
	}

	static final Supplier<String> monthlyIndexNameSupplier(String prefix, Supplier<LocalDate> dateSupplier) {
		return indexNameSupplier(prefix, "yyyy.MM", dateSupplier);
	}

	/**
	 * Yearly index name. the name will be prefix + yyyy
	 * @param prefix the prefix for the index name
	 * @return the supplier for generating index name 
	 */
	public static final Supplier<String> yearlyIndexNameSupplier(String prefix) {
		return yearlyIndexNameSupplier(prefix, localDateUTCSupplier());
	}

	static final Supplier<String> yearlyIndexNameSupplier(String prefix, Supplier<LocalDate> dateSupplier) {
		return indexNameSupplier(prefix, "yyyy", dateSupplier);
	}

	/**
	 * Base method for creating supplier
	 * @param prefix the prefix for the index name
	 * @param pattern the date pattern
	 * @return the supplier for generating index name 
	 */
	public static final Supplier<String> indexNameSupplier(String prefix, String pattern) {
		return indexNameSupplier(prefix, pattern, localDateUTCSupplier());
	}
	
	/**
	 * Base method for creating supplier
	 * @param prefix the prefix for the index name
	 * @param pattern the date pattern
	 * @param dateSupplier a supplier to get de current date
	 * @return the supplier for generating index name 
	 */
	public static final Supplier<String> indexNameSupplier(String prefix, String pattern,
			Supplier<LocalDate> dateSupplier) {
		return () -> indexName(prefix, dateSupplier.get(), pattern);
	}

	/**
	 * A local date supplier with TimeZone UTC
	 * @return the supplier for generating the current date
	 */
	public static final Supplier<LocalDate> localDateUTCSupplier() {
		return () -> LocalDate.now(ZoneId.of("UTC"));
	}

	private static final String indexName(String prefix, LocalDate date, String pattern) {
		return prefix + date.format(DateTimeFormatter.ofPattern(pattern));
	}
}
