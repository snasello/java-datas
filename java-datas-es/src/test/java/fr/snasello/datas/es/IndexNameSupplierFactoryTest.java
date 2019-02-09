package fr.snasello.datas.es;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

public class IndexNameSupplierFactoryTest {

	@Test
	public void staticIndexNameSupplier() {
		assertEquals("test-index", IndexNameSupplierFactory.staticIndexNameSupplier("test-index").get());
	}
	
	@Test
	public void dailyIndexNameSupplier() {
		assertEquals("test-index-2019.01.01", IndexNameSupplierFactory.dailyIndexNameSupplier("test-index-", () -> LocalDate.of(2019, 1, 1)).get());
		assertEquals("test-index-2019.12.31", IndexNameSupplierFactory.dailyIndexNameSupplier("test-index-", () -> LocalDate.of(2019, 12, 31)).get());
		assertNotNull(IndexNameSupplierFactory.dailyIndexNameSupplier("test-index-").get());
	}
	
	@Test
	public void monthlyIndexNameSupplier() {
		assertEquals("test-index-2019.01", IndexNameSupplierFactory.monthlyIndexNameSupplier("test-index-", () -> LocalDate.of(2019, 1, 1)).get());
		assertEquals("test-index-2019.12", IndexNameSupplierFactory.monthlyIndexNameSupplier("test-index-", () -> LocalDate.of(2019, 12, 1)).get());
		assertNotNull(IndexNameSupplierFactory.monthlyIndexNameSupplier("test-index-").get());
	}
	
	@Test
	public void yearlyIndexNameSupplier() {
		assertEquals("test-index-2019", IndexNameSupplierFactory.yearlyIndexNameSupplier("test-index-", () -> LocalDate.of(2019, 1, 1)).get());
		assertEquals("test-index-2018", IndexNameSupplierFactory.yearlyIndexNameSupplier("test-index-", () -> LocalDate.of(2018, 12, 1)).get());
		assertNotNull(IndexNameSupplierFactory.yearlyIndexNameSupplier("test-index-").get());
	}
	
	@Test
	public void indexNameSupplier() {
		assertNotNull(IndexNameSupplierFactory.indexNameSupplier("test-index-", "yyyy.MM.dd").get());

	}
}
