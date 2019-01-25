package fr.snasello.datas.es;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import fr.snasello.datas.es.ScrollInfo;

public class ScrollInfoTest {

	@Test
	public void testNew() {
		ScrollInfo scrollInfo = new ScrollInfo(106, "123456");
		assertEquals(106, scrollInfo.getKeepAliveSecond());
		assertEquals("123456", scrollInfo.getScrollId());
	}
	
	@Test
	public void testWithScrollId() {
		ScrollInfo scrollInfo = new ScrollInfo(106, "123456").withScrollId("789");
		assertEquals(106, scrollInfo.getKeepAliveSecond());
		assertEquals("789", scrollInfo.getScrollId());
	}
}
