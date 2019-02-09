package fr.snasello.datas.es;

/**
 * Define a object with an identifier
 * 
 * @author Samuel Nasello
 */
public interface Identifiable {

	/**
	 * get the identifier.
	 * @return the identifier
	 */
	String getId();
	
	/**
	 * set the identifier
	 * @param id the identifier
	 */
	void setId(String id);
}