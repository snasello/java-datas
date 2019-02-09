package fr.snasello.datas.model;

import java.util.Objects;

/**
 * Define a sort on a field with a direction.
 * 
 * @author Samuel Nasello
 *
 */
public class Sort {

	private final String field;
	
	private final SortDirection direction;

	/**
	 * Constructor.
	 * @param field the field
	 * @param direction the direction
	 * @throws NullPointerException if {@code field} or {@code direction} is null
	 */
	public Sort(String field, SortDirection direction) {
		super();
		
		Objects.requireNonNull(field);
		Objects.requireNonNull(direction);
		
		this.field = field;
		this.direction = direction;
	}

	/**
	 * the field.
	 * @return the field
	 */
	public String getField() {
		return field;
	}

	/**
	 * the sort direction (ASC or DESC).
	 * @return the sort direction
	 */
	public SortDirection getDirection() {
		return direction;
	}
	
}
