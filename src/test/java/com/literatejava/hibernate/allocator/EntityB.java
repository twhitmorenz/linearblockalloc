package com.literatejava.hibernate.allocator;

/**
 * {@inheritDoc}
 *
 * @author Tom Whitmore
 * @author Steve Ebersole
 */
public class EntityB {
	private Long id;
	private String name;

	public EntityB() {}
	public EntityB(String name) {
		this.name = name;
	}

	public Long getId() {return id;}
	public void setId(Long id) {this.id = id;}

	public String getName() {return name;}
	public void setName(String name) {this.name = name;}
	
}
