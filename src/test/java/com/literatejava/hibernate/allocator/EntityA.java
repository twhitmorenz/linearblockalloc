package com.literatejava.hibernate.allocator;

/**
 * {@inheritDoc}
 *
 * @author Tom Whitmore
 * @author Steve Ebersole
 */
public class EntityA {
	private Long id;
	private String name;

	public EntityA() {}
	public EntityA(String name) {
		this.name = name;
	}

	public Long getId() {return id;}
	public void setId(Long id) {this.id = id;}

	public String getName() {return name;}
	public void setName(String name) {this.name = name;}
	
}
