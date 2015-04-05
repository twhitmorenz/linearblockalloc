package com.literatejava.hibernate.allocator;

/*
 * Linear Block Allocator;  a superior & portable ID allocator for Hibernate.
 *     http://literatejava.com/hibernate/linear-block-allocator-a-superior-alternative-to-hilo/
 * 
 * Copyright (c) 2005-2015, Tom Whitmore/ LiterateJava.com and other contributors 
 * as indicated by @author tags or express copyright attribution statements. 
 * This implementation includes sources adapted from Hibernate, copyright (c) 
 * 2008 Red Hat Middleware LLC. All third-party contributions are distributed 
 * under license.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.Session;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinearBlockAllocator_FunctionalTest extends BaseCoreFunctionalTestCase {
    
    private static final Logger log = LoggerFactory.getLogger( LinearBlockAllocator_FunctionalTest.class );

    

	
	public String[] getMappings() {
	    return new String[] { "allocator/Basic.hbm.xml" };
	}
    public String getBaseForMappings() {
        return "com/literatejava/hibernate/";
    }

	
	// ----------------------------------------------------------------------------------





	/** test Block Allocation;     
	 *         - basic functional test,  
	 *         - use a distinct entity (EntityA) to ensure allocated IDs are as expected.
	 */
	@Test
	public void testBlockAllocation() {
		EntityPersister persister = sessionFactory().getEntityPersister( EntityA.class.getName() );
		assertTrue( persister.getIdentifierGenerator() instanceof LinearBlockAllocator);
		LinearBlockAllocator generator = (LinearBlockAllocator) persister.getIdentifierGenerator();

		int count = 20;
		long START_FROM = 10;
		
		EntityA[] entities = new EntityA[count];
		Session s = openSession();
		s.beginTransaction();
		
		for ( int i = 0; i < count; i++ ) {
            String name = "entity " + (i + 1);
			entities[i] = new EntityA( name);
			s.save( entities[i] );
			long expectedId = START_FROM + i;
			assertEquals( expectedId, entities[i].getId().longValue() );
		}
		s.getTransaction().commit();

		assertEquals( 2, generator.getStats_TableAccessCount());
		
		s.beginTransaction();
		for ( int i = 0; i < count; i++ ) {
			assertEquals( i + START_FROM, entities[i].getId().intValue() );
			s.delete( entities[i] );
		}
		s.getTransaction().commit();
		s.close();
		
		// pass.
	}

    
    
    // ----------------------------------------------------------------------------------





    
    /** test Concurrent Allocation;     
     *         - concurrent functional test,  verify unique IDs allocated under concurrent use.
     *         - use a distinct entity (EntityB) to avoid affecting BasicAllocation test's expectations.
     */
    @Test
    public void testConcurrentAllocation() throws InterruptedException {
        EntityPersister persister = sessionFactory().getEntityPersister( EntityA.class.getName() );
        assertTrue( persister.getIdentifierGenerator() instanceof LinearBlockAllocator);
        LinearBlockAllocator generator = (LinearBlockAllocator) persister.getIdentifierGenerator();

        final int THREADS = 4;
        final int ENTITIES_PER_THREAD  = 1000;

        final Set<Long> unionIds = Collections.synchronizedSet( new TreeSet<Long>());
        final AtomicInteger totalCount = new AtomicInteger(0);
        

        class AllocationWork implements Runnable {
            public void run() {
                log.info("thread "+Thread.currentThread().getName()+", allocating "+ENTITIES_PER_THREAD+" entities");
                Set<Long> threadIds = performAllocationOnThread( ENTITIES_PER_THREAD);
                //
                log.info("thread "+Thread.currentThread().getName()+", finished allocation");
                unionIds.addAll( threadIds);
                totalCount.addAndGet( threadIds.size());
            }
        };

        
        // run Threaded Allocation; 
        //      --
        //
        log.info("start threaded allocation "+THREADS+" threads * "+ENTITIES_PER_THREAD+" entities");
        Thread[] threads = new Thread[ THREADS];
        for (int i = 0; i < THREADS; i++) {
            Thread thread = new Thread( new AllocationWork(), "allocation work "+i);
            threads[i] = thread;
            thread.start();
        }
        // wait for completion.
        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }
        log.info("threaded allocation complete.");

        
        // assert no conflicts.
        //      -
        int expectedTotal = THREADS * ENTITIES_PER_THREAD;
        assertEquals( expectedTotal, unionIds.size());
        assertEquals( expectedTotal, totalCount.get());
        
        
        // done.
    }

    
    
    protected Set<Long> performAllocationOnThread (int count) {
        // ENG NOTE -- don't call 'this.openSession()';  that method assigns Session thru a shared field, vulnerable to answering same Session to different threads.
        Session s = sessionFactory().openSession();
        s.beginTransaction();
        
        EntityB[] entities = new EntityB[count];
        Set<Long> allocatedIds = new TreeSet<Long>();
        
        for ( int i = 0; i < count; i++ ) {
            String name = "entity " + (i + 1);
            entities[i] = new EntityB( name);
            s.save( entities[i]);
            
            allocatedIds.add( entities[i].getId());
        }
        
        s.getTransaction().commit();
        s.close();
        
        return allocatedIds;
    }
    
	
}
