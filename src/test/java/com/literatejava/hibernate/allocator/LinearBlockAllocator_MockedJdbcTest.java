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

import static junit.framework.Assert.*;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

import junit.framework.TestCase;

import org.hibernate.cfg.*;
import org.hibernate.connection.ConnectionProvider;
import org.hibernate.dialect.*;
import org.hibernate.engine.SessionFactoryImplementor;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.impl.SessionFactoryImpl;
import org.hibernate.type.IntegerType;
import org.mockito.Matchers;

import com.literatejava.hibernate.allocator.LinearBlockAllocator;

import static org.mockito.Mockito.*;



public class LinearBlockAllocator_MockedJdbcTest extends TestCase {

    
    
    protected Dialect dialect;
    protected LinearBlockAllocator alloc;
    protected SessionImplementor sessionImpl;
    protected Connection connection;


    
    @Override
    protected void setUp() throws Exception {
        this.dialect = new Oracle9iDialect();
    }

    
    
    
    protected void createAllocator() throws SQLException {

        // configure properties for an Allocator;
        //      --
        Properties params = new Properties();
        params.setProperty("sequenceName", "Customer");
        params.setProperty("blockSize", "10");
        params.put(
                PersistentIdentifierGenerator.IDENTIFIER_NORMALIZER,
                new ObjectNameNormalizer() {
                    @Override
                    protected boolean isUseQuotedIdentifiersGlobally() {
                        return false;
                    }

                    @Override
                    protected NamingStrategy getNamingStrategy() {
                        return DefaultNamingStrategy.INSTANCE;
                    }
                }
        );
        
        // create the Allocator.
        //
        this.alloc = createAllocator( IntegerType.INSTANCE, params);
    }

    
    

    protected void setupMockSession() throws SQLException {
        
        // mock SessionImpl -> SessionFactoryImpl;
        //      --
        this.sessionImpl = mock( SessionImplementor.class);
        //
        SessionFactoryImplementor sessionFactoryImpl = mock( SessionFactoryImplementor.class);
        when( sessionImpl.getFactory()).thenReturn( sessionFactoryImpl);
        
        // mock SessionFactoryImpl -> TransactionManager;
        when( sessionFactoryImpl.getTransactionManager()).thenReturn( null);
        
        // mock SessionFactoryImpl -> ConnectionProvider;
        //      --
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class);
        when( sessionFactoryImpl.getConnectionProvider()).thenReturn( connectionProvider);

        // mock ConnectionProvider -> Connection.
        //      --
        this.connection = mock( Connection.class);
        when( connectionProvider.getConnection()).thenReturn( connection);
        
        
        // done.
    }
    


    protected LinearBlockAllocator createAllocator (IntegerType returnType, Properties params) {
        LinearBlockAllocator alloc = new LinearBlockAllocator();
        alloc.configure( returnType, params, dialect);
        return alloc;
    }
    

    
    
    @Override
    protected void tearDown() throws Exception {
    }

    

    // ----------------------------------------------------------------------------------





    
    public void testConfiguration_Parameters() {
        Properties params = new Properties();
        params.setProperty("table", "TEST_ALLOC_TABLE");
        params.setProperty("sequenceColumn", "TEST_SEQ");
        params.setProperty("allocColumn", "TEST_NEXTVAL");
                      
        params.setProperty("sequenceName", "TestSeq");
        params.setProperty("blockSize", "100");           
        params.put(
                PersistentIdentifierGenerator.IDENTIFIER_NORMALIZER,
                new ObjectNameNormalizer() {
                    @Override
                    protected boolean isUseQuotedIdentifiersGlobally() {
                        return false;
                    }

                    @Override
                    protected NamingStrategy getNamingStrategy() {
                        return DefaultNamingStrategy.INSTANCE;
                    }
                }
        );
        LinearBlockAllocator configd = createAllocator( IntegerType.INSTANCE, params);
        
        // check definitions show up in SQL create strings.
        //      --
        String[] sqlCreates = configd.sqlCreateStrings( dialect);
        String sqlTable = sqlCreates[0];
        String sqlRow = sqlCreates[1];
        
        // asserts.
        //      --
        assertTrue( sqlTable.contains("TEST_ALLOC_TABLE"));
        assertTrue( sqlTable.contains("TEST_SEQ"));
        assertTrue( sqlTable.contains("TEST_NEXTVAL"));
        //
        assertTrue( sqlRow.contains("TEST_ALLOC_TABLE"));
        assertTrue( sqlRow.contains("TEST_SEQ"));
        assertTrue( sqlRow.contains("TEST_NEXTVAL"));
        assertTrue( sqlRow.contains("'TestSeq'"));
        assertTrue( sqlRow.contains("100"));
        

        // pass.
    }
    
    
    
    
    public void testGenerate() throws SQLException {
        createAllocator();
        setupMockSession();
        MockAllocatorDBAccess dbMock = new MockAllocatorDBAccess( connection);
        
        
        // allocate Key;
        //      --
        Object nominalEntity = null;
        Serializable result = alloc.generate( sessionImpl, nominalEntity);
        
        // verify interactions;
        //      --
        verify( dbMock.ps2).setLong(1, 310);
        verify( dbMock.ps2).setString(2, "Customer");
        verify( dbMock.ps2).setLong(3, 300);
        

        // pass.
    }


    
    
    
    
    
    protected static class MockAllocatorDBAccess {
        protected Connection connection;
        protected ResultSet rs1;
        protected PreparedStatement ps2;
        
        public MockAllocatorDBAccess (Connection conn) throws SQLException {
            this.connection = conn;
            setup();
        }
        
        
        public void setup() throws SQLException {
            
            // mock Connection -> prepareStatement "SELECT " -> executeQuery;
            //      --
            PreparedStatement ps1 = mock( PreparedStatement.class);
            when( connection.prepareStatement( Matchers.startsWith("select "))).thenReturn( ps1);
            //
            this.rs1 = mock( ResultSet.class);
            when( ps1.executeQuery()).thenReturn(  rs1);
            //
            when( rs1.next()).thenReturn( true, false);
            when( rs1.getLong(1)).thenReturn( 300L, 320L, 340L);
            
            
            
            // mock Connection -> prepareStatement "UPDATE " -> executeQuery;
            //      --
            this.ps2 = mock( PreparedStatement.class);
            when( connection.prepareStatement( Matchers.startsWith("update "))).thenReturn( ps2);
            //
            when( ps2.executeUpdate()).thenReturn(  1);
            
            // done.
        }
    }

    
    
    
}
