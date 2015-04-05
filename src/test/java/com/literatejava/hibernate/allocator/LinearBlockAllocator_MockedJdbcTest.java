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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.sql.*;
import java.util.Properties;

import org.hibernate.cfg.*;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.Oracle9iDialect;
import org.hibernate.engine.jdbc.spi.*;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.transaction.spi.*;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.jdbc.*;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.IntegerType;
import org.junit.*;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;



public class LinearBlockAllocator_MockedJdbcTest {

    
    
    protected Dialect dialect;
    protected LinearBlockAllocator alloc;
    protected SessionImplementor sessionImpl;
    protected Connection connection;


    
    @Before
    public void setUp() throws Exception {
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

        // QUICK OUT;
//        // mock SessionFactoryImpl -> TransactionManager;
//        when( sessionFactoryImpl.getTransactionManager()).thenReturn( null);
        
        // mock SessionImpl -> JdbcConnection Access;
        //      --
        JdbcConnectionAccess connectionAccess = mock( JdbcConnectionAccess.class);
        when( sessionImpl.getJdbcConnectionAccess()).thenReturn( connectionAccess);
//        ConnectionProvider connectionProvider = mock( ConnectionProvider.class);
//        when( sessionFactoryImpl.getConnectionProvider()).thenReturn( connectionProvider);

        // mock JdbcConnectionAccess -> Connection.
        //      --
        this.connection = mock( Connection.class);
        when( connectionAccess.obtainConnection()).thenReturn( connection);
        
        
        
        // mock 'Work in Transaction' infrastructure..
        //      eg.   long allocated = session.getTransactionCoordinator().getTransaction().createIsolationDelegate().delegateWork( 
        //                  work, true);
        //      --
        TransactionCoordinator transactionCoordinator = mock( TransactionCoordinator.class);
        when( sessionImpl.getTransactionCoordinator()).thenReturn( transactionCoordinator);
        //
        TransactionImplementor transactionImpl = mock( TransactionImplementor.class);
        when( transactionCoordinator.getTransaction()).thenReturn( transactionImpl);
        //
        IsolationDelegate isolationDelegate = mock( IsolationDelegate.class);
        when( transactionImpl.createIsolationDelegate()).thenReturn( isolationDelegate);
        //
        final WorkExecutor<Long> workExecutor  = mock( WorkExecutor.class);
        when( workExecutor.executeReturningWork( Matchers.any(ReturningWork.class), Matchers.any(Connection.class))).thenAnswer(
                new Answer<Long>() {
                    public Long answer (InvocationOnMock invocation) throws SQLException {
                        Object[] args = invocation.getArguments();
                        ReturningWork<Long> work = (ReturningWork<Long>) args[0];
                        Connection conn = (Connection) args[1];
                        return work.execute( conn);
                    }
                });
        //
        when( isolationDelegate.delegateWork( Matchers.any(WorkExecutorVisitable.class), Matchers.anyBoolean())).thenAnswer( 
                new Answer<Long>() {
                    public Long answer (InvocationOnMock invocation) throws SQLException {
                        Object[] args = invocation.getArguments();
                        WorkExecutorVisitable<Long> work = (WorkExecutorVisitable<Long>) args[0];
                        return work.accept( workExecutor, connection);      // use same Connection for testing
                    }
                });
        
        
        // mock SessionFactory -> SQL Logger;
        //      eg.   final SqlStatementLogger statementLogger = session
        //                    .getFactory()
        //                    .getServiceRegistry()
        //                    .getService( JdbcServices.class )
        //                    .getSqlStatementLogger();
        //      --
        ServiceRegistryImplementor serviceRegistryImplementor = mock(ServiceRegistryImplementor.class);
        when( sessionFactoryImpl.getServiceRegistry()).thenReturn( serviceRegistryImplementor);
        //
        JdbcServices jdbcServices = mock(JdbcServices.class);
        when( serviceRegistryImplementor.getService( JdbcServices.class)).thenReturn( jdbcServices);
        //
        SqlStatementLogger sqlStatementLogger = new SqlStatementLogger();       // actual, not a mock.
        when( jdbcServices.getSqlStatementLogger()).thenReturn( sqlStatementLogger);

                
                
        // done.
    }
    


    protected LinearBlockAllocator createAllocator (IntegerType returnType, Properties params) {
        LinearBlockAllocator alloc = new LinearBlockAllocator();
        alloc.configure( returnType, params, dialect);
        return alloc;
    }
    

    
    
    @After
    public void tearDown() throws Exception {
    }

    

    // ----------------------------------------------------------------------------------





    
    @Test
    public void testConfiguration_Parameters() {
        Properties params = new Properties();
        params.setProperty("table", "TEST_ALLOC_TABLE");
        params.setProperty("sequenceColumn", "TEST_SEQ");
        params.setProperty("allocColumn", "TEST_NEXTVAL");
                      
        params.setProperty("sequenceName", "TestSeq");
        params.setProperty("blockSize", "500");           
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
        assertTrue( sqlRow.contains("500"));
        

        // pass.
    }
    
    
    
    
    @Test
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
