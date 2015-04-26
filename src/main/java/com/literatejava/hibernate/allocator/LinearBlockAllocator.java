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

import java.io.Serializable;
import java.sql.*;
import java.util.Properties;

import org.hibernate.*;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.naming.ObjectNameNormalizer;
import org.hibernate.boot.model.relational.*;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.engine.spi.SessionEventListenerManager;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.id.*;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.jdbc.AbstractReturningWork;
import org.hibernate.mapping.*;
import org.hibernate.type.*;
import org.jboss.logging.Logger;



/**
 * Allocate Long or Integer keys, using a portable table-based algorithm. 
 * <ul>
 *      <li>This strategy is highly efficient, database-portable, and simplifies/ supersedes HiLo-type strategies.
 *      <li>Keys are allocated in blocks from an "allocator" table, with a current block held in memory;  used by the application as required.
 *      <li>Default block-size of 20 outperforms SEQUENCE allocation by a factor of 10 or more, using only standard SQL &amp; fully portable
 *              between databases.
 *      <li>Block allocation is multiuser &amp; cluster-safe;  database contention is handled by retry.
 *      <li>This allocator requires the ability to obtain a Session-independent connection to the database. This is
 *          possible under most common configurations where connection acquisition is under Hibernate control. If
 *          Hibernate is being driven with user-supplied connections, another generation strategy should be chosen.
 *          
 * </ul>
 * 
 * Allocation using the "linear block" algorithm, can be understood as allocating blocks from a linear number-line. NEXT_VAL in the database
 * represents the start of the next block. Blocks are allocated by simply incrementing the counter in a concurrency-safe manner. Block-based 
 * allocation and portable high performance are achieved simply, without unnecessary complexities.
 * <p>
 * 
 * Compared to HiLo, "linear block allocation" treats keyspace as the linear number-line it fundamentally is, rather than breaking
 * it into a two-dimensional keyspace (the separate "hi" &amp; "lo" words). Keeping NEXT_VAL the same type &amp; magnitude as the actual keys
 * simplifies maintenance, reduces multiplication to simple addition, and removes unnecessary complexity. There is no performance advantage or 
 * any other benefit to justify HiLo's more complicated number-space &amp; design; it is merely a flawed concept rejected by Occam's razor.
 * <p>
 * 
 * Database operation &amp; maintenance are easy. With linear block allocation, "allocator state" and existing keys are always in direct correspondence.
 * NEXT_VAL corresponds directly to MAX(existing keys); and must always be greater. Bulk inserts, validity checking or manual updates to the allocator 
 * are obvious &amp; easy. Unlike Hi-Lo, tuning or changing block-size are possible without losing allocator position &amp; database integrity. 
 * Since NEXT_VAL represents values directly (rather than via multiplier) changing block-size does not affect the next key to be allocated.
 * <p>
 * 
 * LinearBlockAllocator also generates better human-friendly keys. Block-sizes default to human-readable decimal-based (20, 100 etc) sizes --
 * there is no forced bias towards large binary numbers! Key wastage is decreased &amp; restarting your server twice allocates customer=60, not customer=98304 :)
 * Imagine how much easier development will be, without having to type stupid large ugly numbers for all keys.
 * <p>
 * 
 * LinearBlockAllocator is configured with two groups of parameters -- those defining the allocator table, and those selecting/controlling allocation for the specific sequence.
 * <p>
 * 
 * Allocator table definition:
 * <ul>
 * <li>table -- allocator table to use, default "KEY_ALLOC"
 * <li>sequenceColumn -- sequence-name column, default "SEQ"
 * <li>allocColumn -- next-block column, default "NEXT_VAL "
 * </ul>
 * Sequence selection:
 * <ul>
 * <li>sequenceName -- sequence-name to use; defaults to table-name of the Hibernate entity
 * <li>blockSize -- block-size (# of keys) to cache in memory; default 20
 * </ul>
 * 
 * Allocator table can contain multiple "allocation sequences", keyed by "name". Many applications can use a single allocator table to store
 * all their sequences. Each Hibernate generator is configured independently, so shared or independent allocator tables are equally able to 
 * be configured.
 * <p>
 * 
 * Block-size is the key parameter controlling performance. Larger block-sizes increase performance, at the expense of a slight increase in unused keys
 * lost on server restart. Increasing the block-size to 200 achieve most of the performance benefits practically possible; entities/rows must still be 
 * INSERT'ed into the database.
 * <p>
 * 
 * Compared with vendor-specific strategies such as Oracle SEQUENCE, "LinearBlockAllocator" approaches double the performance (half the number of database
 * operations per INSERT) in a completely portable &amp; SQL-standard manner.
 * <p>
 * 
 * 
 * @see MultipleHiLoPerTableGenerator
 * @author Tom Whitmore
 * @author Hibernate implementation of this algorithm includes code adapted from Hibernate sources/ Gavin King, (c) 2008 Red Hat Middleware LLC.
 */

public class LinearBlockAllocator 
/*extends TransactionHelper*/
implements PersistentIdentifierGenerator, Configurable 
{


    /* COLUMN and TABLE should be renamed but it would break the public API */
    public static final String ALLOC_TABLE =        "table";
    public static final String SEQUENCE_COLUMN =    "sequenceColumn";
    public static final String ALLOC_COLUMN =       "allocColumn";
    //
    public static final String SEQUENCE_NAME =      "sequenceName";
    public static final String BLOCK_SIZE =         "blockSize";

    /** default Table &amp; Column names */
    public static final String DEFAULT_TABLE =              "KEY_ALLOC";
    public static final String DEFAULT_SEQUENCE_COLUMN =    "SEQ";
    public static final String DEFAULT_ALLOC_COLUMN =       "NEXT_VAL";
    public static final int    DEFAULT_BLOCK_SIZE =         100;
    /** internal defaults */
    private static final int DEFAULT_SEQUENCE_COLUMNLENGTH = 128;




    // - DB configuration
    protected String    tableName;
    protected String    sequenceColumn;
    protected String    allocColumn;
    protected String    sequenceName;
    protected int       blockSize;
    // - identifier/ allocator Type.
    protected Type      returnType;

    // - compiled
    protected Dialect dialect;
    protected QualifiedName qualifiedTableName;
    protected String query;
    protected String update;
    protected Class returnClass;

    // - current allocation state;
    //      ENG NOTE -- uses 'long' internally for efficiency,  allocation of super-big keys would require IntegralDataTypeHolder for these fields instead.
    protected long allocNext;
    protected long allocHi;
    protected IntegralDataTypeHolder resultFactory;

    // - statistics;  mainly for unit testing.
    protected long statisticsTableAccessCount;

    // -
    private static final Logger log = Logger.getLogger( LinearBlockAllocator.class);





    @Override
    public void configure (Type type, Properties params, JdbcEnvironment jdbcEnv) {
        this.dialect = jdbcEnv.getDialect();
        ObjectNameNormalizer normalizer = (ObjectNameNormalizer) params.get( IDENTIFIER_NORMALIZER);

        this.tableName =        ConfigurationHelper.getString( ALLOC_TABLE, params, DEFAULT_TABLE);
        this.sequenceColumn =   ConfigurationHelper.getString( SEQUENCE_COLUMN, params, DEFAULT_SEQUENCE_COLUMN);
        this.allocColumn =      ConfigurationHelper.getString( ALLOC_COLUMN, params, DEFAULT_ALLOC_COLUMN);

        // get SequenceName;    default to Entities' TableName.
        //      -
        this.sequenceName =     ConfigurationHelper.getString( SEQUENCE_NAME, params, params.getProperty(PersistentIdentifierGenerator.TABLE));
        if (sequenceName == null) {
            throw new IdentifierGenerationException( "LinearBlockAllocator: '"+SEQUENCE_NAME+"' must be specified");
        }

        this.blockSize = ConfigurationHelper.getInt( BLOCK_SIZE, params, DEFAULT_BLOCK_SIZE);
        if (blockSize < 1) {
            blockSize = 1;
        }


        // determine TableName;  qualified & dialect-specific, if necessary.
        //      --
        this.qualifiedTableName = QualifiedNameParser.INSTANCE.parse(
                tableName, 
                normalizer.normalizeIdentifierQuoting( params.getProperty( CATALOG )),
                normalizer.normalizeIdentifierQuoting( params.getProperty( SCHEMA ))
        );
        this.tableName = jdbcEnv.getQualifiedObjectNameFormatter().format( qualifiedTableName, dialect);

        // build SQL strings;
        //      -- use appendLockHint(LockMode) for now, as that is how Hibernate's generators do it.
        //
        this.query = "select " + allocColumn + 
                " from " + dialect.appendLockHint( LockMode.PESSIMISTIC_WRITE, tableName) + 
                " where " + sequenceColumn + " = ?" + 
                dialect.getForUpdateString();
        this.update = "update " + tableName + 
                " set " + allocColumn + " = ? where " + sequenceColumn + " = ? and " + allocColumn + " = ?";


        // setup Return Type & Result Holder.
        //      --
        this.returnType = type;
        this.returnClass = type.getReturnedClass();
        this.resultFactory = IdentifierGeneratorHelper.getIntegralDataTypeHolder( returnClass);


        // done.
    }

    
    
    @Override
    public void registerExportables (Database database) {
        final Dialect dialect = database.getJdbcEnvironment().getDialect();

        final Schema schema = database.locateSchema(
                qualifiedTableName.getCatalogName(),
                qualifiedTableName.getSchemaName()
        );
        final Table table = schema.createTable( qualifiedTableName.getObjectName(), false);

        final Column segmentColumn = new ExportableColumn(
                database,
                table,
                sequenceColumn,
                StringType.INSTANCE,
                dialect.getTypeName( Types.VARCHAR, DEFAULT_SEQUENCE_COLUMNLENGTH, 0, 0)
        );
        segmentColumn.setNullable( false);
        table.addColumn( segmentColumn);

        // REVIEW -- awkward Hibernate API to export Primary Key,  may be subject to change..
        table.setPrimaryKey( new PrimaryKey());
        table.getPrimaryKey().setTable( table);
        table.getPrimaryKey().addColumn( segmentColumn);

        final Column valueColumn = new ExportableColumn(
                database,
                table,
                allocColumn,
                LongType.INSTANCE
        );
        table.addColumn( valueColumn);
    }
    

    // ----------------------------------------------------------------------------------




    /** allocate a Key;
     *      - 
     */
    public synchronized Serializable generate (SessionImplementor session, Object object) throws HibernateException 
    {
        if (allocNext >= allocHi) {
            if (log.isDebugEnabled())
                log.debug( "allocating id block: " + tableName + ", " + sequenceName + ": blockSize=" + blockSize);

            long allocated = allocateBlock( session);
            //
            this.allocNext = allocated;
            this.allocHi = allocated + blockSize;
            if (log.isDebugEnabled())
                log.debug( "  allocated block: " + allocNext + "-" + allocHi);
        }
        long result = allocNext++;
        if (log.isDebugEnabled()) {
            log.debug( "allocated id: " + result);
        }

        // answer Result, as correct type;
        //      -- use one single mutable instance 'Result Holder' instance as our Factory.
        //      
        resultFactory.initialize( result);
        Number resultVal = resultFactory.makeValue();
        //
        return resultVal;
    }




    protected long allocateBlock (final SessionImplementor session) {
        AbstractReturningWork<Long> work = new AbstractReturningWork<Long>() {
            @Override
            public Long execute (Connection conn) throws SQLException {
                final SqlStatementLogger statementLogger = session
                        .getFactory()
                        .getServiceRegistry()
                        .getService( JdbcServices.class )
                        .getSqlStatementLogger();

                long result;
                int rows;
                do {
                    // The loop ensures atomicity of the
                    // select + update even for no transaction
                    // or read committed isolation level

                    statementLogger.logStatement( query, FormatStyle.BASIC.getFormatter());
                    PreparedStatement qps = conn.prepareStatement( query);
                    try {
                        qps.setString( 1, sequenceName);
                        ResultSet rs = qps.executeQuery();
                        if (rs.next()) {
                            // Read Value;
                            result = rs.getLong( 1);
                        } else {
                            // Initialize;  no existing value present.
                            //      -- previously used to do this in the "Create Strings", but Hibernate has removed that capability. duh.
                            result = initializeAllocatorValue( session, conn, statementLogger);
                        }
                        rs.close();
                    } catch (SQLException sqle) {
                        log.error( "could not read/ or initialize a hi value", sqle);
                        throw sqle;
                    } finally {
                        qps.close();
                    }

                    statementLogger.logStatement( update, FormatStyle.BASIC.getFormatter());
                    PreparedStatement ups = conn.prepareStatement( update);
                    try {
                        ups.setLong( 1, result + blockSize);
                        ups.setString( 2, sequenceName);
                        ups.setLong( 3, result);
                        rows = ups.executeUpdate();
                    } catch (SQLException sqle) {
                        log.error( "could not update hi value in: " + tableName, sqle);
                        throw sqle;
                    } finally {
                        ups.close();
                    }
                } while (rows == 0);


                // success;
                //      -- allocated a Block.
                //
                statisticsTableAccessCount++;
                return new Long( result);
            }
        };

        // perform in an isolated Transaction.
        long allocated = session.getTransactionCoordinator().getTransaction().createIsolationDelegate().delegateWork( 
                work, true);
        return allocated;
    }


    
    // initialize Allocator Value;      
    //      -- called when no existing Value found;
    //      -- insert (SequenceName, StartValue) into Allocator Table.
    //
    protected long initializeAllocatorValue (SessionImplementor session, Connection conn, SqlStatementLogger statementLogger) throws SQLException {
        SessionEventListenerManager statsCollector = session.getEventListenerManager();
        
        String insertSql = "insert into " + tableName + "("+dialect.quote(sequenceColumn)+","+dialect.quote(allocColumn)+") values (?, ?)";
        long startValue = blockSize;

        // prepare statement;
        //
        PreparedStatement insertPS;
        statementLogger.logStatement( insertSql, FormatStyle.BASIC.getFormatter());
        try {
            statsCollector.jdbcPrepareStatementStart();
            insertPS = conn.prepareStatement( insertSql);
        } finally {
            statsCollector.jdbcPrepareStatementEnd();
        }
        
        // execute statement;
        //      --
        try {
            insertPS.setString( 1, sequenceName);
            insertPS.setLong( 2, startValue);
            try {
                statsCollector.jdbcExecuteStatementStart();
                insertPS.executeUpdate();
            } finally {
                statsCollector.jdbcExecuteStatementEnd();
            }
        } finally {
            insertPS.close();
        }
        
        // done;  return Start Value.
        return startValue;
    }


    // ----------------------------------------------------------------------------------





    // OUTGOING -- obsolete;  Hibernate 5 no longer uses these SPIs/ or allows Allocator Rows to be created statically.  sigh.
    @Deprecated
    public String[] sqlCreateStrings (Dialect dialect) throws HibernateException 
    {
        // create Sequence Table
        //      SPEC - always output, skips with error if already exist in DB
        String tableStr = "create table " + dialect.quote(tableName) + " (" + 
                dialect.quote(sequenceColumn) + " " + dialect.getTypeName( Types.VARCHAR, DEFAULT_SEQUENCE_COLUMNLENGTH, 0, 0) + ", " + 
                dialect.quote(allocColumn) + " " + dialect.getTypeName( Types.BIGINT) + ", " + 
                "primary key ("+dialect.quote(sequenceColumn)+")" +
                ")";

        // create Row for this Sequence.
        String valueStr = "insert into " + tableName + "("+dialect.quote(sequenceColumn)+","+dialect.quote(allocColumn)+") values ( '" + sequenceName + "', "+blockSize+" )";

        // done.
        return new String[]{ tableStr, valueStr};
    }

    @Deprecated
    public String[] sqlDropStrings (Dialect dialect) {

        // delete Row for this Sequence.
        //
        String dropSequence = "delete from " + tableName + " where " + sequenceColumn + " = '" + sequenceName + "'";
        return new String[]{ dropSequence};
    }


    public Object generatorKey() {
        return "LinearBlockAllocator table="+tableName+", seq="+sequenceName;
    }




    // ----------------------------------------------------------------------------------






    /** Statistics;  Table Access Count.
     * @return number of table accesses
     */
    public long getStats_TableAccessCount() {
        return statisticsTableAccessCount;
    }





}
