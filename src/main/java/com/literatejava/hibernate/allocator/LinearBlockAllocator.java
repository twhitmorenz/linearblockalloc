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

import java.util.*;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.cfg.ObjectNameNormalizer;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.engine.TransactionHelper;
import org.hibernate.id.*;
import org.hibernate.jdbc.util.FormatStyle;
import org.hibernate.mapping.Table;
import org.hibernate.type.Type;
import org.hibernate.util.PropertiesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



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
 * @see TableHiLoGenerator
 * @author Tom Whitmore
 * @author Hibernate implementation of this algorithm includes code adapted from Hibernate sources/ Gavin King, (c) 2008 Red Hat Middleware LLC.
 */

public class LinearBlockAllocator extends TransactionHelper
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
    private static final Logger log = LoggerFactory.getLogger( LinearBlockAllocator.class);






    public void configure (Type type, Properties params, Dialect dialect) {
        ObjectNameNormalizer normalizer = (ObjectNameNormalizer) params.get( IDENTIFIER_NORMALIZER);
        
        this.tableName =        PropertiesHelper.getString( ALLOC_TABLE, params, DEFAULT_TABLE);
        this.sequenceColumn =   PropertiesHelper.getString( SEQUENCE_COLUMN, params, DEFAULT_SEQUENCE_COLUMN);
        this.allocColumn =      PropertiesHelper.getString( ALLOC_COLUMN, params, DEFAULT_ALLOC_COLUMN);
        
        // get SequenceName;    default to Entities' TableName.
        //      -
        this.sequenceName =     PropertiesHelper.getString( SEQUENCE_NAME, params, params.getProperty(PersistentIdentifierGenerator.TABLE));
        if (sequenceName == null) {
            throw new IdentifierGenerationException( "LinearBlockAllocator: '"+SEQUENCE_NAME+"' must be specified");
        }
        
        this.blockSize = PropertiesHelper.getInt( BLOCK_SIZE, params, DEFAULT_BLOCK_SIZE);
        if (blockSize < 1) {
            blockSize = 1;
        }

        
        // qualify Table Name, if necessary;
        //      --
        if (tableName.indexOf( '.') < 0) {
            String schemaName = normalizer.normalizeIdentifierQuoting( params.getProperty( SCHEMA));
            String catalogName = normalizer.normalizeIdentifierQuoting( params.getProperty( CATALOG));
            this.tableName = Table.qualify( catalogName, schemaName, tableName);
        }

        // build SQL strings;
        //      --
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

    
    
    /** allocate a Key;
     *      - 
     */
    public synchronized Serializable generate (SessionImplementor session, Object object) throws HibernateException 
    {
        if (allocNext >= allocHi) {
            if (log.isDebugEnabled())
                log.debug( "allocating id block: " + tableName + ", " + sequenceName + ": blockSize=" + blockSize);
            long allocated = ((Long) doWorkInNewTransaction( session)).longValue();
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
        //      -- use one single mutable instance 'Result Holder' instance as our Factory;
        //      -- 
        resultFactory.initialize( result);
        Number resultVal = resultFactory.makeValue();
        //
        return resultVal;
    }


    
    // ----------------------------------------------------------------------------------
    
    
    
    
    /** allocate a Block;
     *      - answers the Low (starting number) of resulting block.
     */
    @Override
    public Serializable doWorkInCurrentTransaction (Connection conn, String sql) throws SQLException {
        long result;
        int rows;
        do {
            // The loop ensures atomicity of the
            // select + update even for no transaction
            // or read committed isolation level

            sql = query;
            SQL_STATEMENT_LOGGER.logStatement( query, FormatStyle.BASIC);
            PreparedStatement qps = conn.prepareStatement( query);
            try {
                qps.setString( 1, sequenceName);
                ResultSet rs = qps.executeQuery();
                if (!rs.next()) {
                    String err = "could not read a hi value - you need to populate the table: " + tableName + ", " + sequenceName;
                    log.error( err);
                    throw new IdentifierGenerationException( err);
                }
                result = rs.getLong( 1);
                rs.close();
            } catch (SQLException sqle) {
                log.error( "could not read a hi value", sqle);
                throw sqle;
            } finally {
                qps.close();
            }

            sql = update;
            SQL_STATEMENT_LOGGER.logStatement( update, FormatStyle.BASIC);
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

    
    
    
    // ----------------------------------------------------------------------------------





    
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
     * @return 
     */
    public long getStats_TableAccessCount() {
        return statisticsTableAccessCount;
    }
    


}
