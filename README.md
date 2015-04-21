'Linear Block Allocator' for Hibernate – a superior &amp; portable alternative to Hi/Lo
=========

This strategy is highly efficient, database-portable, and simplifies/ supersedes HiLo-type strategies. It allocates Long or Integer keys, using a portable table-based algorithm.

Keys are allocated in blocks from an "allocator" table, with a current block held in memory; used by the application as required. Default block-size of 20 outperforms SEQUENCE allocation by a factor of 10 or more, using only standard SQL with no application lock-in to SEQUENCES or proprietary features.

Block allocation is multiuser & cluster-safe; database contention is handled by retry. 

Simplicity is the Algorithm.
--------------

Allocation using the "linear block" algorithm, can be understood as allocating blocks from a linear number-line. NEXT_VAL in the database represents the start of the next block. Blocks are allocated by simply incrementing the counter in a concurrency-safe manner. Block-based allocation and portable high performance are achieved simply, without unnecessary complexities.

Compared to HiLo, "linear block allocation" treats keyspace as the linear number-line it fundamentally is, rather than breaking it into a two-dimensional keyspace (the separate "hi" & "lo" words). Keeping NEXT_VAL the same type & magnitude as the actual keys simplifies maintenance, reduces multiplication to simple addition, and removes unnecessary complexity. There is no performance advantage or any other benefit to justify HiLo's more complicated number-space & design; it is merely a flawed concept rejected by Occam's razor.

Usage & Configuration
--------

LinearBlockAllocator generator strategy is selected by class 'com.literatejava.hibernate.allocator.LinearBlockAllocator'. Parameters are all defaulted automatically -- unless you want to customize, you may not need to specify anything.

Here's an example Hibernate XML mapping:

    <class name="Customer" table="CUSTOMER">
        <id name="id" column="ID" type="int">
            <generator class="com.literatejava.hibernate.allocator.LinearBlockAllocator" />
        </id>
    </class>

You can also specify parameters to customize sequence key, block-size etc.

    <class name="OrderLine" table="ORDERLINE">
        <id name="id" column="ID" type="long">
            <generator class="com.literatejava.hibernate.allocator.LinearBlockAllocator">
                <param name="sequenceName">OrderLine</param>
                <param name="blockSize">1000</param>
            </generator>
        </id>
    </class>

Configuring with annotations:

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "CustomerIdGenerator")
    @GenericGenerator(name = "CustomerIdGenerator",
            strategy = "com.literatejava.hibernate.allocator.LinearBlockAllocator",
            parameters = {
                @Parameter(name = "sequenceName", value = "Customer")
            })

To customize LinearBlockAllocator, parameters can be considered in two groups -- those defining the allocator table, and those selecting/ controlling allocation for the specific sequence.

Allocator table definition:

- table -- allocator table to use, default "KEY_ALLOC"
- sequenceColumn -- sequence-name column, default "SEQ"
- allocColumn -- next-block column, default "NEXT_VAL"

Sequence selection:

- sequenceName -- sequence-name to use; defaults to table-name of the Hibernate entity
- blockSize -- block-size (# of keys) to cache in memory; default 100

Allocator table can contain multiple "allocation sequences", keyed by "name". Many applications can use a single allocator table to store all their sequences. Each Hibernate generator is configured independently, so shared or independent allocator tables are equally able to be configured.

Block-size is the key parameter controlling performance. Larger block-sizes increase performance, at the expense of a slight increase in unused keys lost on server restart. Increasing the block-size to 200 achieve most of the performance benefits practically possible; entities/rows must still be INSERT'ed into the database.

Human-Friendly Keys & Database Modelling
--------

LinearBlockAllocator generates better, human-friendly keys. Block-sizes default to human-readable decimal-based (20, 100 etc) sizes -- there is no forced bias towards large binary numbers! Key wastage is decreased & restarting your server twice allocates customer=200, not customer=98304 :) Imagine how much easier development will be, without having to type stupid large ugly numbers for all keys.

Database operation & maintenance are easy, too. With linear block allocation, "allocator state" and existing keys are always in direct correspondence. NEXT_VAL corresponds directly to MAX(existing keys); and must always be greater. Bulk inserts, validity checking or manual updates to the allocator are obvious & easy. 

Unlike Hi-Lo, tuning or changing block-size are possible without losing allocator position & database integrity. Since NEXT_VAL represents values directly (rather than via multiplier) changing block-size does not affect the next key to be allocated. This makes it possible for stored procedures to use the allocator table, also.


Performance & Requirements
------------

Compared with vendor-specific strategies such as Oracle SEQUENCE, "LinearBlockAllocator" can achieve anywhere upwards of 50x greater allocation performance, dependent on blocksize. This effectively doubles overall insertion performance; all achieved in a portable & SQL-standard manner.

The allocator is compatible with most common Hibernate usages/ configurations, but the 'block' design does require the ability to obtain a Session-independent connection to the database. This is possible in all configurations where connection acquisition is under Hibernate control. If Hibernate is being driven with user-supplied connections, another generation strategy should be chosen.

Hibernate 4.x Compatible
------------

Linear Block Allocator 4.2.0 is fully compatible with Hibernate 4.2, 4.3 etc. This version is also 'feature compatible' with older Hibernate 4.0 and 4.1 versions, but unit-tests require extra Maven dependencies not correctly provided by Hibernate.
