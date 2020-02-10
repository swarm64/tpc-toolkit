{% set nodes = nodes|int %}
{% set partitions = partitions|int %}
{% set node = node|int %}

CREATE EXTENSION swarm64da;
CREATE EXTENSION int_hash;

CREATE FUNCTION hash_swarm64_bigint(bigint, bigint) RETURNS bigint AS 'SELECT abs(hash($1))' LANGUAGE SQL IMMUTABLE STRICT;
CREATE OPERATOR CLASS swarm64_hash_op_class_bigint FOR TYPE bigint USING hash AS FUNCTION 2 hash_swarm64_bigint(bigint, bigint);

CREATE FUNCTION hash_swarm64_int(int, bigint) RETURNS bigint AS 'SELECT abs(hash($1))' LANGUAGE SQL IMMUTABLE STRICT;
CREATE OPERATOR CLASS swarm64_hash_op_class_int FOR TYPE int USING hash AS FUNCTION 2 hash_swarm64_int(int, bigint);

CREATE TABLE nation (
    n_nationkey int NOT NULL,
    n_name character varying(25) NOT NULL,
    n_regionkey int NOT NULL,
    n_comment character varying(152) NOT NULL
) PARTITION BY HASH (n_nationkey swarm64_hash_op_class_int);

CREATE TABLE region (
    r_regionkey int NOT NULL,
    r_name character varying(25) NOT NULL,
    r_comment character varying(152) NOT NULL
) PARTITION BY HASH (r_regionkey swarm64_hash_op_class_int);

CREATE TABLE part (
    p_partkey int NOT NULL,
    p_name character varying(55) NOT NULL,
    p_mfgr character varying(25) NOT NULL,
    p_brand character varying(10) NOT NULL,
    p_type character varying(25) NOT NULL,
    p_size int NOT NULL,
    p_container character varying(10) NOT NULL,
    p_retailprice numeric(13,2) NOT NULL,
    p_comment character varying(23) NOT NULL
) PARTITION BY HASH (p_partkey swarm64_hash_op_class_int);

CREATE TABLE supplier (
    s_suppkey int NOT NULL,
    s_name character varying(25) NOT NULL,
    s_address character varying(40) NOT NULL,
    s_nationkey int NOT NULL,
    s_phone character varying(15) NOT NULL,
    s_acctbal numeric(13,2) NOT NULL,
    s_comment character varying(101) NOT NULL
) PARTITION BY HASH (s_suppkey swarm64_hash_op_class_int);

CREATE TABLE partsupp (
    ps_partkey int NOT NULL,
    ps_suppkey int NOT NULL,
    ps_availqty int NOT NULL,
    ps_supplycost numeric(13,2) NOT NULL,
    ps_comment character varying(199) NOT NULL
) PARTITION BY HASH (ps_partkey swarm64_hash_op_class_int);

CREATE TABLE customer (
    c_custkey int NOT NULL,
    c_name character varying(25) NOT NULL,
    c_address character varying(40) NOT NULL,
    c_nationkey int NOT NULL,
    c_phone character varying(15) NOT NULL,
    c_acctbal numeric(13,2) NOT NULL,
    c_mktsegment character varying(10) NOT NULL,
    c_comment character varying(117) NOT NULL
) PARTITION BY HASH (c_custkey swarm64_hash_op_class_int);

CREATE TABLE orders (
    o_orderkey bigint NOT NULL,
    o_custkey int NOT NULL,
    o_orderstatus character varying(1) NOT NULL,
    o_totalprice numeric(13,2) NOT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority character varying(15) NOT NULL,
    o_clerk character varying(15) NOT NULL,
    o_shippriority int NOT NULL,
    o_comment character varying(79) NOT NULL
) PARTITION BY HASH (o_orderkey swarm64_hash_op_class_bigint);

CREATE TABLE lineitem (
    l_orderkey bigint NOT NULL,
    l_partkey int NOT NULL,
    l_suppkey int NOT NULL,
    l_linenumber int NOT NULL,
    l_quantity numeric(13,2) NOT NULL,
    l_extendedprice numeric(13,2) NOT NULL,
    l_discount numeric(13,2) NOT NULL,
    l_tax numeric(13,2) NOT NULL,
    l_returnflag character varying(1) NOT NULL,
    l_linestatus character varying(1) NOT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date NOT NULL,
    l_receiptdate date NOT NULL,
    l_shipinstruct character varying(25) NOT NULL,
    l_shipmode character varying(10) NOT NULL,
    l_comment character varying(44) NOT NULL
) PARTITION BY HASH (l_orderkey swarm64_hash_op_class_bigint);


{% for partition in range(partitions) %}

{% set modulus = nodes * partitions %}
{% set remainder = partition * nodes + node %}

CREATE FOREIGN TABLE
    nation_prt_{{ remainder }}
PARTITION OF
    nation FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER swarm64da_server OPTIONS (optimized_columns 'n_nationkey', optimization_level_target '900');

CREATE FOREIGN TABLE
    region_prt_{{ remainder }}
PARTITION OF
    region FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER swarm64da_server OPTIONS (optimized_columns 'r_regionkey', optimization_level_target '900');

CREATE FOREIGN TABLE
    part_prt_{{ remainder }}
PARTITION OF
    part FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER
   swarm64da_server options(optimized_columns 'p_partkey', optimization_level_target '900');

CREATE FOREIGN TABLE
    supplier_prt_{{ remainder }}
PARTITION OF
    supplier FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER
   swarm64da_server options(optimized_columns 's_nationkey', optimization_level_target '900');

CREATE FOREIGN TABLE
    partsupp_prt_{{ remainder }}
PARTITION OF
    partsupp FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER
   swarm64da_server options(optimized_columns 'ps_suppkey', optimization_level_target '900');

CREATE FOREIGN TABLE
    customer_prt_{{ remainder }}
PARTITION OF
    customer FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER
   swarm64da_server options(optimized_columns 'c_nationkey', optimization_level_target '900');

CREATE FOREIGN TABLE
    orders_prt_{{ remainder }}
PARTITION OF
    orders FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER
   swarm64da_server options(optimized_columns 'o_orderdate, o_custkey', optimization_level_target '900');

CREATE FOREIGN TABLE
    lineitem_prt_{{ remainder }}
PARTITION OF
    lineitem FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER
   swarm64da_server options(optimized_columns 'l_shipdate, l_partkey, l_receiptdate', optimization_level_target '900');

{% endfor %}
