CREATE EXTENSION swarm64da;

CREATE FOREIGN TABLE nation (
    n_nationkey int NOT NULL,
    n_name character varying(25) NOT NULL,
    n_regionkey int NOT NULL,
    n_comment character varying(152) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 'n_nationkey',
        optimization_level_target '900');

CREATE FOREIGN TABLE region (
    r_regionkey int NOT NULL,
    r_name character varying(25) NOT NULL,
    r_comment character varying(152) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 'r_regionkey',
        optimization_level_target '900');

CREATE FOREIGN TABLE part (
    p_partkey int NOT NULL,
    p_name character varying(55) NOT NULL,
    p_mfgr character varying(25) NOT NULL,
    p_brand character varying(10) NOT NULL,
    p_type character varying(25) NOT NULL,
    p_size int NOT NULL,
    p_container character varying(10) NOT NULL,
    p_retailprice numeric(13,2) NOT NULL,
    p_comment character varying(23) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 'p_partkey',
        optimization_level_target '900');

CREATE FOREIGN TABLE supplier (
    s_suppkey int NOT NULL,
    s_name character varying(25) NOT NULL,
    s_address character varying(40) NOT NULL,
    s_nationkey int NOT NULL,
    s_phone character varying(15) NOT NULL,
    s_acctbal numeric(13,2) NOT NULL,
    s_comment character varying(101) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 's_suppkey',
        optimization_level_target '900');

CREATE FOREIGN TABLE partsupp (
    ps_partkey int NOT NULL,
    ps_suppkey int NOT NULL,
    ps_availqty int NOT NULL,
    ps_supplycost numeric(13,2) NOT NULL,
    ps_comment character varying(199) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 'ps_partkey, ps_suppkey',
        optimization_level_target '900');

CREATE FOREIGN TABLE customer (
    c_custkey int NOT NULL,
    c_name character varying(25) NOT NULL,
    c_address character varying(40) NOT NULL,
    c_nationkey int NOT NULL,
    c_phone character varying(15) NOT NULL,
    c_acctbal numeric(13,2) NOT NULL,
    c_mktsegment character varying(10) NOT NULL,
    c_comment character varying(117) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 'c_custkey',
        optimization_level_target '900');

CREATE FOREIGN TABLE orders (
    o_orderkey bigint NOT NULL,
    o_custkey int NOT NULL,
    o_orderstatus "char" NOT NULL,
    o_totalprice numeric(13,2) NOT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority character varying(15) NOT NULL,
    o_clerk character varying(15) NOT NULL,
    o_shippriority int NOT NULL,
    o_comment character varying(79) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 'o_orderdate',
        optimization_level_target '900');

CREATE FOREIGN TABLE lineitem (
    l_orderkey bigint NOT NULL,
    l_partkey int NOT NULL,
    l_suppkey int NOT NULL,
    l_linenumber int NOT NULL,
    l_quantity numeric(13,2) NOT NULL,
    l_extendedprice numeric(13,2) NOT NULL,
    l_discount numeric(13,2) NOT NULL,
    l_tax numeric(13,2) NOT NULL,
    l_returnflag "char" NOT NULL,
    l_linestatus "char" NOT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date NOT NULL,
    l_receiptdate date NOT NULL,
    l_shipinstruct character varying(25) NOT NULL,
    l_shipmode character varying(10) NOT NULL,
    l_comment character varying(44) NOT NULL
) SERVER swarm64da_server
OPTIONS(optimized_columns 'l_shipdate,l_receiptdate',
        optimization_level_target '900');
