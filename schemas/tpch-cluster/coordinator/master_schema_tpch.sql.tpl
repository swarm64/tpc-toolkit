DROP DATABASE IF EXISTS {{ node_dbname }}_cluster_master;
CREATE DATABASE {{ node_dbname }}_cluster_master;
\c {{ node_dbname }}_cluster_master;

{% if 's64da' in node_dbname %}
CREATE EXTENSION swarm64da;
{% endif %}
CREATE EXTENSION postgres_fdw;

{% for node in nodes %}
CREATE SERVER swarm_node_{{ node['name'] }}
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS(host '{{ node["ip"] }}', port '5432', dbname '{{ node_dbname }}', fetch_size '{{ fetch_size }}', use_remote_estimate 'true');

CREATE USER MAPPING FOR postgres
SERVER swarm_node_{{ node['name'] }}
OPTIONS(user 'postgres');
{% endfor %}

CREATE FUNCTION hash_swarm64_bigint(bigint, bigint) RETURNS bigint
AS 'SELECT $1::bigint'
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OPERATOR CLASS swarm64_hash_op_class_bigint
FOR TYPE BIGINT
USING HASH AS
OPERATOR 1 =,
FUNCTION 2 hash_swarm64_bigint(bigint, bigint);

CREATE FUNCTION hash_swarm64_int(int, bigint) RETURNS bigint
AS 'SELECT $1::bigint'
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OPERATOR CLASS swarm64_hash_op_class_int
FOR TYPE INT
USING HASH AS
OPERATOR 1 =,
FUNCTION 2 hash_swarm64_int(int, bigint);

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

{% set modulus = partitions %}
{% set remainder = partition %}
{% set node_idx = remainder % nodes|length %}
{% set node = 'swarm_node_' ~ nodes[node_idx]['name'] %}

CREATE FOREIGN TABLE nation_prt_{{ remainder }}
PARTITION OF nation FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'nation_prt_{{ remainder }}')
;

CREATE FOREIGN TABLE region_prt_{{ remainder }}
PARTITION OF region FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'region_prt_{{ remainder }}')
;

CREATE FOREIGN TABLE part_prt_{{ remainder }}
PARTITION OF part FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'part_prt_{{ remainder }}')
;

CREATE FOREIGN TABLE supplier_prt_{{ remainder }}
PARTITION OF supplier FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'supplier_prt_{{ remainder }}')
;

CREATE FOREIGN TABLE partsupp_prt_{{ remainder }}
PARTITION OF partsupp FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'partsupp_prt_{{ remainder }}')
;

CREATE FOREIGN TABLE customer_prt_{{ remainder }}
PARTITION OF customer FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'customer_prt_{{ remainder }}')
;

CREATE FOREIGN TABLE orders_prt_{{ remainder }}
PARTITION OF orders FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'orders_prt_{{ remainder }}')
;

CREATE FOREIGN TABLE lineitem_prt_{{ remainder }}
PARTITION OF lineitem FOR VALUES WITH (
    MODULUS {{ modulus }},
    REMAINDER {{ remainder }}
)
SERVER {{ node }}
OPTIONS(table_name 'lineitem_prt_{{ remainder }}')
;
{% endfor %}
