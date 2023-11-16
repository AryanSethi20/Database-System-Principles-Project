-- Table creation statements
CREATE TABLE public.region
(
    r_regionkey integer not null,
    r_name character(25) collate pg_catalog."default" not null,
    r_comment character varying(152) collate pg_catalog."default",
    constraint region_pkey primary key (r_regionkey)
)
with(
    oids = false
)
tablespace pg_default;
alter table public.region
    owner to postgres;
--
create table public.nation
(
    n_nationkey integer not null,
    n_name character(25) collate pg_catalog."default" not null,
    n_regionkey integer not null,
    n_comment character varying(152) collate pg_catalog."default",
    constraint nation_pkey primary key (n_nationkey),
    constraint fk_nation foreign key (n_regionkey)
        references public.region(r_regionkey) match simple
        on update no action
        on delete no action
)
with(
    OIDS = FALSE
)
tablespace pg_default;
alter table public.region
    owner to postgres;
--
create table public.part
(
    p_partkey integer not null,
    p_name character varying(55) collate pg_catalog."default" not null,
    p_mfgr character(25) collate pg_catalog."default" not null,
    p_brand character(10) collate pg_catalog."default" not null,
    p_type character varying(25) collate pg_catalog."default" not null,
    p_size integer not null,
    p_container character (10) collate pg_catalog."default" not null,
    p_retailprice numeric(15,2) not null,
    p_comment character varying(23) collate pg_catalog."default" not null,
    constraint part_pkey primary key (p_partkey)
)
with(
    oids = false
)
tablespace pg_default;
alter table public.part
    owner to postgres;
--
create table public.supplier
(
    s_suppkey integer not null,
    s_name character(25) collate pg_catalog."default" not null,
    s_address character varying(40) collate pg_catalog."default" not null,
    s_nationkey integer not null,
    s_phone character(15) collate pg_catalog."default" not null,
    s_acctbal numeric(15,2) not null,
    s_comment character varying(101) collate pg_catalog."default" not null,
    constraint supplier_pkey primary key (s_suppkey),
    constraint fk_supplier foreign key (s_nationkey)
        references public.nation(n_nationkey) match simple
        on update no action
        on delete no action
)
with(
    oids = false
)
tablespace pg_default;
alter table public.supplier
    owner to postgres;
--
create table public.partsupp
(
    ps_partkey integer not null,
    ps_suppkey integer not null,
    ps_availqty integer not null,
    ps_supplycost numeric(15,2) not null,
    ps_comment character varying(199) collate pg_catalog."default" not null,
    constraint partsupp_pkey primary key (ps_partkey, ps_suppkey),
    constraint fk_ps_suppkey_partkey FOREIGN key (ps_partkey)
        references public.part(p_partkey) match simple
        on update no action
        on delete no action
)
with(
    oids = false
)
tablespace pg_default;
alter table public.partsupp
    owner to postgres;
--
create table public.customer
(
    c_custkey integer not null,
    c_name character varying(25) collate pg_catalog."default" not null,
    c_address character varying(40) collate pg_catalog."default" not null,
    c_nationkey integer not null,
    c_phone character(15) collate pg_catalog."default" not null,
    c_acctbal numeric(15,2) not null,
    c_mktsegment character(16) collate pg_catalog."default" not null,
    c_comment character varying(117) collate pg_catalog."default" not null,
    CONSTRAINT customer_pkey PRIMARY key (c_custkey),
    CONSTRAINT fk_customer FOREIGN key (c_nationkey)
        references public.nation(n_nationkey) match simple
        on update no action 
        on delete no action
)
with(
    oids = false
)
tablespace pg_default;
alter table public.customer
    owner to postgres;
--
create table public.orders
(
    o_orderkey integer not null,
    o_custkey integer not null,
    o_orderstatus character(1) collate pg_catalog."default" not null,
    o_totalprice numeric(15,2) not null,
    o_orderdate date not null,
    o_orderpriority character(15) collate pg_catalog."default" not null,
    o_clerk character(15) collate pg_catalog."default" not null,
    o_shipprority integer not null,
    o_comment character varying(79) collate pg_catalog."default" not null,
    constraint orders_pkey primary key (o_orderkey),
    constraint fk_orders foreign key (o_custkey)
        references public.customer(c_custkey) match simple
        on update no action
        on delete no action
)
with(
    oids = false
)
tablespace pg_default;
alter table public.orders
    owner to postgres;
--
create table public.lineitem
(
    l_orderkey integer not null,
    l_partkey integer not null,
    l_suppkey integer not null,
    l_linenumber integer not null,
    l_quantity numeric(15,2) not null,
    l_extendedprice numeric(15,2) not null,
    l_discount numeric(15,2) not null,
    l_tax numeric(15,2) not null,
    l_returnflag character(1) collate pg_catalog."default" not null,
    l_linestatus character(1) collate pg_catalog."default" not null,
    l_shipdate date not null,
    l_commitdate date not null,
    l_receiptdate date not null,
    l_shipinstruct character(25) collate pg_catalog."default" not null,
    l_shipmode character(10) collate pg_catalog."default" not null,
    l_comment character varying(44) collate pg_catalog."default" not null,
    constraint lineitem_pkey primary key (l_orderkey, l_partkey, l_suppkey, l_linenumber),
    constraint fk_lineitem_orderkey foreign key (l_orderkey)
        references public.orders (o_orderkey) match simple
        on update no action
        on delete no action,
    constraint fk_lineitem_partkey foreign key (l_partkey)
        references public.part (p_partkey) match simple
        on update no action
        on delete no action,
    constraint fk_lineitem_suppkey foreign key (l_suppkey)
        references public.supplier (s_suppkey) match simple
        on update no action
        on delete no action
)
with(
    oids = false
)
tablespace pg_default;
alter table public.lineitem
    owner to postgres;
-- Copy Command --
COPY your_table_name FROM '/path/to/your/file.tbl' DELIMITER '|' CSV;