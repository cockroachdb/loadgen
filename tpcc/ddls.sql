-- TODO(jordan): put "if not exists" behind a flag since it's non-standard
-- SQL - we'd like to be able to run this against pg as well.
create database if not exists tpcc;

create table if not exists warehouse (
  w_id        integer   not null primary key,
  w_name      varchar(10),
  w_street_1  varchar(20),
  w_street_2  varchar(20),
  w_city      varchar(20),
  w_state     char(2),
  w_zip       char(9),
  w_tax       decimal(4,4),
  w_ytd       decimal(12,2)
);

-- 10 districts per warehouse
create table if not exists district (
  d_id         integer       not null,
  d_w_id       integer       not null,
  d_name       varchar(10),
  d_street_1   varchar(20),
  d_street_2   varchar(20),
  d_city       varchar(20),
  d_state      char(2),
  d_zip        char(9),
  d_tax        decimal(4,4),
  d_ytd        decimal(12,2),
  d_next_o_id  integer,
  primary key (d_w_id, d_id),
  foreign key (d_w_id) references warehouse (w_id)
);

create table if not exists customer (
  c_id           integer        not null,
  c_d_id         integer        not null,
  c_w_id         integer        not null,
  c_first        varchar(16),
  c_middle       char(2),
  c_last         varchar(16),
  c_street_1     varchar(20),
  c_street_2     varchar(20),
  c_city         varchar(20),
  c_state        char(2),
  c_zip          char(9),
  c_phone        char(16),
  c_since        timestamp,
  c_credit       char(2),
  c_credit_lim   decimal(12,2),
  c_discount     decimal(4,4),
  c_balance      decimal(12,2),
  c_ytd_payment  decimal(12,2),
  c_payment_cnt  integer,
  c_delivery_cnt integer,
  c_data         varchar(500),
  primary key (c_w_id, c_d_id, c_id),
  foreign key (c_w_id, c_d_id) references district (d_w_id, d_id)
);

-- No PK necessary for this table.
create table if not exists history (
  h_c_id   integer,
  h_c_d_id integer,
  h_c_w_id integer,
  h_d_id   integer,
  h_w_id   integer,
  h_date   timestamp,
  h_amount decimal(6,2),
  h_data   varchar(24),
  foreign key (h_c_w_id, h_c_d_id, h_c_id) references customer (c_w_id, c_d_id, c_id),
  foreign key (h_w_id, h_d_id) references district (d_w_id, d_id)
);

create table if not exists "order" (
  o_id         integer      not null,
  o_d_id       integer      not null,
  o_w_id       integer      not null,
  o_c_id       integer,
  o_entry_d    timestamp,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  primary key (o_w_id, o_d_id, o_id),
  foreign key (o_w_id, o_d_id, o_c_id) references customer (c_w_id, c_d_id, c_id)
);

create table if not exists new_order (
  no_o_id  integer   not null,
  no_d_id  integer   not null,
  no_w_id  integer   not null,
  primary key (no_w_id, no_d_id, no_o_id),
  foreign key (no_w_id, no_d_id, no_o_id) references "order" (o_w_id, o_d_id, o_id)
);

create table if not exists item (
  i_id     integer      not null,
  i_im_id  integer,
  i_name   varchar(24),
  i_price  decimal(5,2),
  i_data   varchar(50),
  primary key (i_id)
);

create table if not exists stock (
  s_i_id       integer       not null,
  s_w_id       integer       not null,
  s_quantity   integer,
  s_dist_01    char(24),
  s_dist_02    char(24),
  s_dist_03    char(24),
  s_dist_04    char(24),
  s_dist_05    char(24),
  s_dist_06    char(24),
  s_dist_07    char(24),
  s_dist_08    char(24),
  s_dist_09    char(24),
  s_dist_10    char(24),
  s_ytd        integer,
  s_order_cnt  integer,
  s_remote_cnt integer,
  s_data       varchar(50),
  primary key (s_w_id, s_i_id),
  foreign key (s_w_id) references warehouse (w_id),
  foreign key (s_i_id) references item (i_id)
);

create table if not exists order_line (
  ol_o_id         integer   not null,
  ol_d_id         integer   not null,
  ol_w_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_supply_w_id  integer,
  ol_delivery_d   timestamp,
  ol_quantity     integer,
  ol_amount       decimal(6,2),
  ol_dist_info    char(24),
  primary key (ol_w_id, ol_d_id, ol_o_id, ol_number),
  foreign key (ol_w_id, ol_d_id, ol_o_id) references "order" (o_w_id, o_d_id, o_id),
  foreign key (ol_supply_w_id, ol_i_id) references stock (s_w_id, s_i_id)
);

create index if not exists customer_idx on customer (c_w_id, c_d_id, c_last, c_first);
create unique index if not exists order_idx on "order" (o_w_id, o_d_id, o_carrier_id, o_id);
