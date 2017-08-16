package main

import (
	"database/sql"
	"fmt"
)

var ddls = [...]string{
	`
create table warehouse (
  w_id        integer   not null primary key,
  w_name      varchar(10),
  wStreet1  varchar(20),
  wStreet2  varchar(20),
  wCity      varchar(20),
  wState     char(2),
  wZip       char(9),
  w_tax       decimal(4,4),
  w_ytd       decimal(12,2)
);`,

	// 10 districts per warehouse
	`
create table district (
  dID         integer       not null,
  d_w_id       integer       not null,
  d_name       varchar(10),
  dStreet1   varchar(20),
  dStreet2   varchar(20),
  dCity       varchar(20),
  dState      char(2),
  dZip        char(9),
  d_tax        decimal(4,4),
  d_ytd        decimal(12,2),
  d_next_o_id  integer,
  primary key (d_w_id, dID),
  foreign key (d_w_id) references warehouse (w_id)
);`,

	`
create table customer (
  cID           integer        not null,
  cDID         integer        not null,
  cWID         integer        not null,
  cFirst        varchar(16),
  cMiddle       char(2),
  cLast         varchar(16),
  cStreet1     varchar(20),
  cStreet2     varchar(20),
  cCity         varchar(20),
  cState        char(2),
  cZip          char(9),
  cPhone        char(16),
  cSince        timestamp,
  cCredit       char(2),
  cCreditLim   decimal(12,2),
  cDiscount     decimal(4,4),
  cBalance      decimal(12,2),
  c_ytd_payment  decimal(12,2),
  c_payment_cnt  integer,
  c_delivery_cnt integer,
  cData         varchar(500),
  primary key (cWID, cDID, cID),
  foreign key (cWID, cDID) references district (d_w_id, dID)
);`,

	// No PK necessary for this table.
	`
create table history (
  h_c_id   integer,
  h_c_d_id integer,
  h_c_w_id integer,
  h_d_id   integer,
  h_w_id   integer,
  hDate   timestamp,
  hAmount decimal(6,2),
  h_data   varchar(24),
  foreign key (h_c_w_id, h_c_d_id, h_c_id) references customer (cWID, cDID, cID),
  foreign key (h_w_id, h_d_id) references district (d_w_id, dID)
);`,

	`
create table "order" (
  o_id         integer      not null,
  o_d_id       integer      not null,
  o_w_id       integer      not null,
  o_c_id       integer,
  o_entry_d    timestamp,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  primary key (o_w_id, o_d_id, o_id),
  foreign key (o_w_id, o_d_id, o_c_id) references customer (cWID, cDID, cID)
);`,

	`
create table new_order (
  no_o_id  integer   not null,
  no_d_id  integer   not null,
  no_w_id  integer   not null,
  primary key (no_w_id, no_d_id, no_o_id),
  foreign key (no_w_id, no_d_id, no_o_id) references "order" (o_w_id, o_d_id, o_id)
);`,

	`
create table item (
  i_id     integer      not null,
  i_im_id  integer,
  i_name   varchar(24),
  i_price  decimal(5,2),
  i_data   varchar(50),
  primary key (i_id)
);`,

	`
create table stock (
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
);`,

	`
create table order_line (
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
);`,

	`create index customer_idx on customer (cWID, cDID, cLast, cFirst);`,
	`create unique index order_idx on "order" (o_w_id, o_d_id, o_carrier_id, o_id);`,
}

// loadSchema loads the entire TPCC schema into the database.
func loadSchema(db *sql.DB) {
	for _, stmt := range ddls {
		if _, err := db.Exec(stmt); err != nil {
			panic(fmt.Sprintf("Couldn't exec %s: %s\n", stmt, err))
		}
	}
}
