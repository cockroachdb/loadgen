package main

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

// schemaType is the type of schema we want to benchmark on. Tables are
// either interleaved (interleavedvariantTable) or not (Normal).
type schemaType string

const (
	normalSchema      schemaType = "normal"
	interleavedSchema            = "interleaved"
)

type tableName string

const (
	merchantTable tableName = "merchant"
	productTable            = "product"
	variantTable            = "variant"
	storeTable              = "store"
)

// Map of all DDL statements that are run when loadSchema is invoked.
var ddlStmts = map[tableName]string{
	merchantTable: `
	CREATE TABLE merchant (
	  m_id		integer	      not null,
	  m_name	text,
	  m_address	text,
	  primary key (m_id)
	)`,

	productTable: `
	CREATE TABLE product (
	  p_m_id	integer	      not null,
	  p_id		integer	      not null,
	  p_name	text,
	  p_desc	text,
	  primary key (p_m_id, p_id)
	)`,

	variantTable: `
	CREATE TABLE variant (
	  v_m_id	integer	      not null,
	  v_p_id	integer	      not null,
	  v_id		integer	      not null,
	  v_name	text,
	  v_qty		integer,
	  v_price	decimal,
	  primary key (v_m_id, v_p_id, v_id)
	)`,

	storeTable: `
	CREATE TABLE store (
	  s_m_id	integer	      not null,
	  s_id		integer	      not null,
	  s_name	text,
	  s_address	text,
	  primary key (s_m_id, s_id)
	)`,
}

var constraintStmts = map[tableName]string{
	productTable: `
  ALTER TABLE product
  ADD CONSTRAINT merchant_fk FOREIGN KEY (p_m_id) REFERENCES merchant (m_id);
  `,

	variantTable: `
  ALTER TABLE variant
  ADD CONSTRAINT product_fk FOREIGN KEY (v_m_id, v_p_id) REFERENCES product (p_m_id, p_id);
  `,

	storeTable: `
  ALTER TABLE store
  ADD CONSTRAINT merchant_fk FOREIGN KEY (s_m_id) REFERENCES merchant (m_id);
  `,
}

var insertStmts = map[tableName]string{
	merchantTable: `
  INSERT INTO merchant
  (m_id, m_name, m_address)
  VALUES
  `,
	productTable: `
  INSERT INTO product
  (p_m_id, p_id, p_name, p_desc)
  VALUES
  `,
	variantTable: `
  INSERT INTO variant
  (v_m_id, v_p_id, v_id, v_name, v_qty, v_price)
  VALUES
  `,
	storeTable: `
  INSERT INTO store
  (s_m_id, s_id, s_name, s_address)
  VALUES
  `,
}

type columnType int

const (
	textType columnType = iota
	intType
	decType
	pkeyIntType
	fkeyIntType
)

var tableTypes = map[tableName][]columnType{
	merchantTable: {pkeyIntType, textType, textType},
	productTable:  {fkeyIntType, pkeyIntType, textType, textType},
	variantTable:  {fkeyIntType, fkeyIntType, pkeyIntType, textType, intType, decType},
	storeTable:    {fkeyIntType, pkeyIntType, textType, textType},
}

type interleaveInfo struct {
	name           tableName
	interleaveStmt string
}

var interleaveStmts = map[tableName]string{
	productTable: "INTERLEAVE IN PARENT merchant (p_m_id)",
	variantTable: "INTERLEAVE IN PARENT product (v_m_id, v_p_id)",
	storeTable:   "INTERLEAVE IN PARENT merchant (s_m_id)",
}

var tableRows = make(map[tableName]int)

func initDDL() {
	tableRows[merchantTable] = *nMerchants
	tableRows[productTable] = *nProducts
	tableRows[variantTable] = *nVariants
	tableRows[storeTable] = *nStores
}

func loadSchema(db *sql.DB) error {
	stderr.Println("Creating tables...")
	for _, table := range tables {
		stmt := ddlStmts[table]
		if interleaveStmt, ok := interleaveStmts[table]; ok && schemaVar == interleavedSchema {
			stmt = fmt.Sprintf("%s %s", stmt, interleaveStmt)
		}
		stderr.Println(stmt)
		if _, err := db.Exec(stmt); err != nil {
			return errors.Wrap(err, "loading schema failed")
		}
	}
	stderr.Println("Creating tables complete.")

	return nil
}

func applyConstraints(db *sql.DB) error {
	stderr.Println("Applying table constraints (i.e. foreign keys)")
	for _, table := range tables {
		stmt := constraintStmts[table]
		stderr.Println(stmt)
		if _, err := db.Exec(stmt); err != nil {
			return errors.Wrap(err, "applying constraints failed")
		}
	}
	stderr.Println("Applying constraints complete.")

	return nil
}
