//go:build ignore

package main

import (
	"100knocks-preprocess/src/utils"
	"fmt"
	"log"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func main() {
	fmt.Println("=== P-004 ===")
	P004()

	fmt.Println("=== P-005 ===")
	P005()

	fmt.Println("=== P-006 ===")
	P006()

	fmt.Println("=== P-007 ===")
	P007()

	fmt.Println("=== P-008 ===")
	P008()

	fmt.Println("=== P-009 ===")
	P009()
}

func P004() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.Select(
		[]string{"sales_ymd", "customer_id", "product_cd", "amount"}).
		Filter(
			dataframe.F{
				Colname:    "customer_id",
				Comparator: series.Eq,
				Comparando: "CS018205000001",
			},
		)
	fmt.Println(df)
	return
}

func P005() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.Select(
		[]string{"sales_ymd", "customer_id", "product_cd", "amount"}).
		FilterAggregation(
			dataframe.And,
			dataframe.F{
				Colname:    "customer_id",
				Comparator: series.Eq,
				Comparando: "CS018205000001",
			},
			dataframe.F{
				Colname:    "amount",
				Comparator: series.GreaterEq,
				Comparando: 1000,
			},
		)
	fmt.Println(df)
	return
}

func P006() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.Select(
		[]string{"sales_ymd", "customer_id", "product_cd", "quantity", "amount"}).
		Filter(
			dataframe.F{
				Colname:    "customer_id",
				Comparator: series.Eq,
				Comparando: "CS018205000001",
			},
		).
		Filter(
			dataframe.F{
				Colname:    "amount",
				Comparator: series.GreaterEq,
				Comparando: 1000,
			},
			dataframe.F{
				Colname:    "quantity",
				Comparator: series.GreaterEq,
				Comparando: 5,
			},
		)
	fmt.Println(df)
	return
}

func P007() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.Select(
		[]string{"sales_ymd", "customer_id", "product_cd", "quantity", "amount"}).
		FilterAggregation(
			dataframe.And,
			dataframe.F{
				Colname:    "customer_id",
				Comparator: series.Eq,
				Comparando: "CS018205000001",
			},
			dataframe.F{
				Colname:    "amount",
				Comparator: series.GreaterEq,
				Comparando: 1000,
			},
			dataframe.F{
				Colname:    "amount",
				Comparator: series.LessEq,
				Comparando: 2000,
			},
		)
	fmt.Println(df)
	return
}

func P008() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.Select(
		[]string{"sales_ymd", "customer_id", "product_cd", "quantity", "amount"}).
		FilterAggregation(
			dataframe.And,
			dataframe.F{
				Colname:    "customer_id",
				Comparator: series.Eq,
				Comparando: "CS018205000001",
			},
			dataframe.F{
				Colname:    "product_cd",
				Comparator: series.Neq,
				Comparando: "P071401019",
			},
		)
	fmt.Println(df)
	return
}

func P009() {
	dfStore, err := utils.CsvToDataframe("../../data/store.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfStore.
		FilterAggregation(
			dataframe.And,
			dataframe.F{
				Colname:    "prefecture_cd",
				Comparator: series.Neq,
				Comparando: 13,
			},
			dataframe.F{
				Colname:    "floor_area",
				Comparator: series.LessEq,
				Comparando: 900,
			},
		)
	fmt.Println(df)
	return
}
