//go:build ignore

package main

import (
	"100knocks-preprocess/src/utils"
	"fmt"
	"log"
)

func main() {
	fmt.Println("=== P-001 ===")
	P001()

	fmt.Println("=== P-003 ===")
	P003()
}

func P001() {
	df, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(df)
	return
}

func P003() {
	dfReceipt, err := utils.CsvToDataframe("../../data/receipt.csv")
	if err != nil {
		log.Fatalln(err)
	}
	df := dfReceipt.Select(
		[]string{"sales_ymd", "customer_id", "product_cd", "amount"}).Rename("sales_date", "sales_ymd")
	fmt.Println(df)
	return
}
