package main

import (
	"cs5424project/data"
	"cs5424project/store/cassandra"
)

func main() {

	defer cassandra.CloseSession()

	data.CqlDataLoader()

	//var arg1, arg2, arg3, arg4 string
	//var n1, n2, n3, n4 int
	//var err error
	//fmt.Printf("input number: ")
	//_, err = fmt.Scanln(&arg1, &arg2, &arg3, &arg4)
	//if err != nil {
	//	fmt.Println("scan user input error")
	//}
	//n1, err = strconv.Atoi(arg1)
	//n2, err = strconv.Atoi(arg2)
	//n3, err = strconv.Atoi(arg3)
	//n4, err = strconv.Atoi(arg4)
	//if err != nil {
	//	fmt.Printf("input format error: %v\n", err)
	//}
	//
	//var wg sync.WaitGroup
	//
	//for _, i := range []int{n1, n2, n3, n4} {
	//	wg.Add(1)
	//	filePath := fmt.Sprintf("data/xact_files/%d.txt", i)
	//	go driver.CqlClient(&wg, filePath, i)
	//}
	//
	//wg.Wait()
	//cassandra2.DeliveryTransaction(context.Background(), 4, 10)
}
