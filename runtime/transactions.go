package runtime

import "fmt"

func StartTransactions(n1, n2, n3, n4 int) {

	fmt.Println(n1, n2, n3, n4)
	//var wg sync.WaitGroup
	//
	//for _, i := range []int{n1, n2, n3, n4} {
	//	wg.Add(1)
	//	filePath := fmt.Sprintf("data/xact_files/%d.txt", i)
	//	go driver.SqlClient(&wg, filePath, i)
	//}
	//
	//wg.Wait()
}
