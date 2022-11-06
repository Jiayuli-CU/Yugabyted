package runtime

import (
	"cs5424project/driver"
	"fmt"
	"sync"
)

func StartTransactions(n1, n2, n3, n4 int) {

	var wg sync.WaitGroup

	for _, i := range []int{n1, n2, n3, n4} {
		wg.Add(1)
		filePath := fmt.Sprintf("data/xact_files/%d.txt", i)
		go driver.SqlClient(&wg, filePath, i)
	}

	wg.Wait()
}
