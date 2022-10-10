package transactions

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"log"
	"time"
)

var db = postgre.GetDB()

func NewOrderTransaction(
	warehouseId, districtId, customerId uint64,
	numItems, supplierWareHouses, quantities []uint64) {

}

func PaymentTransaction(warehouseId, districtId, customerId uint64, payment float64) {
	err := db.Transaction(func(tx *gorm.DB) error {
		// 1. Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
		// 		• Decrement C_BALANCE by PAYMENT
		// 		• Increment C_YTD_PAYMENT by PAYMENT
		// 		• Increment C_PAYMENT_CNT by 1
		var customer models.Customer
		if err := tx.Model(&models.Customer{}).
			Where("id = ? AND warehouse_id = ? AND district_id = ?", customerId, warehouseId, districtId).
			Find(&customer).Error; err != nil {
			log.Printf("Find customer error: %v", err)
			return err
		}
		if customer.Balance < payment {
			return errors.New(fmt.Sprintf("Not enough balance. Current balance is %v, need to pay %v", customer.Balance, payment))
		}
		customer.Balance -= payment
		customer.YearToDatePayment += payment
		customer.PaymentsNumber++
		if err := tx.Model(&models.Customer{}).
			Where("id = ? AND warehouse_id = ? AND district_id = ?", customerId, warehouseId, districtId).
			Updates(&customer).Error; err != nil {
			log.Printf("Update customer error: %v", err)
			return err
		}

		// 2. Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
		var warehouse models.Warehouse
		if err := tx.Model(&models.Warehouse{}).
			Where("id = ?", warehouseId).
			Find(&warehouse).Error; err != nil {
			log.Printf("Find warehouse error: %v", err)
			return err
		}
		warehouse.YearToDateAmount += payment
		if err := tx.Model(&models.Warehouse{}).
			Where("id = ?", warehouseId).
			Updates(&warehouse).Error; err != nil {
			log.Printf("Update warehouse error: %v", err)
			return err
		}

		// 3. Update the district (C_W_ID,C_D_ID) by incrementing D_YTD by PAYMENT
		var district models.District
		if err := tx.Model(&models.District{}).
			Where("id = ? AND warehouse_id = ?", districtId, warehouseId).
			Find(&district).Error; err != nil {
			log.Printf("Find district error: %v", err)
			return err
		}
		district.Year2DateAmount += payment
		if err := tx.Model(&models.District{}).
			Where("id = ? AND warehouse_id = ?", districtId, warehouseId).
			Updates(&district).Error; err != nil {
			log.Printf("Update district error: %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Payment transaction error: %v", err)
	}
}

func DeliveryTransaction(warehouseId, carrierId uint64) {
	err := db.Transaction(func(tx *gorm.DB) error {
		// todo
		// 1. For DISTRICT_NO = 1 to 10
		// 		(a) Let N denote the value of the smallest order number O_ID for district (W_ID,DISTRICT_NO)
		//			with O_CARRIER_ID = null; i.e.,
		//			N = min{t.O_ID ∈ Order | t.O_W_ID = W_ID, t.D_ID = DISTRICT_NO, t.O_CARRIER_ID = null}
		//			Let X denote the order corresponding to order number N, and let C denote the customer
		//			who placed this order
		//		(b) Update the order X by setting O_CARRIER_ID to CARRIER_ID
		//		(c) Update all the order-lines in X by setting OL_DELIVERY_D to the current date and time
		//		(d) Update customer C as follows:
		//			• Increment C_BALANCE by B, where B denote the sum of OL_AMOUNT for all the
		//			  items placed in order X
		//			• Increment C_DELIVERY_CNT by 1
		var order models.Order
		if err := tx.Model(&models.Order{}).Where("carrier_id = null").First(&order).Error; err != nil {
			log.Printf("First order error: %v", err)
			return err
		}
		var customer models.Customer
		if err := tx.Model(&models.Customer{}).Where("id = ?", order.CustomerId).Find(&customer).Error; err != nil {
			log.Printf("Find customer error: %v", err)
			return err
		}
		order.CarrierId = carrierId
		if err := tx.Model(&models.Order{}).Where("id = ?", order.Id).Updates(&order).Error; err != nil {
			log.Printf("Update order error: %v", err)
			return err
		}
		var orderLines []models.OrderLine
		if err := tx.Model(&models.OrderLine{}).Where("order_id = ?", order.Id).Find(&orderLines).Error; err != nil {
			log.Printf("Find order lines error: %v", err)
			return err
		}
		totalAmount := 0.0
		for _, orderLine := range orderLines {
			orderLine.DeliveryTime = time.Now()
			totalAmount += orderLine.Price
			if err := tx.Model(&models.OrderLine{}).Where("id = ?", orderLine.Id).Updates(&orderLine).Error; err != nil {
				log.Printf("Update order line error: %v", err)
				return err
			}
		}
		customer.Balance += totalAmount
		customer.DeliveriesNumber++
		if err := tx.Model(&models.Customer{}).Where("id = ?", customer.Id).Updates(&customer).Error; err != nil {
			log.Printf("Update customer error: %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Delivery transaction error: %v", err)
	}
}

func OrderStatusTransaction() {
	err := db.Transaction(func(tx *gorm.DB) error {
		// todo
		return nil
	})
	if err != nil {
		log.Printf("Order status transaction error: %v", err)
	}
}

func StockLevelTransaction() {
	err := db.Transaction(func(tx *gorm.DB) error {
		// todo
		// 1. Let N denote the value of the next available order number D_NEXT_O_ID for district (W_ID,D_ID)
		// 2. Let S denote the set of items from the last L orders for district (W_ID,D_ID); i.e.,
		//	  S = {t.OL_I_ID | t ∈ Order-Line, t.OL_D_ID = D_ID, t.OL_W_ID = W_ID, t.OL_O_ID ∈ [N−L,N)}
		// 3. Output the total number of items in S where its stock quantity at W_ID is below the threshold;
		//	  i.e., S_QUANTITY < T
		return nil
	})
	if err != nil {
		log.Printf("Stock level transaction error: %v", err)
	}
}

func PopularItemTransaction() {
	err := db.Transaction(func(tx *gorm.DB) error {
		// todo
		// 1. Let N denote the value of the next available order number D_NEXT_O_ID for district (W_ID,D_ID)
		// 2. Let S denote the set of last L orders for district (W_ID,D_ID); i.e.,
		//	  S = {t.O_ID | t ∈ Order, t.O_D_ID = D_ID, t.O_W_ID = W_ID, t.O_ID ∈ [N − L,N)}
		// 3. For each order number x in S
		//		(a) Let Ix denote the set of order-lines for this order; i.e.,
		//			Ix = {t ∈ Order-Line | t.OL_O_ID = x, t.OL_D_ID = D_ID, t.OL_W_ID = W_ID}
		//		(b) Let Px ⊆ Ix denote the subset of popular items in Ix; i.e.,
		//			t ∈ Px ⇐⇒ ∀ t′ ∈ Ix, t′.OL QUANTITY ≤ t.OL QUANTITY
		return nil
	})
	if err != nil {
		log.Printf("Popular item transaction error: %v", err)
	}
}

func TopBalanceTransaction() {
	err := db.Transaction(func(tx *gorm.DB) error {
		// todo
		// 1. Let C ⊆ Customer denote the subset of 10 customers (i.e., |C| = 10) such that for each pair of
		//	  customers (x, y), where x ∈ C and y ∈ Customer−C, we have x.C_BALANCE ≥ y.C_BALANCE
		return nil
	})
	if err != nil {
		log.Printf("Top balance transaction error: %v", err)
	}
}

func RelatedCustomerTransaction() {
	err := db.Transaction(func(tx *gorm.DB) error {
		// todo
		// 1. Let S be the set of customers who are related to the customer identified by (C_W_ID, C_D_ID,
		//	  C_ID).
		//	  S = {C′ ∈ Customer | C′.C_W_ID != C_W_ID,
		//	  ∃ O ∈ Order, O.O W_ID = C_W_ID, O.O_D_ID = C_D_ID, O.O_C_ID = C_ID,
		//	  ∃ O′ ∈ Order, O′.O W_ID = C′.C_W_ID, O′.O_D_ID = C′.C_D_ID, O′.O_C_ID = C′.C_ID
		//	  ∃ OL1 ∈ OrderItem, OL1.OL_W_ID = O.O_W_ID, OL1.OL_D_ID = O.O_D_ID, OL1.OL_O_ID = O.O_ID,
		//	  ∃ OL2 ∈ OrderItem, OL2.OL_W_ID = O.O_W_ID, OL2.OL_D_ID = O.O_D_ID, OL2.OL_O_ID = O.O_ID,
		//	  ∃ OL1′ ∈ OrderItem, OL1′.OL_W_ID = O′.O_W_ID, OL1′.OL_D_ID = O′.O_D_ID, OL1′.OL_O_ID = O′.O_ID,
		//	  ∃ OL2′ ∈ OrderItem, OL2′.OL_W_ID = O′.O_W_ID, OL2′.OL_D_ID = O′.O_D_ID, OL2′.OL_O_ID = O′.O_ID,
		//	  OL1.OL_I_ID != OL2.OL_I_ID, OL1′.OL_I_ID != OL2′.OL_I_ID,
		//	  OL1.OL_I_ID = OL1′.OL_I_ID, OL2.OL_I_ID = OL2′.OL_I_ID}
		return nil
	})
	if err != nil {
		log.Printf("Related customer transaction error: %v", err)
	}
}
