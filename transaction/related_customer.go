package transaction

import (
	"gorm.io/gorm"
	"log"
)

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
