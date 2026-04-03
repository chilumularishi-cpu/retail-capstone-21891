def test_sales_amount():
    quantity = 5
    price = 100.0
    assert quantity * price == 500.0

def test_state_cleaning():
    raw_state = " ny "
    cleaned = raw_state.strip().upper()
    assert cleaned == "NY"

def test_category_cleaning():
    raw_category = " electronics "
    cleaned = raw_category.strip().title()
    assert cleaned == "Electronics"
