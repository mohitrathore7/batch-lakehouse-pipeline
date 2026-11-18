from lakehouse.silver import build_silver_orders, deduplicate_latest, split_valid_invalid


def test_deduplicate_keeps_latest_version(raw_orders):
    deduped = deduplicate_latest(raw_orders, "order_id", "updated_at")
    o1 = deduped.where("order_id = 'o1'").collect()
    assert len(o1) == 1
    assert o1[0]["order_status"] == "SHIPPED"


def test_invalid_rows_are_rejected_with_reason(raw_orders):
    _, rejected = split_valid_invalid(raw_orders)
    reasons = {r["order_id"]: r["reject_reason"] for r in rejected.collect()}
    assert reasons == {"o6": "null_customer_id", "o7": "non_positive_quantity", "o8": "invalid_status"}


def test_rows_outside_run_date_are_rejected(raw_orders):
    _, rejected = split_valid_invalid(raw_orders, ds="2024-01-02")
    assert {r["reject_reason"] for r in rejected.collect()} >= {"order_date_outside_run"}


def test_silver_enrichment(raw_orders, customers, products, cfg):
    silver, rejected = build_silver_orders(raw_orders, customers, products, cfg, ds="2024-01-01")
    rows = {r["order_id"]: r for r in silver.collect()}

    assert set(rows) == {"o1", "o2", "o3", "o4", "o5"}
    assert rejected.count() == 3
    assert rows["o1"]["order_amount"] == 10.0
    assert rows["o1"]["category"] == "BOOKS"
    assert rows["o1"]["customer_segment"] == "SMB"
    assert rows["o5"]["category"] == "UNKNOWN"  # product 99 not in dimension


def test_optimized_and_baseline_produce_identical_output(raw_orders, customers, products, cfg):
    cols = ["order_id", "category", "customer_segment", "order_amount"]
    opt, _ = build_silver_orders(raw_orders, customers, products, cfg, optimized=True)
    base, _ = build_silver_orders(raw_orders, customers, products, cfg, optimized=False)
    assert sorted(opt.select(cols).collect()) == sorted(base.select(cols).collect())
