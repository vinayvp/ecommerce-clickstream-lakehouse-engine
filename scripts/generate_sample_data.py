"""Sample Raw Data Generator for E-Commerce & Clickstream Lakehouse.

Generates realistic CDC mutation streams (Orders, Customers), high-volume
clickstream user sessions, and product catalog files.
Includes intentional CDC updates, deletes, out-of-order sequences, and malformed
data to validate schema governance, quarantine routing, and rescued data.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate_all_sample_data(base_data_dir: str = "data/raw") -> None:
    """Generate sample datasets across all raw data directories."""
    base_path = Path(base_data_dir)
    orders_path = base_path / "orders_cdc"
    cust_path = base_path / "customers_cdc"
    clicks_path = base_path / "clickstream"
    prod_path = base_path / "products"

    for p in [orders_path, cust_path, clicks_path, prod_path]:
        p.mkdir(parents=True, exist_ok=True)

    # 1. Product Catalog
    categories = [
        ("CAT-ELC", "Electronics"),
        ("CAT-APP", "Apparel & Fashion"),
        ("CAT-HME", "Home & Kitchen"),
        ("CAT-BKS", "Books & Media"),
        ("CAT-SPT", "Sports & Outdoors"),
    ]
    brands = ["Apex", "Nova", "Zenith", "Vanguard", "Summit", "Lumina"]
    products = []
    for i in range(1, 21):
        cat_id, cat_name = categories[(i - 1) % len(categories)]
        prod = {
            "product_id": f"PROD-{i:04d}",
            "product_name": f"{random.choice(brands)} Item {i}",
            "category_id": cat_id,
            "category_name": cat_name,
            "unit_price": round(random.uniform(15.0, 450.0), 2),
            "brand": random.choice(brands),
            "_rescued_data": None,
        }
        products.append(prod)

    with open(prod_path / "products_catalog.json", "w", encoding="utf-8") as f:
        for p in products:
            f.write(json.dumps(p) + "\n")

    # 2. Customers CDC Stream
    countries = ["US", "CA", "GB", "DE", "FR", "IN", "JP", "AU"]
    segments = ["STANDARD", "VIP", "PREMIUM", "ENTERPRISE"]
    customers = []
    base_time = datetime(2026, 10, 1, 9, 0, 0)

    for i in range(1, 31):
        cid = f"CUST-{i:04d}"
        c_time = base_time + timedelta(hours=i)
        # Initial INSERT
        customers.append({
            "customer_id": cid,
            "first_name": f"User{i}",
            "last_name": f"LastName{i}",
            "email": f"user{i}@example.com",
            "country": random.choice(countries),
            "city": f"Metropolis {i % 5}",
            "signup_date": c_time.strftime("%Y-%m-%d"),
            "customer_segment": random.choice(segments),
            "event_timestamp": c_time.isoformat(),
            "_change_seq": 1,
            "op_type": "INSERT",
            "_rescued_data": None,
        })

    # Add an UPDATE mutation for customer 1
    customers.append({
        "customer_id": "CUST-0001",
        "first_name": "User1",
        "last_name": "LastName1_Updated",
        "email": "user1_vip@example.com",
        "country": "US",
        "city": "Metropolis VIP",
        "signup_date": "2026-10-01",
        "customer_segment": "VIP",
        "event_timestamp": (base_time + timedelta(days=2)).isoformat(),
        "_change_seq": 2,
        "op_type": "UPDATE",
        "_rescued_data": None,
    })

    # Add one malformed customer to test quarantine (invalid email)
    customers.append({
        "customer_id": "CUST-9999",
        "first_name": "Corrupt",
        "last_name": "User",
        "email": "bademail_without_at_symbol",
        "country": "XX",
        "city": "Nowhere",
        "signup_date": "2026-10-01",
        "customer_segment": "STANDARD",
        "event_timestamp": base_time.isoformat(),
        "_change_seq": 1,
        "op_type": "INSERT",
        "_rescued_data": None,
    })

    with open(cust_path / "customers_batch_1.json", "w", encoding="utf-8") as f:
        for c in customers:
            f.write(json.dumps(c) + "\n")

    # 3. Orders CDC Stream
    order_statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
    orders = []

    for i in range(1, 61):
        oid = f"ORD-{i:05d}"
        cid = f"CUST-{(i % 25) + 1:04d}"
        prod = products[i % len(products)]
        qty = random.randint(1, 4)
        amt = round(prod["unit_price"] * qty, 2)
        t = base_time + timedelta(minutes=i * 20)

        # Initial INSERT
        orders.append({
            "order_id": oid,
            "customer_id": cid,
            "product_id": prod["product_id"],
            "order_status": "PENDING" if i > 40 else "DELIVERED",
            "order_amount": amt,
            "currency": "USD",
            "quantity": qty,
            "payment_method": random.choice(["CREDIT_CARD", "PAYPAL", "APPLE_PAY"]),
            "event_timestamp": t.isoformat(),
            "_change_seq": 1,
            "op_type": "INSERT",
            "_rescued_data": None,
        })

    # Add CDC mutations:
    # ORD-00045 transition from PENDING -> SHIPPED (_change_seq = 2)
    orders.append({
        "order_id": "ORD-00045",
        "customer_id": "CUST-0021",
        "product_id": "PROD-0005",
        "order_status": "SHIPPED",
        "order_amount": 120.50,
        "currency": "USD",
        "quantity": 2,
        "payment_method": "CREDIT_CARD",
        "event_timestamp": (base_time + timedelta(hours=30)).isoformat(),
        "_change_seq": 2,
        "op_type": "UPDATE",
        "_rescued_data": None,
    })

    # ORD-00046 CANCELLED and then DELETE mutation
    orders.append({
        "order_id": "ORD-00046",
        "customer_id": "CUST-0022",
        "product_id": "PROD-0006",
        "order_status": "CANCELLED",
        "order_amount": 85.00,
        "currency": "USD",
        "quantity": 1,
        "payment_method": "PAYPAL",
        "event_timestamp": (base_time + timedelta(hours=31)).isoformat(),
        "_change_seq": 2,
        "op_type": "UPDATE",
        "_rescued_data": None,
    })
    orders.append({
        "order_id": "ORD-00046",
        "customer_id": "CUST-0022",
        "product_id": "PROD-0006",
        "order_status": "CANCELLED",
        "order_amount": 85.00,
        "currency": "USD",
        "quantity": 1,
        "payment_method": "PAYPAL",
        "event_timestamp": (base_time + timedelta(hours=32)).isoformat(),
        "_change_seq": 3,
        "op_type": "DELETE",
        "_rescued_data": None,
    })

    # Out-of-order test case:
    # Stale mutation with _change_seq = 1 arrives after _change_seq = 2
    orders.append({
        "order_id": "ORD-00045",
        "customer_id": "CUST-0021",
        "product_id": "PROD-0005",
        "order_status": "PENDING",
        "order_amount": 120.50,
        "currency": "USD",
        "quantity": 2,
        "payment_method": "CREDIT_CARD",
        "event_timestamp": (base_time + timedelta(hours=5)).isoformat(),
        "_change_seq": 1,
        "op_type": "UPDATE",
        "_rescued_data": None,
    })

    # Corrupt order to test quarantine routing (negative amount)
    orders.append({
        "order_id": "ORD-BAD-001",
        "customer_id": "CUST-0001",
        "product_id": "PROD-0001",
        "order_status": "PENDING",
        "order_amount": -50.00,
        "currency": "USD",
        "quantity": 1,
        "payment_method": "CREDIT_CARD",
        "event_timestamp": base_time.isoformat(),
        "_change_seq": 1,
        "op_type": "INSERT",
        "_rescued_data": None,
    })

    with open(orders_path / "orders_batch_1.json", "w", encoding="utf-8") as f:
        for o in orders:
            f.write(json.dumps(o) + "\n")

    # 4. Clickstream Events (User Sessions)
    devices = ["DESKTOP", "MOBILE", "TABLET"]
    channels = ["ORGANIC_SEARCH", "PAID_SEARCH", "DIRECT", "EMAIL", "SOCIAL"]
    click_events = []
    event_counter = 1

    for session_num in range(1, 101):
        sid = f"SESS-{session_num:05d}"
        uid = f"CUST-{(session_num % 25) + 1:04d}"
        dev = random.choice(devices)
        chan = random.choice(channels)
        session_start = base_time + timedelta(minutes=session_num * 15)

        # Step 1: VIEW_PAGE
        click_events.append({
            "event_id": f"EVT-{event_counter:07d}",
            "session_id": sid,
            "user_id": uid,
            "event_type": "VIEW_PAGE",
            "product_id": random.choice(products)["product_id"],
            "page_url": "/product/detail",
            "device_type": dev,
            "traffic_source": chan,
            "event_timestamp": session_start.isoformat(),
            "_rescued_data": None,
        })
        event_counter += 1

        # 60% advance to ADD_TO_CART
        if session_num % 10 < 6:
            click_events.append({
                "event_id": f"EVT-{event_counter:07d}",
                "session_id": sid,
                "user_id": uid,
                "event_type": "ADD_TO_CART",
                "product_id": random.choice(products)["product_id"],
                "page_url": "/cart/add",
                "device_type": dev,
                "traffic_source": chan,
                "event_timestamp": (session_start + timedelta(minutes=2)).isoformat(),
                "_rescued_data": None,
            })
            event_counter += 1

            # 40% advance to BEGIN_CHECKOUT
            if session_num % 10 < 4:
                click_events.append({
                    "event_id": f"EVT-{event_counter:07d}",
                    "session_id": sid,
                    "user_id": uid,
                    "event_type": "BEGIN_CHECKOUT",
                    "product_id": random.choice(products)["product_id"],
                    "page_url": "/checkout/start",
                    "device_type": dev,
                    "traffic_source": chan,
                    "event_timestamp": (session_start + timedelta(minutes=4)).isoformat(),
                    "_rescued_data": None,
                })
                event_counter += 1

                # 25% complete PURCHASE
                if session_num % 10 < 3:
                    click_events.append({
                        "event_id": f"EVT-{event_counter:07d}",
                        "session_id": sid,
                        "user_id": uid,
                        "event_type": "PURCHASE_COMPLETED",
                        "product_id": random.choice(products)["product_id"],
                        "page_url": "/checkout/thankyou",
                        "device_type": dev,
                        "traffic_source": chan,
                        "event_timestamp": (session_start + timedelta(minutes=6)).isoformat(),
                        "_rescued_data": None,
                    })
                    event_counter += 1

    with open(clicks_path / "clickstream_batch_1.json", "w", encoding="utf-8") as f:
        for cl in click_events:
            f.write(json.dumps(cl) + "\n")

    print(f"Generated sample datasets successfully:")
    print(f"  - Products: {len(products)} records -> {prod_path}")
    print(f"  - Customers CDC: {len(customers)} records -> {cust_path}")
    print(f"  - Orders CDC: {len(orders)} records -> {orders_path}")
    print(f"  - Clickstream: {len(click_events)} events -> {clicks_path}")


if __name__ == "__main__":
    generate_all_sample_data()
