import requests
import psycopg2
import json
from datetime import datetime
from dotenv import load_dotenv  
import os   

# ----------------------------------------
# Configuración de conexión a PostgreSQL
# ----------------------------------------
load_dotenv()  # Carga las variables del archivo .env

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "camilo"),
    "password": os.getenv("DB_PASSWORD"),
}

if not DB_CONFIG["password"]:
    raise ValueError("DB_PASSWORD no está definido en el archivo .env")

def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def iso_now():
    return datetime.utcnow().isoformat()


# ----------------------------------------
# Manejo de batches en raw.ingestion_batches
# ----------------------------------------
def create_batch(cur, batch_id, source_system, description):
    cur.execute(
        """
        INSERT INTO raw.ingestion_batches (
            batch_id, source_system, description, started_at, status
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (batch_id, source_system, description, iso_now(), "RUNNING"),
    )


def finish_batch(cur, batch_id, status, error_message=None):
    cur.execute(
        """
        UPDATE raw.ingestion_batches
        SET finished_at = %s,
            status = %s,
            error_message = %s
        WHERE batch_id = %s
        """,
        (iso_now(), status, error_message, batch_id),
    )


# ----------------------------------------
# Cargar USERS → customers_raw + addresses_raw
# ----------------------------------------
def load_raw_customers(cur, batch_id):
    print("▶ Cargando usuarios desde FakeStore API...")
    resp = requests.get("https://fakestoreapi.com/users", timeout=30)
    resp.raise_for_status()
    users = resp.json()

    for user in users:
        source_customer_id = str(user.get("id"))
        email = user.get("email")
        phone = user.get("phone")

        name = user.get("name") or {}
        first = (name.get("firstname") or "").strip()
        last = (name.get("lastname") or "").strip()
        full_name = f"{first} {last}".strip()

        payload_text = json.dumps(user)
        loaded_at = iso_now()

        # customers_raw
        cur.execute(
            """
            INSERT INTO raw.customers_raw (
                batch_id,
                source_system,
                source_customer_id,
                email,
                phone_number,
                full_name,
                date_of_birth,
                signup_date,
                marketing_opt_in,
                raw_payload,
                loaded_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                batch_id,
                "FakeStoreAPI",
                source_customer_id,
                email,
                phone,
                full_name,
                None,
                None,
                "unknown",
                payload_text,
                loaded_at,
            ),
        )

        # addresses_raw
        addr = user.get("address") or {}
        street = f"{addr.get('number', '')} {addr.get('street', '')}".strip()
        city = addr.get("city")
        zipcode = addr.get("zipcode")
        geo = addr.get("geolocation") or {}
        lat = geo.get("lat")
        lon = geo.get("long")

        cur.execute(
            """
            INSERT INTO raw.addresses_raw (
                batch_id,
                source_system,
                source_address_id,
                source_customer_id,
                address_type,
                street,
                city,
                state_region,
                zipcode,
                country,
                latitude,
                longitude,
                raw_payload,
                loaded_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                batch_id,
                "FakeStoreAPI",
                None,
                source_customer_id,
                "shipping",
                street,
                city,
                None,
                zipcode,
                None,
                str(lat) if lat is not None else None,
                str(lon) if lon is not None else None,
                json.dumps(addr),
                loaded_at,
            ),
        )

    print(f"✔ Usuarios cargados: {len(users)}")
    return users


# ----------------------------------------
# Cargar PRODUCTS → products_raw + categories_raw
# ----------------------------------------
def load_raw_products(cur, batch_id):
    print("▶ Cargando productos desde FakeStore API...")
    resp = requests.get("https://fakestoreapi.com/products", timeout=30)
    resp.raise_for_status()
    products = resp.json()

    seen_categories = set()
    product_price_map = {}

    for prod in products:
        prod_id = prod.get("id")
        source_product_id = str(prod_id)
        title = prod.get("title")
        description = prod.get("description")
        category = prod.get("category")
        price = float(prod.get("price", 0.0))

        product_price_map[prod_id] = price

        payload_text = json.dumps(prod)
        loaded_at = iso_now()

        cur.execute(
            """
            INSERT INTO raw.products_raw (
                batch_id,
                source_system,
                source_product_id,
                sku,
                product_name,
                product_description,
                category_code,
                brand_name,
                base_price,
                currency_code,
                is_active,
                raw_payload,
                loaded_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                batch_id,
                "FakeStoreAPI",
                source_product_id,
                f"SKU-{source_product_id}",
                title,
                description,
                category,
                None,
                str(price),
                "USD",
                "true",
                payload_text,
                loaded_at,
            ),
        )

        if category and category not in seen_categories:
            seen_categories.add(category)
            cur.execute(
                """
                INSERT INTO raw.categories_raw (
                    batch_id,
                    source_system,
                    source_category_id,
                    category_name,
                    parent_category_id,
                    raw_payload,
                    loaded_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    batch_id,
                    "FakeStoreAPI",
                    category,
                    category,
                    None,
                    json.dumps({"category": category}),
                    loaded_at,
                ),
            )

    print(f"✔ Productos cargados: {len(products)}")
    print(f"✔ Categorías únicas cargadas: {len(seen_categories)}")
    return products, product_price_map


# ----------------------------------------
# Cupones simulados → coupons_raw
# ----------------------------------------
def load_raw_coupons(cur, batch_id):
    print("▶ Insertando cupones simulados en raw.coupons_raw...")

    coupons = [
        {
            "source_coupon_id": "c1",
            "coupon_code": "WELCOME10",
            "description": "10% off on first order over 50 USD",
            "discount_type": "percentage",
            "discount_value": "10",
            "min_order_amount": "50",
            "valid_from": "2025-01-01",
            "valid_to": "2025-12-31",
            "max_uses_per_customer": "1",
            "is_active": "true",
        },
        {
            "source_coupon_id": "c2",
            "coupon_code": "WELCOME20",
            "description": "20% off on first order over 200 USD",
            "discount_type": "percentage",
            "discount_value": "20",
            "min_order_amount": "200",
            "valid_from": "2025-01-01",
            "valid_to": "2025-12-31",
            "max_uses_per_customer": "1",
            "is_active": "true",
        },
    ]

    for c in coupons:
        loaded_at = iso_now()
        cur.execute(
            """
            INSERT INTO raw.coupons_raw (
                batch_id,
                source_system,
                source_coupon_id,
                coupon_code,
                description,
                discount_type,
                discount_value,
                min_order_amount,
                valid_from,
                valid_to,
                max_uses_per_customer,
                is_active,
                raw_payload,
                loaded_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                batch_id,
                "Simulated",
                c["source_coupon_id"],
                c["coupon_code"],
                c["description"],
                c["discount_type"],
                c["discount_value"],
                c["min_order_amount"],
                c["valid_from"],
                c["valid_to"],
                c["max_uses_per_customer"],
                c["is_active"],
                json.dumps(c),
                loaded_at,
            ),
        )

    print(f"✔ Cupones simulados cargados: {len(coupons)}")
    return coupons


def choose_coupon_for_total(total, coupons):
    if not coupons:
        return None

    c_map = {c["coupon_code"]: c for c in coupons}
    if total >= 400 and "WELCOME20" in c_map:
        return c_map["WELCOME20"]
    elif total >= 200 and "WELCOME10" in c_map:
        return c_map["WELCOME10"]
    else:
        return None


# ----------------------------------------
# Carts → orders_raw + order_items_raw + coupon_usage_raw
# ----------------------------------------
def load_raw_carts(cur, batch_id, product_price_map, coupons):
    print("▶ Cargando carritos/órdenes desde FakeStore API...")
    resp = requests.get("https://fakestoreapi.com/carts", timeout=30)
    resp.raise_for_status()
    carts = resp.json()

    for cart in carts:
        cart_id = str(cart.get("id"))
        user_id = str(cart.get("userId"))
        order_date = cart.get("date")
        products = cart.get("products", [])

        total_before = 0.0
        for item in products:
            prod_id = item.get("productId")
            qty = int(item.get("quantity", 0))
            price = float(product_price_map.get(prod_id, 0.0))
            total_before += qty * price

        coupon = choose_coupon_for_total(total_before, coupons)
        if coupon:
            if coupon["discount_type"] == "percentage":
                disc_val = float(coupon["discount_value"]) / 100.0
                discount_amount = total_before * disc_val
            else:
                discount_amount = float(coupon["discount_value"])
            discount_code_applied = coupon["coupon_code"]
        else:
            discount_amount = 0.0
            discount_code_applied = None

        total_after = total_before - discount_amount

        payload_text = json.dumps(cart)
        loaded_at = iso_now()

        cur.execute(
            """
            INSERT INTO raw.orders_raw (
                batch_id,
                source_system,
                source_order_id,
                source_customer_id,
                order_status,
                order_date,
                payment_method,
                total_before_discount,
                discount_code_applied,
                discount_amount,
                total_after_discount,
                currency_code,
                raw_payload,
                loaded_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                batch_id,
                "FakeStoreAPI",
                cart_id,
                user_id,
                "paid",
                order_date,
                "card",
                str(round(total_before, 2)),
                discount_code_applied,
                str(round(discount_amount, 2)),
                str(round(total_after, 2)),
                "USD",
                payload_text,
                loaded_at,
            ),
        )

        for item in products:
            prod_id = item.get("productId")
            qty = int(item.get("quantity", 0))
            price = float(product_price_map.get(prod_id, 0.0))

            source_order_item_id = f"{cart_id}-{prod_id}"
            item_payload = json.dumps(item)

            cur.execute(
                """
                INSERT INTO raw.order_items_raw (
                    batch_id,
                    source_system,
                    source_order_item_id,
                    source_order_id,
                    source_product_id,
                    quantity,
                    unit_price,
                    discount_amount,
                    currency_code,
                    raw_payload,
                    loaded_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    batch_id,
                    "FakeStoreAPI",
                    source_order_item_id,
                    cart_id,
                    str(prod_id),
                    str(qty),
                    str(round(price, 2)),
                    "0",
                    "USD",
                    item_payload,
                    loaded_at,
                ),
            )

        if coupon:
            usage_payload = {
                "cart_id": cart_id,
                "user_id": user_id,
                "coupon_code": coupon["coupon_code"],
            }
            cur.execute(
                """
                INSERT INTO raw.coupon_usage_raw (
                    batch_id,
                    source_system,
                    source_coupon_usage_id,
                    source_coupon_id,
                    source_customer_id,
                    source_order_id,
                    used_at,
                    raw_payload,
                    loaded_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    batch_id,
                    "Simulated",
                    f"usage-{cart_id}-{coupon['source_coupon_id']}",
                    coupon["source_coupon_id"],
                    user_id,
                    cart_id,
                    order_date,
                    json.dumps(usage_payload),
                    loaded_at,
                ),
            )

    print(f"✔ Órdenes/carritos cargados: {len(carts)}")
    return carts


# ----------------------------------------
# MAIN
# ----------------------------------------
def main():
    batch_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    print(f"=== Iniciando carga RAW Fakestore. batch_id={batch_id} ===")

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    try:
        create_batch(cur, batch_id, "FakeStore+SimulatedCoupons", "Full RAW load")
        load_raw_customers(cur, batch_id)
        products, price_map = load_raw_products(cur, batch_id)
        coupons = load_raw_coupons(cur, batch_id)
        load_raw_carts(cur, batch_id, price_map, coupons)
        finish_batch(cur, batch_id, "SUCCESS", None)
        print("=== Carga RAW completada con éxito. ===")
    except Exception as e:
        print("✖ Error durante la carga RAW:", e)
        try:
            finish_batch(cur, batch_id, "FAILED", str(e))
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
