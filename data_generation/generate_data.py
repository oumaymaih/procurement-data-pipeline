from faker import Faker
import pandas as pd
import numpy as np
import os
from datetime import date
import pyarrow as pa
import pyarrow.parquet as pq

fake = Faker()

# =========================
# PARAMÈTRES VOLUME (ALIGNÉS COLLÈGUE)
# =========================
CENTERS = ["casablanca", "nador", "oujda", "tanger"]

NB_PRODUCTS = 250_000
BATCH_SIZE = 50_000
SNAPSHOT_DATE = date.today()

ORDERS_PER_CENTER = 300_000   # ⬅️ IMPORTANT
ORDERS_BATCH = 50_000

BASE_DIR = "DATA_GENERATION"
ORDERS_DIR = os.path.join(BASE_DIR, "orders")
STOCK_DIR = os.path.join(BASE_DIR, "stock")
os.makedirs(ORDERS_DIR, exist_ok=True)
os.makedirs(STOCK_DIR, exist_ok=True)

SKUS = [f"SKU_{str(i).zfill(6)}" for i in range(1, NB_PRODUCTS + 1)]

# =========================
# STOCK : 1 ligne par produit × centre
# =========================
def generate_stock_all_centers():
    for center in CENTERS:
        csv_path = os.path.join(STOCK_DIR, f"stock_{center}.csv")
        parquet_path = os.path.join(STOCK_DIR, f"stock_{center}.parquet")

        writer = None
        first_write = True

        for i in range(0, NB_PRODUCTS, BATCH_SIZE):
            batch_skus = SKUS[i:i+BATCH_SIZE]

            df = pd.DataFrame({
                "snapshot_date": [SNAPSHOT_DATE] * len(batch_skus),
                "center_id": [center] * len(batch_skus),
                "sku": batch_skus,
                "available_stock": np.random.randint(50, 300, size=len(batch_skus)),
                "reserved_stock": np.random.randint(0, 50, size=len(batch_skus)),
            })

            # CSV
            df.to_csv(csv_path, mode="w" if first_write else "a",
                      index=False, header=first_write)
            first_write = False

            # Parquet
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)

        if writer:
            writer.close()

        print(f"✅ STOCK OK: {center} ({NB_PRODUCTS} lignes)")

# =========================
# ORDERS : > 1 million lignes
# =========================
def generate_orders_per_center():
    # Mapping exact demandé par le schéma
    CENTER_CODES = {
        "casablanca": "CASA",
        "tanger": "TANGER",
        "nador": "NADOR",
        "oujda": "OUJDA"
    }

    for center in CENTERS:
        center_code = CENTER_CODES[center]

        csv_path = os.path.join(ORDERS_DIR, f"orders_{center}.csv")
        parquet_path = os.path.join(ORDERS_DIR, f"orders_{center}.parquet")

        writer = None
        first_write = True
        order_id = 1

        for i in range(0, ORDERS_PER_CENTER, ORDERS_BATCH):
            cur = min(ORDERS_BATCH, ORDERS_PER_CENTER - i)

            # Dates en format 'YYYY-MM-DD'
            dates = [fake.date_between(start_date="-30d", end_date="today").strftime("%Y-%m-%d") for _ in range(cur)]

            df = pd.DataFrame({
                "order_id": np.arange(order_id, order_id + cur),
                "order_date": dates,
                "center_id": [center_code] * cur,
                "sku": np.random.choice(SKUS, size=cur, replace=True),
                "qty": np.random.randint(1, 20, size=cur),
                # Prix réaliste (ex: 5 à 200) avec 2 décimales
                "unit_price": np.round(np.random.uniform(5, 200, size=cur), 2)
            })
            order_id += cur

            # CSV append
            df.to_csv(csv_path, mode="w" if first_write else "a",
                      index=False, header=first_write)
            first_write = False

            # Parquet append
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema)
            writer.write_table(table)

        if writer:
            writer.close()

        print(f"✅ ORDERS OK: {center_code} ({ORDERS_PER_CENTER} lignes)")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("🚀 Génération gros volume (alignée collègue)…")
    generate_stock_all_centers()
    generate_orders_per_center()
    print("✅ Génération terminée")
