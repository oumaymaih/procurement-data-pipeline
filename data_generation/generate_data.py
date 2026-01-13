import pandas as pd
import numpy as np
from faker import Faker

# =====================
# PARAMÈTRES GLOBAUX
# =====================
fake = Faker()

ROWS = 1_000_000
DATE = "2024-05-20"

CENTERS = ["CASABLANCA", "TANGER", "NADOR", "OUJDA"]

# SKUs DOIVENT correspondre à products.sku dans PostgreSQL
SKUS = [f"SKU_{str(i).zfill(5)}" for i in range(1, 1001)]  # 1000 produits


# =====================
# GÉNÉRATION DES ORDERS
# =====================
def generate_orders(center):
    data = {
        "order_id": [f"ORD-{center}-{i}" for i in range(ROWS)],
        "order_date": [DATE] * ROWS,
        "center_id": [center] * ROWS,
        "sku": np.random.choice(SKUS, ROWS),
        "qty": np.random.randint(1, 6, ROWS),
        "unit_price": np.round(np.random.uniform(2, 20, ROWS), 2)
    }

    df = pd.DataFrame(data)

    # CSV
    df.to_csv(f"orders/orders_{center.lower()}.csv", index=False)

    # Parquet (BONUS Big Data)
    df.to_parquet(
        f"orders/orders_{center.lower()}.parquet",
        engine="pyarrow"
    )


# =====================
# GÉNÉRATION DES STOCKS
# =====================
def generate_stock(center):
    data = {
        "snapshot_date": [DATE] * len(SKUS),
        "center_id": [center] * len(SKUS),
        "sku": SKUS,
        "available_stock": np.random.randint(50, 500, len(SKUS)),
        "reserved_stock": np.random.randint(0, 50, len(SKUS))
    }

    df = pd.DataFrame(data)

    df.to_csv(f"stock/stock_{center.lower()}.csv", index=False)
    df.to_parquet(
        f"stock/stock_{center.lower()}.parquet",
        engine="pyarrow"
    )


# =====================
# LANCEMENT
# =====================
for center in CENTERS:
    generate_orders(center)
    generate_stock(center)

print(" Génération terminée avec succès.")
