import pandas as pd
from trino.dbapi import connect
import subprocess
import os

# ======================================================
# PARAMÈTRES
# ======================================================
DATE = "2024-05-20"

LOCAL_DIR = f"supplier_orders_{DATE}"
HDFS_DIR = f"/output/supplier_orders/{DATE}"

os.makedirs(LOCAL_DIR, exist_ok=True)

# ======================================================
# CONNEXION À TRINO
# ======================================================
conn = connect(
    host="localhost",
    port=8080,
    user="trino",
    catalog="postgres",
    schema="public"
)

cursor = conn.cursor()

# ======================================================
# REQUÊTE ANALYTIQUE (100 % TRINO)
# ======================================================
query = """
WITH aggregated_orders AS (
    SELECT
        order_date,
        center_id,
        sku,
        SUM(qty) AS total_qty_sold
    FROM postgres.public.tmp_orders
    GROUP BY order_date, center_id, sku
),
net_stock AS (
    SELECT
        center_id,
        sku,
        (available_stock - reserved_stock) AS stock_net
    FROM postgres.public.tmp_stock
),
enriched AS (
    SELECT
        ao.center_id,
        ao.sku,
        ao.total_qty_sold,
        ns.stock_net,
        ss.safety_qty,
        p.pack_size,
        p.moq,
        p.supplier_id
    FROM aggregated_orders ao
    JOIN net_stock ns
        ON ao.sku = ns.sku
       AND ao.center_id = ns.center_id
    JOIN postgres.public.products p
        ON ao.sku = p.sku
    JOIN postgres.public.safety_stock ss
        ON ao.sku = ss.sku
)
SELECT
    center_id,
    sku,
    supplier_id,
    CASE
        WHEN (total_qty_sold + safety_qty - stock_net) <= 0
            THEN moq
        WHEN (total_qty_sold + safety_qty - stock_net) < moq
            THEN moq
        ELSE CEIL(
            (total_qty_sold + safety_qty - stock_net) / pack_size
        ) * pack_size
    END AS final_order_qty
FROM enriched
"""

# ======================================================
# EXÉCUTION DU CALCUL TRINO
# ======================================================
cursor.execute(query)
rows = cursor.fetchall()
columns = [c[0] for c in cursor.description]

df = pd.DataFrame(rows, columns=columns)

print("✅ Calcul analytique exécuté par Trino")
print(df.head())

# ======================================================
# CRÉATION DU DOSSIER HDFS
# ======================================================
subprocess.run([
    "docker", "exec", "namenode",
    "hdfs", "dfs", "-mkdir", "-p", HDFS_DIR
])

# ======================================================
# GÉNÉRATION : 1 FICHIER PAR FOURNISSEUR
# ======================================================
for supplier_id in df["supplier_id"].unique():

    df_supplier = df[df["supplier_id"] == supplier_id][
        ["center_id", "sku", "final_order_qty"]
    ]

    local_file = f"{LOCAL_DIR}/supplier_{supplier_id}.csv"
    df_supplier.to_csv(local_file, index=False)

    # 1️⃣ Copier le fichier dans le conteneur namenode
    subprocess.run([
        "docker", "cp",
        local_file,
        f"namenode:/tmp/supplier_{supplier_id}.csv"
    ])

    # 2️⃣ Envoyer le fichier dans HDFS
    subprocess.run([
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-put", "-f",
        f"/tmp/supplier_{supplier_id}.csv",
        HDFS_DIR
    ])

    print(f"✅ supplier_{supplier_id}.csv stocké dans HDFS")

print("🎯 PIPELINE TERMINÉ AVEC SUCCÈS")
