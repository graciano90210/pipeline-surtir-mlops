import pandas as pd
import numpy as np
import os
from azure.storage.blob import BlobServiceClient

# 1. Configuración
NUM_FILAS = 1000
LOCAL_FILENAME = "ventas_surtir.csv"
CONTAINER_NAME = "datos"

print("🚀 Generando datos simulados para Surtir SAS...")

# 2. Crear datos falsos
np.random.seed(42)
fechas = pd.date_range(start="2024-01-01", periods=NUM_FILAS, freq="H")
tiendas = np.random.choice(['Bogotá-Norte', 'Medellín-Centro', 'Cali-Sur', 'Barranquilla'], NUM_FILAS)
productos = np.random.choice(['Arroz 1kg', 'Aceite 1L', 'Leche Entera', 'Café 500g', 'Jabón'], NUM_FILAS)
cantidades = np.random.randint(1, 20, NUM_FILAS)
precios = np.random.uniform(2000, 15000, NUM_FILAS).round(2)

df = pd.DataFrame({
    'fecha': fechas,
    'tienda': tiendas,
    'producto': productos,
    'cantidad': cantidades,
    'precio_unitario': precios
})
df['total_venta'] = df['cantidad'] * df['precio_unitario']

# Guardar localmente
df.to_csv(LOCAL_FILENAME, index=False)
print(f"✅ Archivo {LOCAL_FILENAME} creado localmente.")

# 3. Subir a Azure
print("☁️ Conectando con Azure...")
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

if not connect_str:
    print("❌ ERROR: No encontré la variable de entorno AZURE_STORAGE_CONNECTION_STRING.")
    print("   Por favor ejecuta en la terminal: export AZURE_STORAGE_CONNECTION_STRING='tu_cadena_larga'")
else:
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=LOCAL_FILENAME)

        with open(LOCAL_FILENAME, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"🎉 ¡ÉXITO! Archivo subido correctamente al contenedor '{CONTAINER_NAME}' en Azure.")
    except Exception as e:
        print(f"❌ Ocurrió un error al subir: {e}")