import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from azure.storage.blob import BlobServiceClient
import joblib
import os

# CONFIGURACIÓN
FILE_NAME = "ventas_surtir.csv"
MODEL_NAME = "modelo_prediccion_ventas.pkl"
CONTAINER_DATOS = "datos"
CONTAINER_MODELOS = "modelos"

print("🧠 Iniciando proceso de entrenamiento...")

# 1. CONEXIÓN A AZURE
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
if not connect_str:
    raise ValueError("No se encontró la variable de entorno de conexión.")

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# 2. DESCARGAR DATOS (E de ETL)
print(f"⬇️ Descargando {FILE_NAME} desde Azure...")
blob_client_datos = blob_service_client.get_blob_client(container=CONTAINER_DATOS, blob=FILE_NAME)

with open(FILE_NAME, "wb") as download_file:
    download_file.write(blob_client_datos.download_blob().readall())

# 3. ENTRENAMIENTO (M de ML)
print("⚙️ Entrenando modelo...")
df = pd.read_csv(FILE_NAME)

# Features (X) y Target (y)
X = df[['cantidad', 'precio_unitario']]
y = df['total_venta']

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Modelo (Regresión Lineal)
model = LinearRegression()
model.fit(X_train, y_train)

# Evaluación
predictions = model.predict(X_test)
r2 = r2_score(y_test, predictions)
print(f"📊 Precisión del modelo (R2 Score): {r2:.4f} (¡1.0 es perfecto!)")

# 4. GUARDAR Y SUBIR MODELO (L de Load)
joblib.dump(model, MODEL_NAME)
print(f"💾 Modelo guardado localmente como {MODEL_NAME}")

print(f"⬆️ Subiendo modelo a Azure container '{CONTAINER_MODELOS}'...")
blob_client_model = blob_service_client.get_blob_client(container=CONTAINER_MODELOS, blob=MODEL_NAME)

with open(MODEL_NAME, "rb") as data:
    blob_client_model.upload_blob(data, overwrite=True)

print("🎉 ¡MISIÓN CUMPLIDA! El modelo ya está en la nube.")