import os
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

# 1) Carga variables de entorno
load_dotenv()  
MONGO_URI = os.getenv("MONGO_URI")  
DB_NAME   = os.getenv("DB_NAME", "mi_base_de_datos")

if not MONGO_URI:
    raise RuntimeError("Define la variable de entorno MONGO_URI en tu .env")

# 2) Conexión a Atlas
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# 3) Mapeo de archivos CSV a colecciones
csv_collections = {
    "data/drivers_updated.csv": "drivers",
    "data/winners.csv": "winners",
    "data/fastest_laps_updated.csv": "fastest_laps",
    "data/teams_updated.csv": "teams",
}

# 4) Función para procesar e insertar
def cargar_csv_a_mongo(path_csv, nombre_coleccion):
    print(f"Cargando '{path_csv}' → colección '{nombre_coleccion}'…")
    df = pd.read_csv(path_csv)

    registros = df.to_dict(orient="records")
    if registros:
        resultado = db[nombre_coleccion].insert_many(registros)
        print(f"  Insertados {len(resultado.inserted_ids)} documentos.")
    else:
        print("  WARNING: no hay registros para insertar.")

# 5) Recorrer todos los CSV
if __name__ == "__main__":
    for archivo, coleccion in csv_collections.items():
        if os.path.isfile(archivo):
            cargar_csv_a_mongo(archivo, coleccion)
        else:
            print(f"ERROR: no existe el archivo '{archivo}'")

    print("¡Carga completada!")
