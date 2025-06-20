import pandas as pd
import os 
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#file_reviews = r"C:\Users\Benjamin\Downloads\reviews.csv"
#file_books = r"C:\Users\Benjamin\Downloads\books.csv"

#Extraer

def  extract(reviews_path, books_path):
    try:

        logging.info("Extrayendo Datasets")
        reviews = pd.read_csv(reviews_path)
        books = pd.read_csv(books_path)
        df = reviews.merge(books, on="book_id", how="left")
        return df
    except Exception as e:
        logging.error(f"Error en extracción {e}")
        raise

#Transformar

def transform(df):
    try:
        logging.info("Transformando datos")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df.dropna(subset=["rating"], inplace=True)
        df = df[df["rating"] >= 3]
        logging.info("Transformacion completada")
        return df
    
    except Exception as e:
        logging.error(f"Error en transformacion {e}")
        raise

def aggregate(df):
    try:
        logging.info("Calculando promedio de rating por genero")
        promedio = df.groupby("genre").agg(avg_rating=("rating", "mean")).reset_index().round(2)
        return promedio
    
    except Exception as e:
        logging.error(f"Error en el calculo de promedio {e}")
        raise

def load(clean_df, agg_df):
    try:
        logging.info("Guardando archivos")
        clean_df.to_csv("clean_path.csv", index=False)
        agg_df.to_csv("agg_path.csv", index=False)
        logging.info("Archivos guardados")

    except Exception as e:
        logging.error(f"El archivo no se pudo cargar correctamente {e}")


def validate(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Archivo no encontrado {path}")
    else:
        logging.info(f"Validación Exitosa {path}")

#Ejecución de pipeline

df = extract(r"C:\Users\Benjamin\Downloads\reviews.csv", r"C:\Users\Benjamin\Downloads\books.csv")
clean_df = transform(df)
agg_df = aggregate(clean_df)
load(clean_df, agg_df)

print("hola mundo")

validate("clean_path.csv")
validate("agg_path.csv")