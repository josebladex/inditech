import csv
import os

# Ruta del archivo CSV
csv_file = "usuarios.csv"
user_ids_file = "user_ids.txt"

def verificar_duplicados(csv_file):
    """
    Verifica si existen IDs de usuario duplicados en el archivo CSV.
    
    Args:
    - csv_file: Ruta del archivo CSV.
    
    Returns:
    - None
    """
    ids_encontrados = set()  # Para almacenar los user_id que ya hemos visto
    duplicados = []  # Lista para almacenar los duplicados encontrados

    try:
        with open(csv_file, mode="r", newline="") as file:
            reader = csv.reader(file)
            next(reader)  # Saltar los encabezados

            for row in reader:
                user_id = row[0]  # Suponiendo que el user_id está en la primera columna

                # Verificar si el user_id ya ha sido encontrado antes
                if user_id in ids_encontrados:
                    duplicados.append(row)  # Guardar el duplicado
                else:
                    ids_encontrados.add(user_id)

        if duplicados:
            print("Se encontraron los siguientes duplicados en el archivo CSV:")
            for duplicado in duplicados:
                print(f"ID: {duplicado[0]}, Country: {duplicado[1]}, R: {duplicado[2]}, F: {duplicado[3]}, M: {duplicado[4]}")
        else:
            print("No se encontraron duplicados en el archivo CSV.")

    except FileNotFoundError:
        print(f"Error: El archivo {csv_file} no fue encontrado.")
    except Exception as e:
        print(f"Ocurrió un error al procesar el archivo: {e}")

def contar_user_ids(user_ids_file):
    """
    Lee y cuenta la cantidad de user_ids en el archivo user_ids.txt.
    
    Args:
    - user_ids_file: Ruta del archivo user_ids.txt.
    
    Returns:
    - count: Cantidad de user_ids en el archivo.
    """
    try:
        if os.path.exists(user_ids_file):
            with open(user_ids_file, "r") as f:
                user_ids = f.readlines()
            print(f"Cantidad de user_ids en el archivo 'user_ids.txt': {len(user_ids)}")
            return len(user_ids)
        else:
            print(f"Error: El archivo {user_ids_file} no fue encontrado.")
            return 0
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo {user_ids_file}: {e}")
        return 0

def contar_entradas_csv(csv_file):
    """
    Lee y cuenta la cantidad de entradas (filas) en el archivo CSV.
    
    Args:
    - csv_file: Ruta del archivo CSV.
    
    Returns:
    - count: Cantidad de entradas en el archivo CSV.
    """
    try:
        if os.path.exists(csv_file):
            with open(csv_file, mode="r", newline="") as file:
                reader = csv.reader(file)
                rows = list(reader)
                # Excluir encabezados
                print(f"Cantidad de entradas en el archivo CSV (excluyendo encabezados): {len(rows) - 1}")
                return len(rows) - 1  # Restamos 1 por los encabezados
        else:
            print(f"Error: El archivo {csv_file} no fue encontrado.")
            return 0
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo {csv_file}: {e}")
        return 0


# Llamadas a las nuevas funciones
contar_user_ids(user_ids_file)  # Contar los user_ids en el archivo de texto
contar_entradas_csv(csv_file)  # Contar las entradas en el archivo CSV

# Llamar a la función para verificar los duplicados
verificar_duplicados(csv_file)
