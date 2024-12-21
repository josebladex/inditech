import asyncio
import aiohttp
import csv
import os
import signal

# URL base de la API
BASE_URL = "https://zara-boost-hackathon.nuwe.io"

# Número máximo de solicitudes concurrentes
MAX_CONCURRENT_REQUESTS = 6  # Puedes ajustar este valor según tus necesidades

# Variable global para controlar la interrupción
interrumpido = False

# Función para obtener todos los user_id
async def obtener_lista_user_ids():
    print("Iniciando la obtención de user_ids...")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BASE_URL}/users") as response:
            if response.status == 200:
                print("user_ids obtenidos correctamente.")
                user_ids = await response.json()  # Retorna la lista de user_id
                # Guardar los user_ids en un archivo de texto
                with open("user_ids.txt", "w") as f:
                    for user_id in user_ids:
                        f.write(f"{user_id}\n")
                return user_ids
            else:
                print(f"Error al obtener user_ids: {response.status}")
                return []

# Función para obtener los detalles de un usuario
# Función para obtener los detalles de un usuario con manejo de errores y reintentos
async def obtener_datos_usuario(session, user_id, semaphore, writer, user_ids_procesados):
    global interrumpido
    async with semaphore:  # Limitar el número de solicitudes concurrentes
        if interrumpido:
            return  # Si el proceso fue interrumpido, detener el procesamiento

        if user_id in user_ids_procesados:
            print(f"El usuario {user_id} ya fue procesado, omitiendo...")
            return  # Si el usuario ya fue procesado, no hacer nada

        intentos = 3  # Número de reintentos permitidos
        tiempo_espera = 1  # Tiempo inicial de espera entre reintentos (en segundos)

        for intento in range(1, intentos + 1):
            try:
                print(f"Iniciando la obtención de datos para el usuario {user_id}, intento {intento}...")
                async with session.get(f"{BASE_URL}/users/{user_id}", timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        print(f"Datos obtenidos para el usuario {user_id}.")
                        datos_usuario = await response.json()  # Retorna el diccionario con los detalles

                        # Extraer los datos necesarios
                        user_id = datos_usuario["user_id"]
                        country = datos_usuario["values"]["country"][0] if datos_usuario["values"]["country"] else None
                        R = datos_usuario["values"]["R"][0] if datos_usuario["values"]["R"] else None
                        F = datos_usuario["values"]["F"][0] if datos_usuario["values"]["F"] else None
                        M = datos_usuario["values"]["M"][0] if datos_usuario["values"]["M"] else None

                        # Escribir los resultados en el archivo CSV inmediatamente
                        writer.writerow([user_id, country, R, F, M])

                        # Marcar el usuario como procesado
                        user_ids_procesados.add(user_id)
                        
                        # Eliminar el user_id del archivo de texto
                        eliminar_user_id_del_txt(user_id)
                        return  # Salir del bucle si fue exitoso
                    else:
                        print(f"Error al obtener datos para el usuario {user_id}: {response.status}")
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Error al obtener datos del usuario {user_id}: {e}. Reintentando en {tiempo_espera} segundos...")
                await asyncio.sleep(tiempo_espera)
                tiempo_espera *= 2  # Incrementar el tiempo de espera (retroceso exponencial)
        print(f"No se pudo obtener los datos del usuario {user_id} después de {intentos} intentos.")
# Función para eliminar el user_id del archivo de texto
def eliminar_user_id_del_txt(user_id):
    # Leer todos los user_ids del archivo
    with open("user_ids.txt", "r") as f:
        user_ids = f.readlines()

    # Eliminar el user_id procesado
    with open("user_ids.txt", "w") as f:
        for line in user_ids:
            if line.strip() != user_id:  # Solo escribir si el user_id no es el que estamos eliminando
                f.write(line)

# Función para cargar los user_ids desde el archivo de texto
def cargar_user_ids_desde_txt():
    if os.path.exists("user_ids.txt"):
        with open("user_ids.txt", "r") as f:
            return [line.strip() for line in f.readlines()]
    return []

# Función para crear el archivo CSV con los datos de todos los usuarios
async def crear_csv_con_datos_usuarios():
    global interrumpido
    print("Iniciando el proceso de obtención de user_ids...")
    
    # Intentar cargar los user_ids desde el archivo de texto
    user_ids = cargar_user_ids_desde_txt()

    if not user_ids:
        print("No se encontraron user_ids en el archivo. Obteniendo desde la API...")
        user_ids = await obtener_lista_user_ids()  # Obtener todos los user_id si no hay en el archivo
        if not user_ids:
            print("No se encontraron user_ids para procesar.")
            return

    # Comprobar si el archivo CSV ya existe para evitar duplicados
    user_ids_procesados = set()
    if os.path.exists("usuarios.csv"):
        # Leer los user_id ya procesados desde el archivo CSV
        with open("usuarios.csv", mode="r", newline="") as file:
            reader = csv.reader(file)
            next(reader)  # Saltar los encabezados
            for row in reader:
                user_ids_procesados.add(row[0])  # Asumiendo que el user_id es la primera columna

    print("Iniciando la creación del archivo CSV...")
    # Abrir el archivo CSV en modo append
    with open("usuarios.csv", mode="a", newline="") as file:
        writer = csv.writer(file)
        
        # Escribir los encabezados solo si el archivo está vacío
        file.seek(0, 2)  # Moverse al final del archivo
        if file.tell() == 0:
            writer.writerow(["user_id", "country", "R", "F", "M"])

        # Usamos un ClientSession para hacer las solicitudes de manera eficiente
        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Limitar solicitudes concurrentes

            # Crear una lista de tareas para obtener los datos de todos los usuarios de manera concurrente
            tasks = []
            for user_id in user_ids:
                tasks.append(obtener_datos_usuario(session, user_id, semaphore, writer, user_ids_procesados))

            print("Esperando a que todas las tareas se completen...")
            try:
                # Esperar a que todas las tareas se completen
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                print("Proceso cancelado. Guardando los datos actuales...")
                interrumpido = True  # Marcar como interrumpido para que las tareas se detengan correctamente

    print("Proceso finalizado. CSV creado correctamente.")

# Manejo de la interrupción (CTRL+C) para guardar los datos en caso de interrupción
def signal_handler(sig, frame):
    global interrumpido
    print("\nInterrupción recibida. Guardando los datos...")
    interrumpido = True  # Marcar el proceso como interrumpido
    # No usar exit() directamente, lo que garantiza que el flujo asincrónico termine correctamente

# Asignar el manejador de señal para la interrupción
signal.signal(signal.SIGINT, signal_handler)

# Ejecutar la función asincrónica
if __name__ == "__main__":
    asyncio.run(crear_csv_con_datos_usuarios())
