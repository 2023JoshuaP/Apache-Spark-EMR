import os

def dividir_archivo_seguro_spark(archivo_entrada, tamano_mb=100):
    limite_bytes = tamano_mb * 1024 * 1024 
    
    numero_archivo = 1
    bytes_actuales = 0
    
    carpeta_salida = "datos_spliteados"
    if not os.path.exists(carpeta_salida):
        os.makedirs(carpeta_salida)
        
    nombre_salida = f"{carpeta_salida}/doc_parte{numero_archivo}.txt"
    archivo_salida = open(nombre_salida, 'w', encoding='utf-8')
    
    print(f"Iniciando la división de {archivo_entrada} en bloques de {tamano_mb} MB...")

    try:
        with open(archivo_entrada, 'r', encoding='utf-8') as f:
            for linea in f:
                archivo_salida.write(linea)
                bytes_actuales += len(linea.encode('utf-8'))
                
                if bytes_actuales >= limite_bytes:
                    archivo_salida.close()
                    print(f"[{nombre_salida}] completado con {bytes_actuales / (1024*1024):.2f} MB.")
                    
                    numero_archivo += 1
                    bytes_actuales = 0
                    nombre_salida = f"{carpeta_salida}/doc_parte{numero_archivo}.txt"
                    archivo_salida = open(nombre_salida, 'w', encoding='utf-8')
                    
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {archivo_entrada}")
    finally:
        archivo_salida.close()
        print(f"\n¡División completada con éxito! Se generaron {numero_archivo} archivos.")

dividir_archivo_seguro_spark('wikipedia/wikipedia.txt', tamano_mb=100)