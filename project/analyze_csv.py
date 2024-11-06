import os
import csv

def analizar_archivos_csv(directorio):
    datos_archivos = []

    for archivo in os.listdir(directorio):
        if archivo.endswith('.csv'):
            ruta_archivo = os.path.join(directorio, archivo)

            with open(ruta_archivo, 'r', encoding='latin1') as f:
                num_lineas = sum(1 for linea in f)

            tamano_archivo = os.path.getsize(ruta_archivo) / (1024 * 1024)

            datos_archivos.append([archivo, num_lineas, tamano_archivo])

    print("{:<30} {:<15} {:<10}".format("Nombre del Archivo", "Número de Líneas", "Tamaño (MB)"))
    print("="*55)
    for datos in datos_archivos:
        print("{:<30} {:<15} {:<10}".format(datos[0], datos[1], datos[2]))

directorio = r'C:\Users\nicolas\Desktop\U\15vo\Sistemas Operativos\Proyecto3\datasets'
analizar_archivos_csv(directorio)
