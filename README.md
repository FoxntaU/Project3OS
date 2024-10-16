# PROYECTO 1 SISTEMAS OPERATIVOS 

Integrantes:
- SAMUEL ACOSTA ARISTIZABAL
- DAVID ALBERTO CUADROS MARIÑO
- NICOLAS TOVAR ALMANZA

## ACTIVIDADES

La práctica consiste en que se lean los 10 archivos .csv del dataset en memoria de tres maneras diferentes a través del mismo programa.

## 
dataload – Lector de datos.

## SINOPSIS
# Cómo Ejecutar

Antes de ejecutar el script, asegúrate de tener instalados los siguientes requisitos:

1. **Python 3.x**: Puedes descargar Python desde [python.org](https://www.python.org/downloads/).
2. **Crear el venv en el folder Project1OS**:

   ```sh
   python -m venv venv

3. **Activa el entorno virtual**
   
   Windows

   ```sh
   .\venv\Scripts\activate
   ```
   macOS/Linux
   ```sh
   venv/bin/activate

4. **Instalar dependencias**
   ```sh
   pip install -r requirements.txt

5. **Entrar al folder project y ejecutar**
   ```sh
   python dataload.py [OPCIONES -s, -m or nothing] -f "C:\Users\nicolas\Desktop\Sistemas Operativos\Project1OS\datasets"


## DESCRIPCIÓN

Se verifica si `FOLDER` es una carpeta que está en la misma ruta donde está el ejecutable. Por cada archivo con extensión `.csv` que encuentre en la carpeta, debe cargarlo en memoria en un `ArrayList` o similar. Cuando termine el proceso, se muestra un mensaje de resumen que debe indicar:

1. Hora de inicio del programa.
2. Hora de inicio de la carga del primer archivo.
3. Hora de finalización de la carga del último archivo.
4. Tabla de resumen con la duración de la carga de todos los archivos según el orden que hayan sido procesados.
5. Tiempo (en formato mm:ss) que tomó todo el proceso.

Si no tiene OPCIONES habilitadas, se lee cada archivo uno a la vez de manera secuencial hasta que termine.

## OPCIONES 
- **-s** : Indica al programa que lea los n archivos con extensión `.csv` que encuentre en la carpeta al tiempo, donde cada archivo que deba leer debe asignarse a un proceso independiente asignado en el mismo core donde corre el dataload inicial.

- **-m** : Al igual que el `-s`, cada proceso recibe un archivo para ser leído pero cada proceso puede asignarse a cualquiera de los cores que tenga el computador disponible.

## Estado de salida del proceso
- **0** : Si el proceso termina OK.
- **1** : Si el proceso termina con errores.

El dataset lo pueden descargar de Interactiva Virtual.

## CONSIDERACIONES GENERALES

1. El desarrollo de la práctica puede ser individual o en equipos de máximo tres personas.
2. La entrega de la práctica se realizará entregando los fuentes en un archivo y el informe por el buzón recepción de trabajos de Eafit Interactiva (cualquier otro medio no será admitido).
3. Se debe informar al profesor a más tardar el 23 de Julio a las 6:00 p.m. los integrantes del equipo.
4. El informe final es una presentación que deberá contener una breve descripción de cómo funciona el programa, tablas o gráficos donde se muestre la ejecución de su programa en diferentes máquinas describiendo de cada una el tipo de procesador, cantidad de memoria RAM y sistema operativo que tienen instalado (puede ser en varios) y unas conclusiones que ustedes hagan sobre los datos obtenidos.
5. La práctica se puede realizar en cualquier lenguaje de programación. En el informe deben informar cual es la versión del compilador o del runtime que están usando.
6. Cada semana los jueves, se sacará un espacio de 10 a 15 minutos al inicio de la clase para hablar de la práctica y resolver dudas.
7. Criterios de evaluación (ver Anexo 1).
