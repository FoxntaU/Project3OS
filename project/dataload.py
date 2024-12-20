import os
import sys
from datetime import datetime
from rich import print
from rich.table import Table
import argparse
import pandas as pd
import multiprocessing
import psutil
import platform
import mmap
import threading
from io import StringIO

#LECTURA PANDAS
def read_files(file_path, block_size=4096):
    start_time = datetime.now()
    pid = os.getpid()

    try:
        chunks = pd.read_csv(file_path, encoding='latin1', chunksize=block_size)
        data = pd.concat(chunks, ignore_index=True)
    except pd.errors.EmptyDataError:
        data = pd.DataFrame()  

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    memory_virtual = psutil.Process(pid).memory_info().vms
    rss_memory = psutil.Process(pid).memory_info().rss

    return data, start_time, end_time, duration, pid, memory_virtual, rss_memory

def check_cpu_affinity():
    p = psutil.Process(os.getpid())
    affinity = p.cpu_affinity()
    print(f"El proceso está asignado a los núcleos: {affinity}")

def analize_data(data_dict):
    all_data = pd.concat(data_dict.values(), ignore_index=True)

    for year in [2017, 2018]:
        yearly_data = all_data[all_data['publish_time'].str.startswith(str(year))]
        if not yearly_data.empty:
            popular_videos = yearly_data.sort_values(by='views', ascending=False).head(2)
            table = Table(title=f"Dos videos más populares globalmente en {year}")
            table.add_column("Título", justify="left", style="cyan")
            table.add_column("ID", style="yellow")
            table.add_column("Vistas", style="magenta")
            for _, row in popular_videos.iterrows():
                table.add_row(row['title'], str(row['video_id']), str(row['views']))
            print(table)

            unpopular_videos = yearly_data.sort_values(by='views', ascending=True).head(2)
            table = Table(title=f"Dos videos más impopulares globalmente en {year}")
            table.add_column("Título", justify="left", style="cyan")
            table.add_column("ID", style="yellow")
            table.add_column("Vistas", style="magenta")
            for _, row in unpopular_videos.iterrows():
                table.add_row(row['title'], str(row['video_id']), str(row['views']))
            print(table)

    for file_path, data in data_dict.items():
        print(f"\nAnalizando el archivo: {file_path}")
        if not data.empty:
            for year in [2017, 2018]:
                region = os.path.basename(file_path)[:2]  
                data['region'] = region
                yearly_data = data[data['publish_time'].str.startswith(str(year))]
                region_data = yearly_data[yearly_data['region'] == region]

                if not region_data.empty:
                    popular_region_video = region_data.sort_values(by='views', ascending=False).head(2)
                    table = Table(title=f"Dos videos más populares en la región {region} en {year}")
                    table.add_column("Título", justify="left", style="cyan")
                    table.add_column("ID", style="yellow")
                    table.add_column("Vistas", style="magenta")
                    for _, row in popular_region_video.iterrows():
                        table.add_row(row['title'], str(row['video_id']), str(row['views']))
                    print(table)

                    unpopular_region_video = region_data.sort_values(by='views', ascending=True).head(2)
                    table = Table(title=f"Dos videos más impopulares en la región {region} en {year}")
                    table.add_column("Título", justify="left", style="cyan")
                    table.add_column("ID", style="yellow")
                    table.add_column("Vistas", style="magenta")
                    for _, row in unpopular_region_video.iterrows():
                        table.add_row(row['title'], str(row['video_id']), str(row['views']))
                    print(table)

def read_files_sequentially(file_paths):
    print(f"\nLeyendo los archivos en [bold cyan] sequentially [/bold cyan] mode")
    start_time_program = datetime.now()
    p = psutil.Process(os.getpid())

    cpu= [p.cpu_affinity() for _ in range(len(file_paths))]
    data_list = []
    durations = []
    start_times = []
    end_times = []
    pids = []
    memory_virtuals = []
    memory_rss =[]
    data_dict = {}

    for i,file_path in enumerate(file_paths):
        data, start_time, end_time, duration, pid, memory_virtual, rss_memory  = read_files(file_path)
        if data is not None:
            data_dict[file_paths[i]] = data
            data_list.append(data)
            start_times.append(start_time)
            end_times.append(end_time)
            durations.append(duration)
            pids.append(pid)
            memory_virtuals.append(memory_virtual)
            memory_rss.append(rss_memory)
        else: 
            print(f"[bold red]Error:[/bold red] El archivo {file_path} no pudo ser leído.")
        
    end_time_program = datetime.now()

    analize_data(data_dict)
    
    print_end("sequentially", start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss, cpu )
    save_to_csv("sequentially", start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss)


def get_least_busy_core():
    cpu_percentages = psutil.cpu_percent(percpu=True)
    least_busy_core = cpu_percentages.index(min(cpu_percentages))
    return least_busy_core


def read_files_in_unique_process(file_paths):
    print(f"\nLeyendo los archivos en [bold cyan] unique process [/bold cyan] mode")
    start_time_program = datetime.now()

    p = psutil.Process(os.getpid())
    p.cpu_affinity([get_least_busy_core()])
    check_cpu_affinity()

    data_dict = {}
    data_list = []
    start_times = []
    end_times = []
    durations = []
    pids = []
    memory_virtuals = []
    memory_rss =[]
    
    def thread_task(file_path, thread_results, index):
        result = read_files(file_path)  
        thread_results[index] = result  

    thread_results = [None] * len(file_paths)
    threads = []

    for i, file_path in enumerate(file_paths):
        t = threading.Thread(target=thread_task, args=(file_path, thread_results, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    for i, result in enumerate(thread_results):
        if result is not None:
            data, start_time, end_time, duration, pid, memory_virtual, rss_memory = result
            data_list.append(data)
            data_dict[file_paths[i]] = data
            start_times.append(start_time)
            end_times.append(end_time)
            durations.append(duration)
            pids.append(pid)
            memory_virtuals.append(memory_virtual)
            memory_rss.append(rss_memory)

    analize_data(data_dict)

    end_time_program = datetime.now()

    cpu = [p.cpu_affinity()[0] for _ in range(len(file_paths))]
    print(cpu)
    
    print_end("unique process", start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss, cpu)
    save_to_csv("unique_process", start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss)


def read_file_chunk(file_path, start_line, num_lines, is_first_chunk=False):
    # print(f"Reading lines {start_line} to {start_line + num_lines} from {file_path}")
    try:
        if is_first_chunk:
            return pd.read_csv(file_path, skiprows=start_line, nrows=num_lines, encoding='latin1', header=0)
        else:
            return pd.read_csv(file_path, skiprows=start_line, nrows=num_lines, encoding='latin1', header=None)
    except pd.errors.EmptyDataError:
        print(f"Empty data error for lines {start_line} to {start_line + num_lines} in {file_path}")
        return pd.DataFrame()

def process_file(file_path):
    data_chunks = {}
    threads = []
    num_lines_per_thread = 20000
    chunk_size = 10**6
    num_lines_total = 0
    # Calcular el número total de líneas
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, encoding='latin1'):
        num_lines_total += len(chunk)
    num_threads = (num_lines_total + num_lines_per_thread - 1) // num_lines_per_thread
    # print(f"\nLeyendo el archivo: {file_path}\nNúmero total de líneas: {num_lines_total}\nNúmero de hilos: {num_threads}")
    def thread_function(index, start_line, num_lines, is_first_chunk):
        chunk = read_file_chunk(file_path, start_line, num_lines, is_first_chunk)
        # print(f"Thread {index} finished reading lines {start_line} to {start_line + num_lines}")
        # print(chunk)
        data_chunks[index] = chunk  
    # Crear y lanzar hilos
    for i in range(num_threads):
        start_line = i * num_lines_per_thread   
        end_line = min((i + 1) * num_lines_per_thread, num_lines_total+1)
        is_first_chunk = i == 0
        thread = threading.Thread(target=thread_function, args=(i, start_line, end_line - start_line, is_first_chunk))
        threads.append(thread)
        thread.start()
    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()
    # Concatenar los fragmentos en el orden correcto
    ordered_chunks = []
    for i in sorted(data_chunks.keys()):
        chunk = data_chunks[i]
        if not chunk.empty:
            if i == 0:
                column_labels = chunk.columns
            else:
                chunk.columns = column_labels
            ordered_chunks.append(chunk)
    combined_df = pd.concat(ordered_chunks, ignore_index=True)
    return combined_df


def wrapper_process(file_path):
    """Envolver la función process_file para medir tiempos y recursos."""
    start_time = datetime.now()
    pid = os.getpid()
    p = psutil.Process(pid)
    cpu = p.cpu_affinity()
    print(f"Procesando el archivo {file_path} en el core {cpu}")

    data = process_file(file_path)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    memory_virtual = psutil.Process(pid).memory_info().vms
    rss_memory = psutil.Process(pid).memory_info().rss

    return data, start_time, end_time, duration, pid, memory_virtual, rss_memory, cpu

def read_files_in_multi_process(file_paths):
    print(f"\nLeyendo los archivos en [bold cyan] multi process [/bold cyan] mode")
    start_time_program = datetime.now()
    
    p = psutil.Process(os.getpid())
    p.cpu_affinity(list(range(psutil.cpu_count())))  
    check_cpu_affinity()

    data_dict = {}
    data_list = []
    durations = []
    start_times = []
    end_times = []
    pids = []
    memory_virtuals = []
    memory_rss = []
    cpu = []

    with multiprocessing.Pool() as pool:
        results = pool.map(wrapper_process, file_paths)

    for file_path, result in zip(file_paths, results):
        data, start_time, end_time, duration, pid, memory_virtual, rss_memory, affinity = result
        if data is not None:
            data_dict[file_path] = data
            data_list.append(data)
        start_times.append(start_time)
        end_times.append(end_time)
        durations.append(duration)
        pids.append(pid)
        memory_virtuals.append(memory_virtual)
        memory_rss.append(rss_memory)
        cpu.append(affinity)
    
    analize_data(data_dict)

    end_time_program = datetime.now()
    
    print_end("multi process", start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss, cpu)
    save_to_csv("multi_process", start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss)

def show_info_sys():
    print(f"[bold cyan]Tipo de procesador:[/bold cyan] {platform.processor()}")
    print(f"[bold cyan]Cantidad de memoria RAM:[/bold cyan] {psutil.virtual_memory().total / (1024 ** 3):.2f} GB")
    print(f"[bold cyan]Cantidad de memoria swap:[/bold cyan] {psutil.swap_memory().total / (1024 ** 3):.2f} GB")
    print(f"[bold cyan]Numero total de paginas (4 KB):[/bold cyan] {psutil.virtual_memory().total / 2**12:.2f}")
    print(f"[bold cyan]Sistema operativo:[/bold cyan] {platform.system()} {platform.release()}")
    print(f"[bold cyan]Numero de CPUs:[/bold cyan] {multiprocessing.cpu_count()}")
    

def print_end(mode, start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss, cpu):
    print("\n")
    print("[bold]Información del sistema[/bold]")
    show_info_sys()
    table = Table(title="Resumen de carga de archivos")
    table.add_column("Archivo", justify="left", style="cyan", no_wrap=True)
    table.add_column("PID", style="yellow")
    table.add_column("CPU", style="magenta")
    table.add_column("Hora de inicio", style="magenta")
    table.add_column("Hora de finalización", style="magenta")
    table.add_column("Duración (s)", style="green")
    table.add_column("Memoria Virtual (bytes)", style="red") 
    table.add_column("Memoria RRS (bytes)", style="red") 

    for i, file_path in enumerate(file_paths):
        table.add_row(
            os.path.basename(file_path),
            str(pids[i]),
            str(cpu[i]),
            start_times[i].strftime("%H:%M:%S.%f"),
            end_times[i].strftime("%H:%M:%S.%f"),
            f"{durations[i]:.6f}",
            f"{memory_virtuals[i]:,}",
            f"{memory_rss[i]:,}"
        )

    start_time_str = start_times[0].strftime("%H:%M:%S.%f")
    end_time_str = end_times[-1].strftime("%H:%M:%S.%f")
    duration_total = (end_time_program - start_time_program).total_seconds()
    duration_total_str = f"{duration_total:.6f}"

    print(f"\n[bold cyan]Modo:[/bold cyan] {mode}")
    print(f"[bold magenta]Hora de inicio del programa:[/bold magenta] {start_time_program.strftime('%H:%M:%S.%f')}")
    print(f"[bold magenta]Hora de inicio de la carga del primer archivo:[/bold magenta] {start_time_str}")
    print(f"[bold magenta]Hora de finalización de la carga del último archivo:[/bold magenta] {end_time_str}")
    print(f"[bold magenta]Hora de finalización del programa:[/bold magenta] {end_time_program.strftime('%H:%M:%S.%f')}")
    print(table)
    print(f"[bold green]Tiempo total del proceso:[/bold green] {duration_total_str} segundos")

def save_to_csv(mode, start_time_program, end_time_program, file_paths, start_times, end_times, durations, pids, memory_virtuals, memory_rss):
    data = {
        "Archivo": [os.path.basename(fp) for fp in file_paths],
        "PID": pids,
        "Hora de inicio": [st.strftime("%H:%M:%S.%f") for st in start_times],
        "Hora de finalización": [et.strftime("%H:%M:%S.%f") for et in end_times],
        "Duración (s)": [f"{duration:.6f}" for duration in durations],
        "Memoria Virtual (bytes)": memory_virtuals,
        "Memoria RSS (bytes):": memory_rss
    }
    df = pd.DataFrame(data)
    df["Modo"] = mode
    df["Hora de inicio del programa"] = start_time_program.strftime("%H:%M:%S.%f")
    df["Hora de inicio de la carga del primer archivo"] = start_times[0].strftime("%H:%M:%S.%f")
    df["Hora de finalización de la carga del último archivo"] = end_times[-1].strftime("%H:%M:%S.%f")
    df["Hora de finalización del programa"] = end_time_program.strftime("%H:%M:%S.%f")
    df["Tiempo total del proceso (s)"] = f"{(end_time_program - start_time_program).total_seconds():.6f}"
    output_file = f"{mode}_summary_{end_time_program.strftime('%H%M%S')}.csv"
    df.to_csv(output_file, index=False)
    print(f"\n[bold green]Resumen guardado en:[/bold green] {output_file}\n")
    
def main():
    parser = argparse.ArgumentParser(description="dataload - Lector de datos.")
    parser.add_argument("-f", "--folder", required=True, help="Carpeta con archivos CSV")
    parser.add_argument("-s", "--unique-process", action="store_true", help="Leer archivos en paralelo en el mismo core")
    parser.add_argument("-m", "--multi-process", action="store_true", help="Leer archivos en paralelo en múltiples cores")
    args = parser.parse_args()
    
    if not os.path.isdir(args.folder):
        print("[bold red]Error:[/bold red] La carpeta especificada no existe.")
        sys.exit(1)

    if args.unique_process and args.multi_process:
        print("[bold red]Error:[/bold red] No se pueden usar las opciones -s y -m al mismo tiempo.")
        sys.exit(1)

    file_paths = [os.path.join(args.folder, file) for file in os.listdir(args.folder) if file.endswith('.csv')]
    
    if not file_paths:
        print("[bold red] Error: [/bold red] No se encontraron archivos csv en la carpeta especifica.")
        sys.exit(1)

    if args.unique_process:
        read_files_in_unique_process(file_paths)
    elif args.multi_process:
        read_files_in_multi_process(file_paths)
    else:
        read_files_sequentially(file_paths)

if __name__ == "__main__":
    main()