import time
import psutil
import os
from rich import print
from rich.console import Console
from rich.text import Text

console = Console()

def display_usage(cpu_usages, mem_usage, bars=50):
    mem_percent = (mem_usage / 100.0)
    mem_bar = '█' * int(mem_percent * bars) + '-' * (bars - int(mem_percent * bars))
    
    # Display memory usage
    console.print(f"Mem Usage: |{mem_bar}| {mem_usage:.2f}%", style="bold green" if mem_usage <= 90 else "bold red")
    
    # Display CPU usage for each core
    for i, cpu_usage in enumerate(cpu_usages):
        cpu_percent = (cpu_usage / 100.0)
        cpu_bar = '█' * int(cpu_percent * bars) + '-' * (bars - int(cpu_percent * bars))
        style = "bold green" if cpu_usage <= 90 else "bold red"
        console.print(f"Core {i}: |{cpu_bar}| {cpu_usage:.2f}%", style=style)

while True:
    os.system('cls')  # Clear the console on Windows
    cpu_usages = psutil.cpu_percent(percpu=True)
    mem_usage = psutil.virtual_memory().percent
    display_usage(cpu_usages, mem_usage, 30)
    time.sleep(0.5)