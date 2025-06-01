import subprocess
import argparse



def start_workers(num_workers,Logfile,Logpath):
    for i in range(num_workers):
        subprocess.Popen(['python3', 'Process_Worker.py', f'{Logfile}{i}', f'{Logpath}'])
    print(f"Started {num_workers} workers.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("NumberWorkers",type=int, help="The number of workers to be Created")
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")
    parser.add_argument("LogFolder", nargs="?",default="Logs",help="The folder the log files should be saved to (default: Logs")
    args = parser.parse_args()
    start_workers(args.NumberWorkers, args.LogFile,args.LogFolder)


