import subprocess
import argparse



def Start_Workers(Num_Workers,Logfile,Log_path):
    for i in range(Num_Workers):
        subprocess.Popen(['python3', 'Download_Worker.py', f'{Logfile}{i}', f'{Log_path}'])
    print(f"Started {Num_Workers} workers.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("NumberWorkers",type=int, help="The number of workers to be Created")
    parser.add_argument("LogFile", nargs="?", default="tiff_process.log", help="Log file name")
    parser.add_argument("LogFolder", nargs="?",default="Logs",help="The folder the log files should be saved to (default: Logs")
    args = parser.parse_args()
    Start_Workers(args.NumberWorkers, args.LogFile,args.LogFolder)


