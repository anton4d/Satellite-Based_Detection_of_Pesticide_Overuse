from Database.SQLHandler import SQLHandler
import dotenv
import os
import argparse
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy
import time

def load_env():
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)

def filter_cloudy_dates(datadict, max_cloud_cover):
    bad_dates = [
        date for date, data in datadict.items()
        if any(float(cc.strip()) >= max_cloud_cover for cc in data["CloudCover"].split(",") if cc.strip())
    ]
    for date in bad_dates:
        datadict.pop(date)
    return datadict

def SavePlot(FileName,outputDir):
    os.makedirs(outputDir, exist_ok=True)
    plt.savefig(os.path.join(outputDir, FileName))
    plt.close()

def setupPlot(title):
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)
    plt.ylabel("NDVI")
    plt.xlabel("Time")
    plt.title(title)
    plt.legend
    plt.grid(True)
    plt.ylim(-0.5, 1)

def dataModeswitch(datadict,dataMode):
    if dataMode == "min":
            ndvi_data = [data["minNdvi"] for data in datadict.values()]
    elif dataMode == "max":
        ndvi_data = [data["MaxNdvi"] for data in datadict.values()]
    else:
        ndvi_data = [data["AverageNdvi"] for data in datadict.values()]
    return ndvi_data

def plot_individual_ndvi(id_list, db_handler, from_date, to_date, max_cloud_cover, output_dir):
    print("start the method plot_individual_ndvi")
    for field_id in id_list:
        datadict = db_handler.GetallNdviDataBasedOnIdAndDateRange(field_id, from_date, to_date)
        datadict = filter_cloudy_dates(datadict, max_cloud_cover)

        dates = list(datadict.keys())
        min_ndvi = [data["minNdvi"] for data in datadict.values()]
        max_ndvi = [data["MaxNdvi"] for data in datadict.values()]
        avg_ndvi = [data["AverageNdvi"] for data in datadict.values()]

        plt.figure(figsize=(10, 5))
        plt.plot(dates, min_ndvi, label="Min NDVI", linestyle="--", marker="o")
        plt.plot(dates, max_ndvi, label="Max NDVI", linestyle="--", marker="s")
        plt.plot(dates, avg_ndvi, label="Avg NDVI", linestyle="-", marker="d")

        title = f"NDVI for Field {field_id} MaxCloud:{max_cloud_cover}"
        setupPlot(title)
        filename = f"Ndvi_Field_{field_id}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
        SavePlot(filename, output_dir)

def plot_all_ndvi(id_list, db_handler, from_date, to_date, max_cloud_cover, data_mode, output_dir):
    print("start the method plot_all_ndvi")
    plt.figure(figsize=(10, 5))
    for field_id in id_list:
        datadict = db_handler.GetallNdviDataBasedOnIdAndDateRange(field_id, from_date, to_date)
        datadict = filter_cloudy_dates(datadict, max_cloud_cover)

        dates = list(datadict.keys())
        ndvi_data = dataModeswitch(datadict, data_mode)

        plt.plot(dates, ndvi_data, color="gray", alpha=0.6)


    title= f"NDVI ({data_mode}) for All Fields from {from_date} to {to_date} MaxCloud:{max_cloud_cover}"
    setupPlot(title)
    filename = f"Ndvi_All_{data_mode}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
    SavePlot(filename, output_dir)

def Plot_all_ndvi_with_and_average_line(db_handler,from_datestr,to_datestr,max_cloud_cover,data_mode,output_dir):
    print("start the method Plot_all_ndvi_with_and_average_line")
    from_date = datetime.strptime(from_datestr, "%Y-%m-%d")
    to_date = datetime.strptime(to_datestr, "%Y-%m-%d")
    dateList = [from_date + timedelta(days=i) for i in range((to_date - from_date).days + 1)]
    xavglist = []
    yavglist = []
    xlist = []
    ylist = []
    for date in dateList:
        datadict = db_handler.GetAllNdviBasedOnDate(datetime.strftime(date,"%Y-%m-%d"))
        datadict = filter_cloudy_dates(datadict, max_cloud_cover)
        if len(list(datadict.keys())) <= 1:
            continue
        ndvi_data = dataModeswitch(datadict, data_mode)
        
        dates = [data["Date"] for data in datadict.values()]
        if ndvi_data:
            yavglist.append(numpy.average(ndvi_data))
            xavglist.append(date)
            xlist.append(dates)
            ylist.append(ndvi_data)
    flat_x = [item for sublist in xlist for item in sublist]
    flat_y = [item for sublist in ylist for item in sublist]
    plt.figure(figsize=(10,5))
    title = f"NDVI ({data_mode}) for All Fields from {from_datestr} to {to_datestr} with avg line, MaxCloud:{max_cloud_cover}"
    setupPlot(title)
    plt.scatter(flat_x,flat_y,label="NDVI values", alpha=0.6)
    plt.plot(xavglist,yavglist,color="red", label="Average NDVI")
    filename = f"Ndvi_All_Avg_Line_{data_mode}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
    SavePlot(filename, output_dir)

def main():
    parser = argparse.ArgumentParser(description="NDVI Graph Generator")
    parser.add_argument("Mode", choices=["indMinMaxAvg", "All","AllAvgLine"], help="Graph mode")
    parser.add_argument("MaxCloud", type=float, help="Maximum allowed cloud cover")
    parser.add_argument("ID", help="Field ID to graph, or 'all' for all fields")
    parser.add_argument("DataMode", choices=["min", "max", "avg"], help="NDVI type for 'All' mode")
    parser.add_argument("FromDate",help="fromDate in format yyyy-mm-dd")
    parser.add_argument("ToDate",help="Todate in format yyyy-mm-dd")
    parser.add_argument("OutputDir",help="The output dir that the plot is saved to")
    args = parser.parse_args()
    load_env()

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )

    from_date = args.FromDate#"2020-07-01"
    to_date = args.ToDate#"2020-12-01"
    output_dir = args.OutputDir#plot"
    Datamode = args.Datamode
    maxCloud = args.MaxCloud
    if args.ID == "all":
        id_list = db_handler.GetAllFeildIdsFromNdviData()
    else:
        id_list = [int(args.ID)]

    starttime = time.time()
    if args.Mode == "indMinMaxAvg":
        plot_individual_ndvi(id_list, db_handler, from_date, to_date, maxCloud, output_dir)
    elif args.Mode == "All":
        plot_all_ndvi(id_list, db_handler, from_date, to_date, maxCloud, Datamode, output_dir)
    elif args.Mode == "AllAvgLine":
        Plot_all_ndvi_with_and_average_line(db_handler,from_date,to_date,maxCloud, Datamode ,output_dir)
    endTime = time.time()
    print(f"The plot process took {endTime-starttime} secounds")

if __name__ == "__main__":
    main()
