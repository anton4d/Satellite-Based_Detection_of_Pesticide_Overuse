from Database.SQLHandler import SQLHandler
import dotenv, os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def main():
    dotenvFile = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenvFile)

    db_handler = SQLHandler(
        host=os.getenv("SQLHOST"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        database=os.getenv("DBDB")
    )
    

    fromdate = "2020-07-01"
    toDate= "2020-12-01"
    maxCloudcover = 75
    output_dir = "plot"
    idList = db_handler.GetAllFeildIdsFromNdviData()
    for id in idList:

        datadict =  db_handler.GetallNdviDataBasedOnIdAndDateRange(id,fromdate,toDate)

        badDataList = []
        for date, data in list(datadict.items()):

            

            cloudcoverlist = [
                float(x.strip()) for x in data["CloudCover"].split(',') if x.strip()
            ]

            for cloudCover in cloudcoverlist:
                if cloudCover >= maxCloudcover:
                    badDataList.append(date)
                    break

        for badData in badDataList:
            datadict.pop(badData)
        dates = list(datadict.keys())
        min_ndvi = [data["minNdvi"] for data in datadict.values()]
        max_ndvi = [data["MaxNdvi"] for data in datadict.values()]
        avg_ndvi = [data["AverageNdvi"] for data in datadict.values()]


        plt.figure(figsize=(10, 5))
        plt.plot(dates, min_ndvi, label="Min NDVI", linestyle="--", marker="o")
        plt.plot(dates, max_ndvi, label="Max NDVI", linestyle="--", marker="s")
        plt.plot(dates, avg_ndvi, label="Avg NDVI", linestyle="-", marker="d")


        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.xticks(rotation=45)

    
        plt.title(f"NDVI for {id} over {fromdate} to {toDate} with max cloud: {maxCloudcover}")
        plt.ylabel("NDVI")
        plt.xlabel("Time")
        plt.legend()
        plt.grid(True)
        plt.ylim(-0.5, 1)
        os.makedirs(output_dir, exist_ok=True)

        # Save the figure inside the "plot" directory
        plt.savefig(os.path.join(output_dir, f"Ndvi_for_{id}_from_{fromdate}_to_{toDate}withMaxCloud_{maxCloudcover}.png"))
        plt.close()

if __name__ == "__main__":
    main()