from Database.SQLHandler import SQLHandler
import dotenv
import os
import argparse
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy
import time
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

def load_env():
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)

def filter_cloudy_dates(datadict, FieldCropYearId, db_handler: SQLHandler, max_cloud_cover: int):
    return {
        date: data for date, data in datadict.items()
        if all(float(cc) <= max_cloud_cover for cc in db_handler.GetCloudCoversBasedOnFieldCropYearIdAndDate(FieldCropYearId, date))
    }
    
def filter_cloudy_ndvi_by_field(datadict, db_handler: SQLHandler, max_cloud_cover: int):
    filtered_dict = {}

    for field_crop_year_id, data in datadict.items():
        date = data["Date"]
        cloud_covers = db_handler.GetCloudCoversBasedOnFieldCropYearIdAndDate(field_crop_year_id, date)

        if all(float(cc) <= max_cloud_cover for cc in cloud_covers):
            filtered_dict[field_crop_year_id] = data

    return filtered_dict

def SavePlot(FileName,outputDir):
    os.makedirs(outputDir, exist_ok=True)
    plt.savefig(os.path.join(outputDir, FileName))
    plt.close()
    

def setupPlot(title,datatypeName):
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=30)
    plt.ylabel(f"{datatypeName}")
    plt.xlabel("Time")
    plt.title(title)
    plt.grid(True)

def dataModeswitch(datadict, dataMode):
   

    if dataMode not in ["Min", "Max", "Average", "Median"]:
        raise ValueError(f"Invalid data mode '{dataMode}'. Expected one of ['min', 'max', 'median', 'average'].")
    
    
    DataPoints = [data[dataMode] for data in datadict.values()]
    
    return DataPoints

def plot_individual_datapoints(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, output_dir,cropType,datatype,tiffFormat):
    print("start the method plot_individual_datapoints")
    for CropYearId in id_list:
        datadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatypeAndTiffFormat(CropYearId, from_date, to_date,datatype,tiffFormat)
        FilteredDatadict = filter_cloudy_dates(datadict,CropYearId,db_handler, max_cloud_cover)
        field_id = db_handler.GetFieldIdBasedOnFieldCropYearId(CropYearId)
        datatypeName = db_handler.GetDatatypeName(datatype)
        dates = list(FilteredDatadict.keys())
        min_Datapoints = [data["Min"] for data in FilteredDatadict.values()]
        max_Datapoints = [data["Max"] for data in FilteredDatadict.values()]
        avg_Datapoints = [data["Average"] for data in FilteredDatadict.values()]
        med_Datapoints = [data["Median"] for data in FilteredDatadict.values()]

        plt.figure(figsize=(10, 5))
        plt.plot(dates, min_Datapoints, label=f"Min {datatypeName}", linestyle="--", marker="o")
        plt.plot(dates, max_Datapoints, label=f"Max {datatypeName}", linestyle="-.", marker="s")
        plt.plot(dates, avg_Datapoints, label=f"Avg {datatypeName}", linestyle="-", marker="d")
        plt.plot(dates, med_Datapoints,label=f"Median {datatypeName}",linestyle=":", marker="p")
        plt.legend()
        title = f"{datatypeName} for Field {field_id} with croptype: {cropType} MaxCloud:{max_cloud_cover} tiffFormat:{tiffFormat}"
        setupPlot(title,datatypeName)
        filename = f"{datatypeName}_Field_{field_id}_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}_{tiffFormat}.png"
        SavePlot(filename, output_dir)

def plot_NdviAndCvi(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, output_dir,cropType,tiffFormat):
    print("start the method NDviAndCvi")
    for CropYearId in id_list:
        ndvi = 5
        cvi = 8
        ndviname= "Ndvi"
        cviName="Cvi"
        ndvidatadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatypeAndTiffFormat(CropYearId, from_date, to_date,ndvi,tiffFormat)
        cvidatadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatypeAndTiffFormat(CropYearId, from_date, to_date,cvi,tiffFormat)
        FilteredndviDatadict = filter_cloudy_dates(ndvidatadict,CropYearId,db_handler, max_cloud_cover)
        FilteredcviDatadict = filter_cloudy_dates(cvidatadict,CropYearId,db_handler, max_cloud_cover)
        field_id = db_handler.GetFieldIdBasedOnFieldCropYearId(CropYearId)
        
        ndvidates = list(FilteredndviDatadict.keys())
        ndviavg_Datapoints = [data["Average"] for data in FilteredndviDatadict.values()]

        cviDated = list(FilteredcviDatadict.keys())
        cviAvgDatapoints = [data["Average"] for data in FilteredcviDatadict.values()]
        plt.figure(figsize=(10, 5))
        plt.plot(ndvidates, ndviavg_Datapoints, label=f"Avg {ndviname}", linestyle="--", marker="o")
        plt.plot(cviDated, cviAvgDatapoints, label=f"Avg {cviName}", linestyle="-.", marker="s")
        plt.legend()
        title = f"{ndviname} vs {cviName} for Field {field_id} with croptype: {cropType} MaxCloud:{max_cloud_cover} tiffFormat:{tiffFormat}"
        setupPlot(title,f"{ndviname} and {cviName}")
        filename = f"{ndviname}_and_{cviName}_Field_{field_id}_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}_{tiffFormat}.png"
        SavePlot(filename, output_dir)
    
def plot_NdviAndNdre(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, output_dir,cropType,tiffFormat):
    print("start the method NDviAndCvi")
    for CropYearId in id_list:
        ndvi = 5
        Ndre = 9
        ndviname= "Ndvi"
        NdreName="Ndre"
        ndvidatadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatypeAndTiffFormat(CropYearId, from_date, to_date,ndvi,tiffFormat)
        Ndredatadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatypeAndTiffFormat(CropYearId, from_date, to_date,Ndre,tiffFormat)
        FilteredndviDatadict = filter_cloudy_dates(ndvidatadict,CropYearId,db_handler, max_cloud_cover)
        FilteredNdreDatadict = filter_cloudy_dates(Ndredatadict,CropYearId,db_handler, max_cloud_cover)
        field_id = db_handler.GetFieldIdBasedOnFieldCropYearId(CropYearId)
        
        ndvidates = list(FilteredndviDatadict.keys())
        ndviavg_Datapoints = [data["Average"] for data in FilteredndviDatadict.values()]

        cviDated = list(FilteredNdreDatadict.keys())
        cviAvgDatapoints = [data["Average"] for data in FilteredNdreDatadict.values()]
        plt.figure(figsize=(10, 5))
        plt.plot(ndvidates, ndviavg_Datapoints, label=f"Avg {ndviname}", linestyle="--", marker="o")
        plt.plot(cviDated, cviAvgDatapoints, label=f"Avg {NdreName}", linestyle="-.", marker="s")
        plt.legend()
        title = f"{ndviname} vs {NdreName} for Field {field_id} with croptype: {cropType} MaxCloud:{max_cloud_cover} tiffFormat:{tiffFormat}"
        setupPlot(title,f"{ndviname} and {NdreName}")
        filename = f"{ndviname}_and_{NdreName}_Field_{field_id}_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}_{tiffFormat}.png"
        SavePlot(filename, output_dir)

def plot_field_deviation_boxplots(db_handler: SQLHandler, from_date, to_date, datatype, max_cloud_cover, output_dir, cropType, dataMode):
    print("Starting plot_field_deviation_boxplots")
    datatypeName = db_handler.GetDatatypeName(datatype)


    data_dict = db_handler.GetallDataPointsBasedOnDateRangeAndDatatype(from_date, to_date, datatype)
    print(len(data_dict))
    filtered_data = {
        field_id: filter_cloudy_dates(date_dict, field_id, db_handler, max_cloud_cover)
        for field_id, date_dict in data_dict.items()
    }
    print(len(filtered_data))
    date_range = pd.date_range(start=from_date, end=to_date, freq='D')
    interpolated_fields = {}

    for field_id, date_data in filtered_data.items():
        if not date_data:
            continue
        try:
            values = dataModeswitch(date_data, dataMode)
            dates = list(date_data.keys())
            series = pd.Series(values, index=pd.to_datetime(dates)).sort_index()
            interpolated = series.reindex(date_range).interpolate(method='time')

            if interpolated.notna().sum() > 0:
                interpolated_fields[field_id] = interpolated
        except Exception as e:
            print(f"Skipping field {field_id} due to error: {e}")

    if not interpolated_fields:
        print("No valid data available after filtering and interpolation.")
        return


    all_df = pd.DataFrame(interpolated_fields)
    overall_avg = all_df.mean(axis=1)
    deviation_df = all_df.subtract(overall_avg, axis=0)


    deviation_df = deviation_df.dropna(axis=1, how='all')

    if deviation_df.empty:
        print("No numeric deviations to plot.")
        return


    total_deviation = deviation_df.abs().sum(axis=0)
    sorted_deviation = total_deviation.sort_values(ascending=False)

   
    labels = sorted_deviation.index.astype(str)
    values = sorted_deviation.values
    y_pos = numpy.arange(len(labels))
    bar_height = 0.6  


    fig_height = max(6, len(labels) * 0.25)

    plt.figure(figsize=(14, fig_height))
    plt.barh(y_pos, values, height=bar_height)
    plt.yticks(y_pos, labels, fontsize=7)
    plt.xlabel(f"Total Absolute Deviation in {datatypeName}")
    plt.ylabel("FieldCropYearId")
    plt.title(f"Total {datatypeName} Deviation from Daily Average\nCropType: {cropType}, MaxCloud: {max_cloud_cover}")
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.tight_layout()

    filename = f"{datatypeName}_Deviation_Barplot_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
    SavePlot(filename, output_dir)


def plot_Datapoints_gradient_and_rank_ids(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, output_dir, cropType, datatype, dataMode):
    print("Start the method plot_Datapoints_gradient_and_rank_ids")
    results = []
    datatype_name = db_handler.GetDatatypeName(datatype)
    
    from_date = pd.to_datetime(from_date)
    to_date = pd.to_datetime(to_date)
    year = from_date.year
    timeframfrom= f"{year}-08-01"
    timeframto=f"{year}-10-31"

    for CropYearId in id_list:
        datadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatype(CropYearId, from_date, to_date, datatype)
        FilteredDatadict = filter_cloudy_dates(datadict, CropYearId, db_handler, max_cloud_cover)

        if len(FilteredDatadict) < 2:
            continue  

        DataPoints = dataModeswitch(FilteredDatadict, dataMode)

        df = pd.DataFrame(DataPoints, index=list(FilteredDatadict.keys()), columns=[dataMode])
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)

        
        df_window = df[(df.index >= pd.to_datetime(timeframfrom)) & (df.index <= pd.to_datetime(timeframto))]

        if df_window.empty or len(df_window) < 2:
            continue

       

        delta_t = df_window.index.to_series().diff().dt.total_seconds() / (60 * 60 * 24)  
        delta_Datapoints = df_window[dataMode].diff() 
        df_window.loc[:, "Gradient"] = delta_Datapoints / delta_t  

        
        min_grad_index = df_window["Gradient"].idxmin()
        min_grad_value = df_window.loc[min_grad_index, "Gradient"]

        
        field_id = db_handler.GetFieldIdBasedOnFieldCropYearId(CropYearId)
        

        
        results.append({
            "CropYearId": CropYearId,
            "FieldId": field_id,
            "MinGradient": min_grad_value,
            "StartDate": df_window.index[df_window.index.get_loc(min_grad_index) - 1] if df_window.index.get_loc(min_grad_index) > 0 else None,
            "EndDate": min_grad_index
        })

        
        plt.figure(figsize=(10, 5))
        plt.plot(df_window.index, df_window["Gradient"], label=f"{datatype_name} Gradient", color="red", marker="o")
        plt.axhline(0, color='gray', linestyle='--')
        plt.title(f"{datatype_name} Gradient for Field {field_id} (CropType: {cropType})")
        plt.xlabel("Date")
        plt.ylabel(f"{datatype_name} Gradient (per day)")
        plt.legend()
        plt.annotate(f"Max Drop: {min_grad_value:.2f}",
                    xy=(min_grad_index, min_grad_value),
                    xytext=(min_grad_index, min_grad_value - 0.1),
                    arrowprops=dict(facecolor='black', shrink=0.05))


        filename = f"{datatype_name}_Gradient_Field_{field_id}_{cropType}_{dataMode}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
        SavePlot(filename, output_dir)

    
    sorted_results = sorted(results, key=lambda x: x["MinGradient"])


    must_visit_df = pd.DataFrame(sorted_results)
    must_visit_csv_path = os.path.join(output_dir, f"{cropType}_{datatype_name}_{dataMode}_must_visit.csv")
    must_visit_df.to_csv(must_visit_csv_path, index=False)

    print(f"Finished processing. Results saved to {must_visit_csv_path}")

def plot_all_datapoints(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, data_mode, output_dir,cropType,datatype):
    print("start the method plot_all_datapoints")
    plt.figure(figsize=(10, 5))
    datatypeName = db_handler.GetDatatypeName(datatype)
    for CropYearId in id_list:
        datadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatype(CropYearId, from_date, to_date,datatype)
        FilteredDatadict = filter_cloudy_dates(datadict,CropYearId,db_handler, max_cloud_cover)

        dates = list(FilteredDatadict.keys())
        data = dataModeswitch(FilteredDatadict, data_mode)

        plt.plot(dates, data, color="blue", alpha=0.2)


    title= f"{datatypeName} ({data_mode}) for All Fields with croptype: {cropType} from {from_date} to {to_date} MaxCloud:{max_cloud_cover}"
    setupPlot(title,datatypeName)
    filename = f"{datatypeName}_All_{data_mode}_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
    SavePlot(filename, output_dir)


def plot_ndvi_heatmap_synced_dips(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, data_mode, output_dir, cropType, datatype):
    print("Starting plot_ndvi_heatmap_synced_dips...")
    datatype_name = db_handler.GetDatatypeName(datatype)
    all_series = []
    num_points = 100  

    for CropYearId in id_list:
        datadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatype(CropYearId, from_date, to_date,datatype)
        FilteredDatadict = filter_cloudy_dates(datadict, CropYearId, db_handler, max_cloud_cover)

        if len(FilteredDatadict) < 5:
            continue

        dates = list(FilteredDatadict.keys())
        ndvi_data = dataModeswitch(FilteredDatadict, data_mode)

        x = numpy.array([(date - dates[0]).days for date in dates])
        y = numpy.array(ndvi_data)

        if len(x) < 2 or len(numpy.unique(x)) < 2:
            continue  


        x_uniform = numpy.linspace(0, (dates[-1] - dates[0]).days, num=num_points)
        y_uniform = numpy.interp(x_uniform, x, y)

   
        dip_idx = numpy.argmin(y_uniform)
        center_idx = num_points // 2
        shift = center_idx - dip_idx
        y_aligned = numpy.roll(y_uniform, shift)

        all_series.append(y_aligned)

    if not all_series:
        print("No valid data series after filtering and processing.")
        return

    data_matrix = numpy.vstack(all_series)

    plt.figure(figsize=(12, 6))
    plt.imshow(data_matrix, aspect='auto', cmap='viridis', interpolation='none')
    plt.colorbar(label='{datatype_name}')
    plt.title(f"{datatype_name} Heatmap aligned by dip — CropType: {cropType}")
    plt.xlabel("Aligned Time (normalized)")
    plt.ylabel("Field Instances")
    filename = f"Ndvi_Heatmap_SyncedDip_{data_mode}_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
    SavePlot(filename, output_dir)



def Plot_all_Datapoints_with_and_average_line(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, data_mode, output_dir, cropType, datatype):
    print("start the method Plot_all_ndvi_with_and_average_line")

    datatypeName = db_handler.GetDatatypeName(datatype)


    ndvi_by_date = defaultdict(list)
    plt.figure(figsize=(12, 6))
    for id in id_list:
        datadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatype(id, from_date, to_date, datatype)
        filtered_dict = filter_cloudy_dates(datadict,id, db_handler, max_cloud_cover)
        dates = list(filtered_dict.keys())
        data = dataModeswitch(filtered_dict,data_mode)

        for date_str, entry in datadict.items():
            entry["Date"] = date_str
        for date_str, entry in filtered_dict.items():
            value_list = dataModeswitch({date_str: entry}, data_mode)
            if value_list:
                ndvi_by_date[date_str].extend(value_list)

        plt.plot(dates, data, color="blue", alpha=0.2)

    
    flat_x = []
    flat_y = []
    avg_x = []
    avg_y = []

    for date in sorted(ndvi_by_date):
        values = ndvi_by_date[date]
        if values:
            flat_x.extend([date] * len(values))
            flat_y.extend(values)
            avg_x.append(date)
            avg_y.append(numpy.mean(values))

    
    
    title = f"{datatypeName} ({data_mode}) for All Fields with croptype: {cropType} from {from_date} to {to_date} with avg line, MaxCloud:{max_cloud_cover}"
    setupPlot(title,datatypeName)
    #plt.scatter(flat_x, flat_y, label="{datatype_name} values", alpha=0.5, s=10)
    plt.plot(avg_x, avg_y, color="red", label="Daily Average {datatype_name}", linewidth=2)
    plt.legend()

    filename = f"{datatypeName}_All_Avg_Line_{data_mode}_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
    SavePlot(filename, output_dir)


    
def plot_Datapoints_heatmap_matplotlib(id_list, db_handler: SQLHandler, from_date, to_date, max_cloud_cover, data_mode, output_dir, cropType,datatype):
    print("Generating {datatype_name} heatmap with matplotlib...")
    datatypeName = db_handler.GetDatatypeName(datatype)
    Data_matrix = defaultdict(dict)

    for CropYearId in id_list:
        datadict = db_handler.GetallDataPointsBasedOnIdAndDateRangeAndDatatype(CropYearId, from_date, to_date,datatype)
        FilteredDatadict = filter_cloudy_dates(datadict, CropYearId, db_handler, max_cloud_cover)
        
        dates = list(FilteredDatadict.keys())
        data = dataModeswitch(FilteredDatadict, data_mode)

        for date, Data in zip(dates, data):
            Data_matrix[CropYearId][date] = Data

  
    df = pd.DataFrame.from_dict(Data_matrix, orient='index')
    df = df[sorted(df.columns)] 

    height_per_field = 0.5  
    max_height = 50
    

    fig_height = min(len(df) * height_per_field, max_height)
    fig_width = min(len(df) * height_per_field, max_height)

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))


    df.columns = [pd.to_datetime(c).strftime('%Y-%m-%d') for c in df.columns]


    cax = ax.imshow(df.values, aspect='auto', cmap='YlGn', vmin=-1, vmax=1)


    ax.set_xticks(numpy.arange(len(df.columns)))
    ax.set_yticks(numpy.arange(len(df.index)))
    ax.set_xticklabels(df.columns, rotation=45, ha='right')
    ax.set_yticklabels(df.index)

    cbar = fig.colorbar(cax, ax=ax)
    cbar.set_label(datatypeName)

    ax.set_title(f"{datatypeName} ({data_mode}) Heatmap - {cropType}\n{from_date} to {to_date}, MaxCloud: {max_cloud_cover}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Field ID")
    plt.tight_layout()


    filename = f"{datatypeName}_Heatmap_MPL_{data_mode}_{cropType}_{from_date}_to_{to_date}_MaxCloud_{max_cloud_cover}.png"
    SavePlot(filename, output_dir)
    print(f"Saved heatmap to {filename}")

def main():
    parser = argparse.ArgumentParser(description="{datatype_name} Graph Generator")
    parser.add_argument("Mode", choices=["indMinMaxAvg", "All","AllAvgLine", "Heatmap","Gradient","Deviation","CvivsNdvi","NdrevsNdvi"], help="Graph mode")
    parser.add_argument("MaxCloud", type=float, help="Maximum allowed cloud cover")
    parser.add_argument("CropId",help="The crop to be used in graph making")
    parser.add_argument("FieldId", help="Field ID to graph, or 'all' for all fields")
    parser.add_argument("DataMode", choices=['min', 'max', 'median', 'average'], help="Datatype type for 'All' mode")
    parser.add_argument("FromDate",help="fromDate in format yyyy-mm-dd")
    parser.add_argument("ToDate",help="Todate in format yyyy-mm-dd")
    parser.add_argument("OutputDir",help="The output dir that the plot is saved to")
    parser.add_argument("Datatype",help="The data that should be taken out of the database")
    parser.add_argument("TiffFormat",choices=["BB","Field"])
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
    output_dir = f"plot/{args.OutputDir}"#plot
    Datamode = args.DataMode
    maxCloud = args.MaxCloud
    cropid = args.CropId
    cropTyptmp = db_handler.GetCropTypeBasedOnCropId(cropid)
    cropType = cropTyptmp.replace("/","_")
    datatype =args.Datatype
    Datamode = Datamode.capitalize()
    plotmode = args.Mode 
    TiffFormat = args.TiffFormat
    if args.FieldId == "all":
        id_list = db_handler.GetAllCropYearIdFromDataBasedOnCropIdAndTiffFormat(cropid,TiffFormat)
        print(id_list)
    else:
        id_list = db_handler.GetALLCropYearIdBasedOnCropIdAndFieldIdFromData(cropid,args.FieldId)
        print(id_list)

    starttime = time.time()
    if plotmode == "indMinMaxAvg":
        plot_individual_datapoints(id_list, db_handler, from_date, to_date, maxCloud, output_dir,cropType,datatype,TiffFormat)
    elif plotmode == "All":
        plot_all_datapoints(id_list, db_handler, from_date, to_date, maxCloud, Datamode, output_dir,cropType,datatype)
    elif plotmode == "AllAvgLine":
        Plot_all_Datapoints_with_and_average_line(id_list,db_handler,from_date,to_date,maxCloud, Datamode ,output_dir,cropType,datatype)
    elif plotmode == "Heatmap":
        plot_Datapoints_heatmap_matplotlib(id_list, db_handler, from_date, to_date, maxCloud, Datamode, output_dir, cropType,datatype)
    elif plotmode =="Gradient":
        plot_Datapoints_gradient_and_rank_ids(id_list, db_handler, from_date, to_date, maxCloud, output_dir, cropType, datatype, Datamode)
    elif plotmode == "Deviation":
        plot_field_deviation_boxplots(db_handler,from_date,to_date,datatype,maxCloud,output_dir,cropType,Datamode)
    elif plotmode == "CvivsNdvi":
        plot_NdviAndCvi(id_list, db_handler, from_date, to_date, maxCloud, output_dir,cropType,TiffFormat)
    elif plotmode == "NdrevsNdvi":
        plot_NdviAndNdre(id_list, db_handler, from_date, to_date, maxCloud, output_dir,cropType,TiffFormat)


    endTime = time.time()
    print(f"The plot process took {endTime-starttime} secounds")

if __name__ == "__main__":
    main()
