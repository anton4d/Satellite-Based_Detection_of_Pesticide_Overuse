# **Satellite-Based Detection of Pesticide Overuse**  
GitHub repository for the **SDU Master Project** by **Oskar and Anton**

---

## **Setup Instructions**

To set up the environment and start the project, follow these steps inside the `Code` folder:

### 1. Start Docker and Set Up the Environment

```bash
cd Code &&
docker compose up -d &&
python3 -m venv .venv &&
source .venv/bin/activate &&
pip install -r requirements.txt

```
### 2. Configure Environment Variables
You will need .env files for the different parts of the project.

In Database_Populator and Process folders:
Create a .env file with the following content:

```.env
SQLHOST="localhost"
DBUSER="user"
DBPASSWORD="password"
DBDB="db"
```
In Download folder:
Create a .env file with the following content:

```.env
SQLHOST="localhost"
DBUSER="user"
DBPASSWORD="password"
DBDB="db"
ApiClienId=""
ApiClienSecret=""
APIToken=""
```
### 3. Setup Database
with the databasepopulator and a a given sapefile in the shapefile folde the database can be setup easily


### 4. Running the Code

The first step is to start either a process or a download worker.  
Within the corresponding folder, run the following command:

```bash
python3 Worker_Start.py {Number of workers to start} {LogFileName} {LogFolder}
```
Next, you need to create a RabbitMQ Work Order in either new_Download_Work_Order or new_Process_Work_Order.
This will assign tasks to the workers to either start collecting data or process already collected data.
