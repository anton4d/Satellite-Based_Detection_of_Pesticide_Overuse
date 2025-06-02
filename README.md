# **Satellite-Based Detection of Pesticide Overuse**  
GitHub repository for the **SDU Master Project** by **Oskar and Anton**  

## **Setup Instructions**  

Run the following command inside the code folder to set up the virtual environment and install dependencies:  

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

In Database_Populator, Download and Process a .env file is needed to connect to the database and more
in donwload it should look like 

```.env
SQLHOST="localhost"
DBUSER="user"
DBPASSWORD="password"
DBDB="db"
ApiClienId=""
ApiClienSecret=""
APIToken=""
```

In Process it should look like
```.env
SQLHOST="localhost"
DBUSER="user"
DBPASSWORD="password"
DBDB="db"
```
Now you're ready to start working on the project! 🚀