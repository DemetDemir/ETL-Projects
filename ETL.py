!unzip datasource.zip -d dealership_data

tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored

# Function to extract all csv files in folder and save it as dataframe
def extract_csv(FileToProcess):
    df = pd.read_csv(FileToProcess)
    return df

# Function to extract all json files in folder and save it as dataframe
def extract_json(FileToProcess):
    df = pd.read_json(FileToProcess, lines=True)
    return df

# Function to extract all xml files in folder and save it as dataframe
def extract_from_xml(FileToProcess):
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(FileToProcess)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = car.find("year_of_manufacture").text
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = df.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}, ignore_index=True)
    return df

def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("dealership_data/*.csv"):
        extracted_data = extracted_data.append(extract_csv(csvfile), ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob("dealership_data/*.json"):
        extracted_data = extracted_data.append(extract_json(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("dealership_data/*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data

    # Transformation step to round up price to 2 decimal points
def transform(data):
        data['price'] = round(data.price,2)
        return data

# load transformed data into targetfile
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)

# Function to log ETL steps
def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


log("ETL Process started")
log("Extract started")
extracted_data = extract()
extracted_data
log("Extract phase completed")
log("Transform step started")
transformed_data = transform(extracted_data)
log("Transorm completed")
log("Loading started")
load(targetfile, transformed_data)
log("Loading completed")
