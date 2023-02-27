
from flask import Flask, request, jsonify
import os
import pandas as pd
from threading import Thread
from app.ingestion_controller import Ingestion_controller
from app.background_tasks import ingestor
from zipfile import ZipFile
import io

# Config
app = Flask(__name__)

@app.route("/", methods=["POST"])
def ingestion_positions():
    
    if request.method == "POST":

        try:
            file = request.files['positions']  

            file_like_object = file.stream._file  
            zipfile_ob = ZipFile(file_like_object)
            file_names = zipfile_ob.namelist()
            
            # Filter names to only include the filetype that you want:
            file_names = [file_name for file_name in file_names if file_name.endswith(".csv")]
            files = [(zipfile_ob.open(name).read(), name) for name in file_names]
            
        except:
            return jsonify("file can not be read, please send the zip file with 'positions' tag"), 422

        try:
            uploaded_file = pd.read_csv(files[0][0], delimiter=";", encoding="latin-1")
        except:
            return jsonify("file can not be read, 'encoding' or 'delimiter' has changed. "), 422

        controller = Ingestion_controller()
        flag, errors = controller.run(uploaded_file)
        if not flag: return errors, 422

        thread = Thread(target=ingestor, args=(uploaded_file,))
        thread.daemon = True
        thread.start()
        
        return jsonify("ingestor worker started succesfully and will terminate in some minutes."), 200
