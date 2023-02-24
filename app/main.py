
from flask import Flask, request, jsonify
import os
import pandas as pd
from threading import Thread
from app.ingestion_controller import Ingestion_controller
from app.background_tasks import ingestor

# Config
app = Flask(__name__)

@app.route("/", methods=["POST"])
def ingestion_customers():
    
    if request.method == "POST":

        try:
            uploaded_file = request.files["customers"]
            uploaded_file.save(uploaded_file.filename)
        except:
            return jsonify("file can not be read, please send the csv file with 'customers' tag"), 422

        try:
            uploaded_file = pd.read_csv("work_clienti.csv", delimiter=";", encoding="latin-1")
        except:
            return jsonify("file can not be read, please name the csv file: 'work_clienti.csv' "), 422

        controller = Ingestion_controller()
        flag, errors = controller.run(uploaded_file)
        if not flag: return errors, 422

        thread = Thread(target=ingestor, args=(uploaded_file,))
        thread.daemon = True
        thread.start()

        # remove the file
        os.remove("work_clienti.csv")
        
        return jsonify("ingestor worker started succesfully and will terminate in some minutes."), 200
