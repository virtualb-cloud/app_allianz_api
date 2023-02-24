
from flask import Flask, request, jsonify
import os
import pandas as pd
from threading import Thread
from app.ingestion_controller import Ingestion_controller
from app.background_tasks import ingestor

# Config
app = Flask(__name__)

@app.route("/", methods=["POST"])
def train():
    
    if request.method == "POST":

        try:
            uploaded_file = request.files["products"]
            uploaded_file.save(uploaded_file.filename)
        except:
            return jsonify("file can not be read, please send the csv file with 'products' tag"), 422

        try:
            df = pd.read_csv("work_prodotti.csv", delimiter=";", encoding="latin-1")
        except:
            return jsonify("file can not be read, please name the csv file: 'work_prodotti.csv' "), 422

        controller = Ingestion_controller()
        flag, errors = controller.run(df)
        if not flag: return errors, 422

        thread = Thread(target=ingestor, args=(df,))
        thread.daemon = True
        thread.start()

        # remove the file
        os.remove("work_prodotti.csv")
        
        return jsonify("ingestor worker started succesfully and will terminate in some minutes."), 200
