
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
            uploaded_file = request.files["advisors"]
            name = uploaded_file.filename
            uploaded_file.save(name)
        except:
            return jsonify("file can not be read, please send the csv file with 'advisors' tag"), 422

        try:
            df = pd.read_csv(name, delimiter=";", encoding="latin-1")
        except:
            return jsonify("file can not be read, either encoding or delimiter has changed. "), 422

        controller = Ingestion_controller()
        flag, errors = controller.run(df)
        if not flag: return errors, 422

        thread = Thread(target=ingestor, args=(df,))
        thread.daemon = True
        thread.start()

        # remove the file
        os.remove(name)
        
        return jsonify("ingestor worker started succesfully and will terminate in some minutes."), 200
