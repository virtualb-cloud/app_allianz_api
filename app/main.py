from flask import Flask, request, jsonify
from app.questionnaire_controller import Mifid_controller
from app.questionnaire_main import Questionnaire


# Config
app = Flask(__name__)

@app.route("/questionnaire_interpreter", methods=["POST"])
def questionnaire_interpreter():
    
    if request.method == "POST":

        body = request.get_data(force=True, silent=False, cache=True)

        controller = Mifid_controller()

        response = controller.run(people=body)
        if response[0] == []: return jsonify(response[1]), 422

        try:
            interpreter = Questionnaire()
            answer = interpreter.run(response[0])
        except:
            answer = []
        return jsonify(answer), 200
