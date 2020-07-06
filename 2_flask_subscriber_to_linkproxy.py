#!flask/bin/python
from flask import Flask, request, jsonify
from flask_cors import CORS
import os 
import datetime
import json
import random, string

app = Flask(__name__)

CORS(app)

@app.route('/')
def index():
    return "Hello, World!"


@app.route('/linkproxypost', methods=['POST', 'GET'])
def enrich():
    if request.method == 'POST':
        body = request.json
        print(body)

        today = str(datetime.datetime.today()).split()[0]
        if not os.path.exists("static/"+today+"/"):
            os.makedirs("static/"+today+"/")
            
        x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16)) 

        with open('static/'+today+'/'+x+'.json', 'w') as f:
            json.dump(body, f)

        return "ok"


if __name__ == '__main__':
    app.run(debug=True, port=4242)
