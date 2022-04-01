# Import libraries
import numpy as np
from flask import Flask, request, jsonify
import pickle

app = Flask(__name__)

# Load the model
model = pickle.load(open('knnmodel.pkl','rb'))

@app.route('/api',methods=['POST'])
def predict():
    # Get the data from the POST request.
    data = request.get_json(force=True)
    
    # Make prediction using model loaded from disk as per the data.
    prediction = model.predict([np.array(data['val'])])
    
    # Take the first value of prediction
    output = prediction[0]
    
    return jsonify(output)

if __name__ == '__main__':
    app.run(port=5050, debug=True)
    
    
"""
import requests

url = 'http://127.0.0.1:5050/api'

r = requests.post(url,json={'val':[0.745659,0.35180,0.886998,0.711639],})

print(r.json())
"""