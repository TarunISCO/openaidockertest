from flask import Flask, request
from querybot import QueryBot
import os
from pyspark.sql import SparkSession

app = Flask(__name__)

openai_api_key = os.environ.get('OPENAI_API_KEY')

# Create a new querybot
queryBot = QueryBot(openai_api_key)


@app.route("/query", methods=['GET', 'POST'])
def app_response():
    answer = ""
    query_json = request.get_json()
    print(query_json)
    question = str(query_json['question'])#.encode("utf-8")
    print(question, type(question))

    if request.method == 'POST' or request.method == 'GET':
        answer = queryBot.get_response(question)
        # answer = 'This is answer to : ' + question
        
    return answer


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)