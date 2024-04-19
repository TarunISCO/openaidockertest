from flask import Flask, request, json
from querybot import QueryBot
import os

app = Flask(__name__)

openai_api_key = os.environ.get('OPENAI_API_KEY')

# Create a new querybot
queryBot = QueryBot(openai_api_key)


@app.route("/query", methods=['GET', 'POST'])
def app_response():
    respose_dict = {}
    
    query_json = request.get_json()
    userQuery = str(query_json['question'])#.encode("utf-8")
    
    respose_dict['userQuery'] = userQuery

    if request.method == 'POST' or request.method == 'GET':
        agent_response = queryBot.get_response(userQuery)
        respose_dict['result'] = agent_response

    app.logger.info(agent_response)

    return json.dumps(respose_dict), {'Content-Type': 'application/json'}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)